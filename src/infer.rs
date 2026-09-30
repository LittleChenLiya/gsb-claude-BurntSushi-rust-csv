/*!
Streaming column type inference for CSV data.

This module provides a standalone type inferencer that scans CSV input and
reports, for each column, the narrowest type it can be read as without
modifying the underlying data. The set of inferred types is deliberately
small: null (empty fields), booleans, signed 64-bit integers, exact
decimals and text.

Types are combined with an explicit promotion lattice:

* Null never affects any other type. A column of only empty fields stays
  null.
* Integers and decimals merge into decimals.
* Booleans mixed with numbers, or any other incompatible combination, are
  promoted to text.

Booleans only accept the exact strings `true` and `false`. Numbers are
never trimmed of whitespace, and integers with leading zeros (like `001`)
are kept as text. The number grammar is fixed to
`-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?`. Decimals are handled
syntactically and never pass through an `f64`, so precision is never a
consideration. Valid numbers that overflow an `i64`, or that carry a
fraction or exponent, are classified as exact decimals.

Inference is streaming: memory usage grows only with the number of columns,
never with the number of records. Invalid CSV is still reported as a parse
error. An optional sample limit caps how many records are scanned; when the
limit is hit before EOF, the report is marked as a prefix-only (incomplete)
conclusion.

# Example

```
use csv::{Inferencer, InferredType, ReaderBuilder};

let data = "flag,count,ratio
true,0,1.20
false,5,3.5
";
let mut rdr = ReaderBuilder::new().from_reader(data.as_bytes());
let report = Inferencer::new().infer(&mut rdr)?;
assert!(report.complete());
assert_eq!(report.columns()[0].inferred_type(), InferredType::Boolean);
assert_eq!(report.columns()[1].inferred_type(), InferredType::Integer);
assert_eq!(report.columns()[2].inferred_type(), InferredType::Decimal);
# Ok::<(), csv::Error>(())
```
*/

use std::io::Read;

use crate::reader::Reader;
use crate::Result;

/// The type inferred for a single CSV column.
///
/// The variants are ordered by the promotion lattice: null is the identity
/// element, integers and decimals merge into decimals, and any incompatible
/// combination collapses to text.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum InferredType {
    /// Only empty fields have been observed in this column.
    Null,
    /// Every non-empty field is exactly `true` or `false`.
    Boolean,
    /// Every non-empty field is an integer that fits in a signed 64-bit
    /// integer, with no leading zeros and no surrounding whitespace.
    Integer,
    /// Every non-empty field is a valid number, but at least one of them
    /// has a fraction, an exponent, or overflows an `i64`. Decimals are
    /// classified syntactically and never pass through an `f64`.
    Decimal,
    /// The column contains values incompatible with every other type, or
    /// mixes booleans with numbers.
    Text,
}

impl InferredType {
    /// Returns the type of a single field.
    fn classify(field: &[u8]) -> InferredType {
        if field.is_empty() {
            return InferredType::Null;
        }
        let s = match std::str::from_utf8(field) {
            Ok(s) => s,
            // Non-UTF-8 data can never be anything but text.
            Err(_) => return InferredType::Text,
        };
        if s == "true" || s == "false" {
            return InferredType::Boolean;
        }
        match classify_number(s) {
            Some(ty) => ty,
            None => InferredType::Text,
        }
    }

    /// Returns the promotion of `self` with another observed field type.
    fn merge(self, other: InferredType) -> InferredType {
        use InferredType::*;
        match (self, other) {
            (Null, ty) | (ty, Null) => ty,
            (Text, _) | (_, Text) => Text,
            (Integer, Integer) => Integer,
            (Decimal, Decimal) => Decimal,
            (Integer, Decimal) | (Decimal, Integer) => Decimal,
            (Boolean, Boolean) => Boolean,
            // Booleans mixed with numbers, or anything else incompatible.
            _ => Text,
        }
    }
}

/// Classifies a string as a number according to the fixed grammar
/// `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?`.
///
/// Returns `None` when the string does not match the grammar at all.
/// Integers that fit in an `i64` are `Integer`; anything else that
/// matches (fractions, exponents, overflowing integers) is `Decimal`.
fn classify_number(s: &str) -> Option<InferredType> {
    let b = s.as_bytes();
    let mut i = 0;
    if i < b.len() && b[i] == b'-' {
        i += 1;
    }
    // Integer part: 0 | [1-9][0-9]*
    if i >= b.len() {
        return None;
    }
    if b[i] == b'0' {
        i += 1;
    } else if b[i].is_ascii_digit() {
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
    } else {
        return None;
    }
    let mut decimal = false;
    // Optional fraction: \.[0-9]+
    if i < b.len() && b[i] == b'.' {
        i += 1;
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return None;
        }
        decimal = true;
    }
    // Optional exponent: [eE][+-]?[0-9]+
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return None;
        }
        decimal = true;
    }
    if i != b.len() {
        return None;
    }
    if decimal {
        return Some(InferredType::Decimal);
    }
    // A bare integer: it is only an Integer when it fits in an i64.
    // Overflowing values are still valid numbers, so they become exact
    // decimals instead of text.
    match s.parse::<i64>() {
        Ok(_) => Some(InferredType::Integer),
        Err(_) => Some(InferredType::Decimal),
    }
}

/// The first record that caused a column's type to be promoted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TypePromotion {
    record: u64,
    line: u64,
    byte: u64,
    from: InferredType,
    to: InferredType,
}

impl TypePromotion {
    /// Returns the index of the data record (0-based, excluding any header
    /// row) that first caused the promotion.
    pub fn record(&self) -> u64 {
        self.record
    }

    /// Returns the 1-based line number at which that record starts.
    pub fn line(&self) -> u64 {
        self.line
    }

    /// Returns the byte offset at which that record starts.
    pub fn byte(&self) -> u64 {
        self.byte
    }

    /// Returns the column type before the promotion.
    pub fn from(&self) -> InferredType {
        self.from
    }

    /// Returns the column type after the promotion.
    pub fn to(&self) -> InferredType {
        self.to
    }
}

/// The inference result for a single column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnInference {
    inferred_type: InferredType,
    null_count: u64,
    first_promotion: Option<TypePromotion>,
}

impl ColumnInference {
    fn new() -> ColumnInference {
        ColumnInference {
            inferred_type: InferredType::Null,
            null_count: 0,
            first_promotion: None,
        }
    }

    /// Returns the final inferred type of this column.
    pub fn inferred_type(&self) -> InferredType {
        self.inferred_type
    }

    /// Returns the number of empty (null) fields observed in this column.
    pub fn null_count(&self) -> u64 {
        self.null_count
    }

    /// Returns the first record that caused this column's already
    /// established type to be promoted, if any promotion ever happened.
    ///
    /// The initial transition out of null (the first non-empty field) is
    /// not a promotion, since null never affects other types.
    pub fn first_promotion(&self) -> Option<TypePromotion> {
        self.first_promotion
    }

    /// Folds one observed field into this column's state.
    fn observe(&mut self, field: &[u8], record: u64, line: u64, byte: u64) {
        let ty = InferredType::classify(field);
        if ty == InferredType::Null {
            self.null_count += 1;
            return;
        }
        let merged = self.inferred_type.merge(ty);
        if merged != self.inferred_type {
            // The transition out of Null just establishes the type; only a
            // change of an already established type is a promotion.
            if self.inferred_type != InferredType::Null
                && self.first_promotion.is_none()
            {
                self.first_promotion = Some(TypePromotion {
                    record,
                    line,
                    byte,
                    from: self.inferred_type,
                    to: merged,
                });
            }
            self.inferred_type = merged;
        }
    }
}

/// The result of running type inference over a CSV input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InferenceReport {
    complete: bool,
    records_scanned: u64,
    headers: Option<Vec<String>>,
    columns: Vec<ColumnInference>,
}

impl InferenceReport {
    /// Returns true when every record of the input was scanned.
    ///
    /// This is false only when a sample limit stopped the scan before EOF,
    /// in which case the report describes a prefix of the input rather than
    /// a complete conclusion.
    pub fn complete(&self) -> bool {
        self.complete
    }

    /// Returns the number of data records that were scanned.
    pub fn records_scanned(&self) -> u64 {
        self.records_scanned
    }

    /// Returns the header row as lossy UTF-8 strings when the reader was
    /// configured with headers. Duplicate header names are preserved as-is.
    pub fn headers(&self) -> Option<&[String]> {
        self.headers.as_deref()
    }

    /// Returns the inference result of each column, in column order.
    pub fn columns(&self) -> &[ColumnInference] {
        &self.columns
    }
}

/// A builder for configuring a column type inferencer.
#[derive(Clone, Debug, Default)]
pub struct InferencerBuilder {
    sample_limit: Option<u64>,
}

impl InferencerBuilder {
    /// Creates a new builder with no sample limit.
    pub fn new() -> InferencerBuilder {
        InferencerBuilder::default()
    }

    /// Sets the maximum number of data records to scan.
    ///
    /// When `None` (the default), the entire input is scanned. When the
    /// limit is reached before EOF, the resulting report is marked
    /// incomplete, since it only describes a prefix of the input.
    pub fn sample_limit(
        &mut self,
        limit: Option<u64>,
    ) -> &mut InferencerBuilder {
        self.sample_limit = limit;
        self
    }

    /// Builds the inferencer.
    pub fn build(&self) -> Inferencer {
        Inferencer { sample_limit: self.sample_limit }
    }
}

/// A streaming column type inferencer.
///
/// See the [module level documentation](self) for the inference rules.
#[derive(Clone, Debug, Default)]
pub struct Inferencer {
    sample_limit: Option<u64>,
}

impl Inferencer {
    /// Creates a new inferencer that scans the entire input.
    pub fn new() -> Inferencer {
        Inferencer::default()
    }

    /// Creates a new inferencer that scans at most `limit` data records.
    pub fn with_sample_limit(limit: u64) -> Inferencer {
        Inferencer { sample_limit: Some(limit) }
    }

    /// Scans the CSV data produced by `rdr` and returns a type inference
    /// report for each column.
    ///
    /// The reader's own configuration (delimiter, headers, quoting, ...) is
    /// respected. Records are read as raw bytes, so non-UTF-8 fields do not
    /// cause an error; they simply classify their column as text. Invalid
    /// CSV (such as inconsistent record lengths) is returned as an error.
    ///
    /// Memory usage grows only with the number of columns, never with the
    /// number of records.
    pub fn infer<R: Read>(
        &self,
        rdr: &mut Reader<R>,
    ) -> Result<InferenceReport> {
        let headers = if rdr.has_headers() {
            Some(
                rdr.byte_headers()?
                    .iter()
                    .map(|h| String::from_utf8_lossy(h).into_owned())
                    .collect::<Vec<String>>(),
            )
        } else {
            None
        };
        let mut columns: Vec<ColumnInference> = Vec::new();
        let mut records_scanned: u64 = 0;
        let mut complete = true;
        let mut records = rdr.byte_records();
        loop {
            if self
                .sample_limit
                .map_or(false, |limit| records_scanned >= limit)
            {
                // The limit is exhausted. Probe one more record to learn
                // whether the input continues past the scanned prefix.
                complete = records.next().transpose()?.is_none();
                break;
            }
            let record = match records.next().transpose()? {
                None => break,
                Some(record) => record,
            };
            let (line, byte) = record
                .position()
                .map(|p| (p.line(), p.byte()))
                .unwrap_or((0, 0));
            if record.len() > columns.len() {
                columns.resize(record.len(), ColumnInference::new());
            }
            for (i, field) in record.iter().enumerate() {
                columns[i].observe(field, records_scanned, line, byte);
            }
            records_scanned += 1;
        }
        if let Some(ref headers) = headers {
            if headers.len() > columns.len() {
                columns.resize(headers.len(), ColumnInference::new());
            }
        }
        Ok(InferenceReport { complete, records_scanned, headers, columns })
    }
}
