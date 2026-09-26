/*!
Column type inference for CSV data.

This module provides a standalone inferencer that scans CSV input and
reports the likely data type of each column *without* modifying the
underlying data. It is useful for peeking at a CSV file before deciding
how to read it.

The set of inferred types is deliberately small:

* [Type::Empty]: only empty fields have been observed.
* [Type::Boolean]: only `true` or `false` (and empty) fields.
* [Type::Integer]: only integers that fit in a signed 64-bit integer.
* [Type::Decimal]: numbers in a fixed decimal syntax, kept exact (they
  are never round-tripped through an `f64`).
* [Type::Text]: anything else, or any incompatible mixture.

Types are combined with an explicit promotion lattice:

* Empty fields never change the type of a column.
* `Integer` mixed with `Decimal` promotes to `Decimal`.
* `Boolean` mixed with any numeric type, or any other incompatible
  mixture, promotes to `Text`.

Field classification is strict:

* Booleans are exactly `true` or `false`.
* Numbers are never trimmed of surrounding whitespace.
* Numbers must match `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?`.
  Integer-looking values with leading zeroes such as `001` do not match
  and are therefore classified as `Text`.
* Integer values that overflow a signed 64-bit integer, and any number
  with a fraction or exponent, are classified as `Decimal`.
* Fields that are not valid UTF-8 are classified as `Text`.

Memory usage grows only with the number of columns, not the number of
records. Records are streamed and discarded as they are scanned.

An optional sample limit caps the number of data records scanned. When
the limit is reached before the input is exhausted, the resulting report
is marked as partial: it describes a prefix of the input rather than a
complete conclusion.

Invalid CSV (for example, records with mismatched lengths) is reported
as a parse error, just like [Reader](crate::Reader) does.

# Example

```
let data = "\
active,count,ratio
true,1,1
false,2,1.20
maybe,3,3
";
let report = csv::infer::Inferencer::new()
    .infer_reader(data.as_bytes())
    .unwrap();
assert!(report.is_complete());
assert_eq!(report.records_scanned(), 3);

let columns = report.columns();
assert_eq!(columns[0].ty(), csv::infer::Type::Text);
assert_eq!(columns[1].ty(), csv::infer::Type::Integer);
assert_eq!(columns[2].ty(), csv::infer::Type::Decimal);

// The first column was promoted from Boolean to Text by the third
// data record (`maybe`).
let promotion = columns[0].first_promotion().unwrap();
assert_eq!(promotion.record(), 2);
assert_eq!(promotion.from(), csv::infer::Type::Boolean);
assert_eq!(promotion.to(), csv::infer::Type::Text);
```
*/

use std::fmt;
use std::io;
use std::path::Path;
use std::str;

use crate::{ByteRecord, Position, ReaderBuilder, Result};

/// The inferred data type of a single column.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Type {
    /// Only empty fields have been observed in the column so far.
    Empty,
    /// The column contains only `true` or `false` (and empty) fields.
    Boolean,
    /// The column contains only integers that fit in a signed 64-bit
    /// integer (and empty) fields.
    Integer,
    /// The column contains only numbers in the fixed decimal syntax
    /// `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?` (and empty)
    /// fields. Values are kept exact; they are never parsed through an
    /// `f64`.
    Decimal,
    /// The column contains values that are incompatible with every other
    /// type, or an incompatible mixture of types.
    Text,
}

impl Type {
    /// Returns the type that results from merging `other` into `self`
    /// according to the promotion lattice.
    ///
    /// * `Empty` never changes the other type.
    /// * `Integer` and `Decimal` merge into `Decimal`.
    /// * `Boolean` mixed with a numeric type, or any other incompatible
    ///   combination, promotes to `Text`.
    pub fn promote(self, other: Type) -> Type {
        use self::Type::*;
        match (self, other) {
            (Text, _) | (_, Text) => Text,
            (Empty, ty) | (ty, Empty) => ty,
            (Boolean, Boolean) => Boolean,
            (Integer, Integer) => Integer,
            (Integer, Decimal) | (Decimal, Integer) => Decimal,
            (Decimal, Decimal) => Decimal,
            (Boolean, Integer) | (Integer, Boolean) => Text,
            (Boolean, Decimal) | (Decimal, Boolean) => Text,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match *self {
            Type::Empty => "empty",
            Type::Boolean => "boolean",
            Type::Integer => "integer",
            Type::Decimal => "decimal",
            Type::Text => "text",
        };
        f.pad(name)
    }
}

/// Information about the first record that caused a column's type to be
/// promoted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Promotion {
    record: u64,
    position: Position,
    from: Type,
    to: Type,
}

impl Promotion {
    /// Returns the index of the data record (0-based, not counting the
    /// header record) that first caused the column's type to change.
    pub fn record(&self) -> u64 {
        self.record
    }

    /// Returns the position of that record in the original input.
    pub fn position(&self) -> &Position {
        &self.position
    }

    /// Returns the column's type before the promotion.
    pub fn from(&self) -> Type {
        self.from
    }

    /// Returns the column's type after the promotion.
    pub fn to(&self) -> Type {
        self.to
    }
}

/// The inference result for a single column.
#[derive(Clone, Debug)]
pub struct Column {
    ty: Type,
    empty_count: u64,
    first_promotion: Option<Promotion>,
}

impl Column {
    fn new() -> Column {
        Column { ty: Type::Empty, empty_count: 0, first_promotion: None }
    }

    /// Returns the final inferred type of this column.
    pub fn ty(&self) -> Type {
        self.ty
    }

    /// Returns the number of empty fields observed in this column.
    pub fn empty_count(&self) -> u64 {
        self.empty_count
    }

    /// Returns information about the first record that promoted this
    /// column's type, if any. The initial transition away from `Empty`
    /// (caused by the first non-empty field) is not considered a
    /// promotion.
    pub fn first_promotion(&self) -> Option<&Promotion> {
        self.first_promotion.as_ref()
    }
}

/// The result of scanning CSV input for column types.
#[derive(Clone, Debug)]
pub struct Report {
    headers: ByteRecord,
    columns: Vec<Column>,
    records_scanned: u64,
    complete: bool,
}

impl Report {
    /// Returns the header record of the input. When the inferencer was
    /// configured with `has_headers(false)`, this is empty. Headers are
    /// reported as-is; duplicate header names are preserved.
    pub fn headers(&self) -> &ByteRecord {
        &self.headers
    }

    /// Returns the inference result for each column, in order.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Returns the number of data records that were scanned.
    pub fn records_scanned(&self) -> u64 {
        self.records_scanned
    }

    /// Returns true when the entire input was scanned. This is false
    /// only when a sample limit stopped the scan before the input was
    /// exhausted, in which case this report describes a prefix of the
    /// input rather than a complete conclusion.
    pub fn is_complete(&self) -> bool {
        self.complete
    }
}

/// A builder for scanning CSV input and inferring column types.
///
/// The inferencer reads the input as raw bytes, so fields that are not
/// valid UTF-8 do not cause an error; they are classified as `Text`.
/// Memory usage grows only with the number of columns.
#[derive(Clone, Debug)]
pub struct Inferencer {
    has_headers: bool,
    sample_limit: Option<u64>,
}

impl Default for Inferencer {
    fn default() -> Inferencer {
        Inferencer::new()
    }
}

impl Inferencer {
    /// Creates a new inferencer with the default configuration: the
    /// first record is treated as a header row and there is no limit on
    /// the number of records scanned.
    pub fn new() -> Inferencer {
        Inferencer { has_headers: true, sample_limit: None }
    }

    /// Sets whether the first record is treated as a header row.
    ///
    /// When enabled (the default), the first record is reported via
    /// [Report::headers] and is not classified. When disabled, every
    /// record, including the first, participates in inference.
    pub fn has_headers(&mut self, yes: bool) -> &mut Inferencer {
        self.has_headers = yes;
        self
    }

    /// Sets the maximum number of data records to scan.
    ///
    /// When the limit is reached and more input remains, scanning stops
    /// and the report is marked as partial
    /// ([Report::is_complete] returns false). `None` (the default)
    /// scans the entire input.
    pub fn sample_limit(&mut self, limit: Option<u64>) -> &mut Inferencer {
        self.sample_limit = limit;
        self
    }

    /// Scans CSV data from the given reader and returns the inference
    /// report.
    ///
    /// Parse errors (such as records with mismatched lengths) and I/O
    /// errors are returned as [csv::Error](crate::Error).
    pub fn infer_reader<R: io::Read>(&self, rdr: R) -> Result<Report> {
        let mut rdr = ReaderBuilder::new()
            .has_headers(self.has_headers)
            .from_reader(rdr);
        let headers = if self.has_headers {
            rdr.byte_headers()?.clone()
        } else {
            ByteRecord::new()
        };
        let width = rdr.byte_headers()?.len();
        let mut columns: Vec<Column> =
            (0..width).map(|_| Column::new()).collect();
        let mut record = ByteRecord::new();
        let mut scanned = 0u64;
        let mut complete = true;
        loop {
            if !rdr.read_byte_record(&mut record)? {
                break;
            }
            if let Some(limit) = self.sample_limit {
                if scanned >= limit {
                    // More input remains, so this report only describes
                    // a prefix of the input.
                    complete = false;
                    break;
                }
            }
            for (i, field) in record.iter().enumerate() {
                let ty = classify(field);
                let column = &mut columns[i];
                if ty == Type::Empty {
                    column.empty_count += 1;
                }
                let merged = column.ty.promote(ty);
                if merged != column.ty {
                    if column.ty != Type::Empty
                        && column.first_promotion.is_none()
                    {
                        column.first_promotion = Some(Promotion {
                            record: scanned,
                            position: record
                                .position()
                                .expect("record from reader has position")
                                .clone(),
                            from: column.ty,
                            to: merged,
                        });
                    }
                    column.ty = merged;
                }
            }
            scanned += 1;
        }
        Ok(Report { headers, columns, records_scanned: scanned, complete })
    }

    /// Scans CSV data from the file at the given path and returns the
    /// inference report.
    pub fn infer_path<P: AsRef<Path>>(&self, path: P) -> Result<Report> {
        self.infer_reader(std::fs::File::open(path)?)
    }
}

/// Classifies a single raw field.
fn classify(field: &[u8]) -> Type {
    if field.is_empty() {
        return Type::Empty;
    }
    let text = match str::from_utf8(field) {
        Ok(text) => text,
        Err(_) => return Type::Text,
    };
    if text == "true" || text == "false" {
        return Type::Boolean;
    }
    match classify_number(text) {
        Some(NumberKind::Integer) => Type::Integer,
        Some(NumberKind::Decimal) => Type::Decimal,
        None => Type::Text,
    }
}

enum NumberKind {
    Integer,
    Decimal,
}

/// Classifies a string against the fixed number syntax
/// `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?`.
///
/// Returns `None` when the string does not match the syntax exactly (no
/// whitespace is trimmed). Integers that overflow an `i64`, and numbers
/// with a fraction or exponent, are classified as `NumberKind::Decimal`.
fn classify_number(text: &str) -> Option<NumberKind> {
    let bytes = text.as_bytes();
    let mut i = 0;
    if i < bytes.len() && bytes[i] == b'-' {
        i += 1;
    }
    if i >= bytes.len() {
        return None;
    }
    if bytes[i] == b'0' {
        i += 1;
    } else if bytes[i].is_ascii_digit() {
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
    } else {
        return None;
    }
    let mut decimal = false;
    if i < bytes.len() && bytes[i] == b'.' {
        decimal = true;
        i += 1;
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return None;
        }
    }
    if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
        decimal = true;
        i += 1;
        if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
            i += 1;
        }
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return None;
        }
    }
    if i != bytes.len() {
        return None;
    }
    if decimal {
        return Some(NumberKind::Decimal);
    }
    match text.parse::<i64>() {
        Ok(_) => Some(NumberKind::Integer),
        Err(_) => Some(NumberKind::Decimal),
    }
}
