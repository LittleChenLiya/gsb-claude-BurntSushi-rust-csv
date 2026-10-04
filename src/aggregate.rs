//! Aggregation of CSV records by business key with exact decimal amounts.
//!
//! This module provides [`Aggregator`], which reads CSV records and aggregates
//! a numeric amount column grouped by a key column. Amounts are converted to
//! integer units using an explicit decimal precision, **without any floating
//! point intermediary**.
//!
//! # State format
//!
//! The aggregator state is composed of:
//!
//! * `precision` — number of decimal places amounts are scaled to. For
//!   example, with `precision = 2`, the amount `0.10` becomes `10` integer
//!   units and `1.00` becomes `100`.
//! * `capacity` — maximum number of distinct keys the state will hold.
//! * `key_strategy` — how keys are matched ([`KeyStrategy::Exact`],
//!   [`KeyStrategy::CaseInsensitive`] or [`KeyStrategy::Trimmed`]).
//! * `keys` — keys in **first-valid-occurrence order**. This is the order in
//!   which keys are output.
//! * `sums` — sum per key in integer units.
//! * `counts` — number of valid rows per key.
//! * `total_sum` / `total_count` — grand totals across all keys.
//!
//! A row only updates the state when **both** its key and its amount are
//! valid. Overflow, over-precision and new keys exceeding capacity are
//! reported with the 1-based record number, and a failed row does not
//! increment `count` or `sum`.
//!
//! # Merging
//!
//! Two aggregators with the same `precision` and `key_strategy` can be merged
//! with [`Aggregator::merge`]. Merging is atomic: if any entry fails (e.g. a
//! sum overflow or a capacity breach), the target state is left unchanged.
//! Merging a state with a different precision or key strategy fails with a
//! config mismatch.
//!
//! # Example
//!
//! ```
//! use csv::aggregate::{Aggregator, AggregateConfig, KeyStrategy};
//! use csv::ReaderBuilder;
//!
//! let data = "a,0.10\na,0.20\n";
//! let mut rdr = ReaderBuilder::new()
//!     .has_headers(false)
//!     .from_reader(data.as_bytes());
//! let config = AggregateConfig {
//!     precision: 2,
//!     capacity: 16,
//!     key_strategy: KeyStrategy::Exact,
//!     ..AggregateConfig::default()
//! };
//! let mut agg = Aggregator::new(config);
//! agg.extend(&mut rdr).unwrap();
//! assert_eq!(agg.total_count(), 2);
//! assert_eq!(agg.total_sum(), 30); // 0.10 + 0.20 = 0.30 -> 30 units
//! ```

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::io;

use crate::{Reader, StringRecord, Writer};

/// The integer unit type used for amounts.
///
/// Amounts are scaled to this type using the configured precision. `i128` is
/// used so that exact decimal arithmetic has a wide range without floating
/// point.
pub type Amount = i128;

/// The count type used per key and for grand totals.
pub type Count = u64;

/// The strategy used to match business keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum KeyStrategy {
    /// Keys must match byte-for-byte exactly.
    #[default]
    Exact,
    /// Keys are compared case-insensitively (ASCII lowercase).
    CaseInsensitive,
    /// Keys are trimmed of leading and trailing ASCII whitespace before
    /// comparison.
    Trimmed,
}

impl KeyStrategy {
    /// Normalize a key according to this strategy.
    fn normalize(&self, key: &str) -> String {
        match self {
            KeyStrategy::Exact => key.to_string(),
            KeyStrategy::CaseInsensitive => key.to_ascii_lowercase(),
            KeyStrategy::Trimmed => key.trim().to_string(),
        }
    }
}

/// A column selector: either a 0-based index or a header name.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Column {
    /// Select the column at this 0-based index.
    Index(usize),
    /// Select the column with this header name.
    Name(String),
}

impl Default for Column {
    fn default() -> Self {
        Column::Index(0)
    }
}

/// Configuration for an [`Aggregator`].
#[derive(Debug, Clone)]
pub struct AggregateConfig {
    /// Number of decimal places amounts are converted to.
    ///
    /// For example, with `precision = 2`, the amount `0.10` becomes `10`
    /// integer units and `1.00` becomes `100`.
    pub precision: u32,
    /// Maximum number of distinct keys the aggregator will hold.
    pub capacity: usize,
    /// How keys are matched.
    pub key_strategy: KeyStrategy,
    /// Which column holds the business key.
    pub key_column: Column,
    /// Which column holds the amount.
    pub amount_column: Column,
}

impl Default for AggregateConfig {
    fn default() -> Self {
        AggregateConfig {
            precision: 2,
            capacity: 1024,
            key_strategy: KeyStrategy::Exact,
            key_column: Column::Index(0),
            amount_column: Column::Index(1),
        }
    }
}

/// An error that can occur during aggregation.
///
/// Errors carry the 1-based record number of the row that caused them, when
/// applicable.
#[derive(Debug)]
pub struct AggregateError {
    kind: Box<AggregateErrorKind>,
    record: Option<u64>,
}

/// The specific kind of aggregation error.
#[derive(Debug)]
#[non_exhaustive]
pub enum AggregateErrorKind {
    /// A CSV parsing error propagated from the reader.
    Csv(crate::Error),
    /// The amount has more fractional digits than the configured precision.
    OverPrecision {
        /// The offending amount string.
        amount: String,
        /// The configured precision.
        precision: u32,
    },
    /// The amount or a sum overflowed the integer unit type.
    Overflow {
        /// The offending amount string, if available.
        amount: String,
    },
    /// A new key would exceed the configured capacity.
    CapacityExceeded {
        /// The key that would exceed capacity.
        key: String,
        /// The configured capacity.
        capacity: usize,
    },
    /// The key field is empty.
    EmptyKey,
    /// The amount field is empty.
    EmptyAmount,
    /// The amount is not a valid decimal number.
    InvalidAmount {
        /// The offending amount string.
        amount: String,
    },
    /// The record does not have the required key or amount column.
    MissingColumns {
        /// The resolved key column index.
        key: usize,
        /// The resolved amount column index.
        amount: usize,
        /// The number of fields in the record.
        len: usize,
    },
    /// Merge failed because the configurations differ.
    ConfigMismatch {
        /// The target configuration.
        expected: AggregateConfig,
        /// The source configuration.
        found: AggregateConfig,
    },
    /// A column name could not be resolved against the headers.
    UnknownColumn {
        /// The column name that was not found.
        name: String,
    },
}

impl AggregateError {
    /// Return the kind of this error.
    pub fn kind(&self) -> &AggregateErrorKind {
        &self.kind
    }

    /// Return the 1-based record number associated with this error, if any.
    pub fn record(&self) -> Option<u64> {
        self.record
    }

    fn csv(err: crate::Error) -> AggregateError {
        AggregateError { kind: Box::new(AggregateErrorKind::Csv(err)), record: None }
    }

    fn over_precision(record: u64, amount: &str, precision: u32) -> AggregateError {
        AggregateError {
            kind: Box::new(AggregateErrorKind::OverPrecision {
                amount: amount.to_string(),
                precision,
            }),
            record: Some(record),
        }
    }

    fn overflow(record: u64, amount: &str) -> AggregateError {
        AggregateError {
            kind: Box::new(AggregateErrorKind::Overflow { amount: amount.to_string() }),
            record: Some(record),
        }
    }

    fn capacity_exceeded(record: u64, key: &str, capacity: usize) -> AggregateError {
        AggregateError {
            kind: Box::new(AggregateErrorKind::CapacityExceeded {
                key: key.to_string(),
                capacity,
            }),
            record: Some(record),
        }
    }

    fn empty_key(record: u64) -> AggregateError {
        AggregateError { kind: Box::new(AggregateErrorKind::EmptyKey), record: Some(record) }
    }

    fn empty_amount(record: u64) -> AggregateError {
        AggregateError {
            kind: Box::new(AggregateErrorKind::EmptyAmount),
            record: Some(record),
        }
    }

    fn invalid_amount(record: u64, amount: &str) -> AggregateError {
        AggregateError {
            kind: Box::new(AggregateErrorKind::InvalidAmount { amount: amount.to_string() }),
            record: Some(record),
        }
    }

    fn missing_columns(
        record: u64,
        key: usize,
        amount: usize,
        len: usize,
    ) -> AggregateError {
        AggregateError {
            kind: Box::new(AggregateErrorKind::MissingColumns { key, amount, len }),
            record: Some(record),
        }
    }

    fn config_mismatch(
        expected: AggregateConfig,
        found: AggregateConfig,
    ) -> AggregateError {
        AggregateError {
            kind: Box::new(AggregateErrorKind::ConfigMismatch { expected, found }),
            record: None,
        }
    }

    fn unknown_column(name: &str) -> AggregateError {
        AggregateError {
            kind: Box::new(AggregateErrorKind::UnknownColumn { name: name.to_string() }),
            record: None,
        }
    }
}

impl fmt::Display for AggregateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(record) = self.record {
            write!(f, "record {}: ", record)?;
        }
        match &*self.kind {
            AggregateErrorKind::Csv(err) => write!(f, "{}", err),
            AggregateErrorKind::OverPrecision { amount, precision } => {
                write!(
                    f,
                    "amount {:?} has more than {} decimal place(s)",
                    amount, precision
                )
            }
            AggregateErrorKind::Overflow { amount } => {
                write!(f, "amount {:?} overflows the integer unit type", amount)
            }
            AggregateErrorKind::CapacityExceeded { key, capacity } => {
                write!(
                    f,
                    "key {:?} would exceed the capacity of {}",
                    key, capacity
                )
            }
            AggregateErrorKind::EmptyKey => write!(f, "key field is empty"),
            AggregateErrorKind::EmptyAmount => write!(f, "amount field is empty"),
            AggregateErrorKind::InvalidAmount { amount } => {
                write!(f, "amount {:?} is not a valid decimal number", amount)
            }
            AggregateErrorKind::MissingColumns { key, amount, len } => {
                write!(
                    f,
                    "record has {} field(s) but key column {} and amount column {} were requested",
                    len, key, amount
                )
            }
            AggregateErrorKind::ConfigMismatch { expected, found } => {
                write!(
                    f,
                    "config mismatch: target has precision={}, key_strategy={:?}, but source has precision={}, key_strategy={:?}",
                    expected.precision,
                    expected.key_strategy,
                    found.precision,
                    found.key_strategy
                )
            }
            AggregateErrorKind::UnknownColumn { name } => {
                write!(f, "column {:?} was not found in the headers", name)
            }
        }
    }
}

impl StdError for AggregateError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match &*self.kind {
            AggregateErrorKind::Csv(err) => Some(err),
            _ => None,
        }
    }
}

impl From<crate::Error> for AggregateError {
    fn from(err: crate::Error) -> AggregateError {
        AggregateError::csv(err)
    }
}

/// Aggregates CSV records by business key with exact decimal amounts.
///
/// See the [module documentation](index.html) for details on the state format
/// and merging semantics.
#[derive(Debug, Clone)]
pub struct Aggregator {
    config: AggregateConfig,
    /// Keys in first-valid-occurrence order.
    keys: Vec<String>,
    /// Sum per key in integer units.
    sums: Vec<Amount>,
    /// Count per key.
    counts: Vec<Count>,
    /// Grand total sum in integer units.
    total_sum: Amount,
    /// Grand total count.
    total_count: Count,
    /// Normalized key -> index into `keys`/`sums`/`counts`.
    map: HashMap<String, usize>,
    /// Resolved key column index.
    key_idx: usize,
    /// Resolved amount column index.
    amount_idx: usize,
}

impl Aggregator {
    /// Create a new empty aggregator with the given configuration.
    pub fn new(config: AggregateConfig) -> Aggregator {
        let key_idx = match config.key_column {
            Column::Index(i) => i,
            Column::Name(_) => 0,
        };
        let amount_idx = match config.amount_column {
            Column::Index(i) => i,
            Column::Name(_) => 1,
        };
        Aggregator {
            config,
            keys: Vec::new(),
            sums: Vec::new(),
            counts: Vec::new(),
            total_sum: 0,
            total_count: 0,
            map: HashMap::new(),
            key_idx,
            amount_idx,
        }
    }

    /// Create an aggregator and consume all records from `rdr`.
    pub fn from_reader<R: io::Read>(
        config: AggregateConfig,
        rdr: &mut Reader<R>,
    ) -> Result<Aggregator, AggregateError> {
        let mut agg = Aggregator::new(config);
        agg.extend(rdr)?;
        Ok(agg)
    }

    /// Read all records from `rdr` and aggregate them.
    pub fn extend<R: io::Read>(
        &mut self,
        rdr: &mut Reader<R>,
    ) -> Result<(), AggregateError> {
        self.resolve_columns(rdr)?;
        for result in rdr.records() {
            let record = result.map_err(AggregateError::csv)?;
            self.add_record(&record)?;
        }
        Ok(())
    }

    /// Resolve the key and amount column selectors against the reader's
    /// headers.
    fn resolve_columns<R: io::Read>(
        &mut self,
        rdr: &mut Reader<R>,
    ) -> Result<(), AggregateError> {
        let key_needs_name = matches!(self.config.key_column, Column::Name(_));
        let amount_needs_name =
            matches!(self.config.amount_column, Column::Name(_));
        if rdr.has_headers() && (key_needs_name || amount_needs_name) {
            let headers = rdr.headers().map_err(AggregateError::csv)?;
            if key_needs_name {
                let name = match &self.config.key_column {
                    Column::Name(n) => n,
                    _ => unreachable!(),
                };
                self.key_idx = headers
                    .iter()
                    .position(|h| h == name)
                    .ok_or_else(|| AggregateError::unknown_column(name))?;
            }
            if amount_needs_name {
                let name = match &self.config.amount_column {
                    Column::Name(n) => n,
                    _ => unreachable!(),
                };
                self.amount_idx = headers
                    .iter()
                    .position(|h| h == name)
                    .ok_or_else(|| AggregateError::unknown_column(name))?;
            }
        } else {
            if let Column::Index(i) = self.config.key_column {
                self.key_idx = i;
            }
            if let Column::Index(i) = self.config.amount_column {
                self.amount_idx = i;
            }
        }
        Ok(())
    }

    /// Add a single record to the aggregation.
    ///
    /// The record number is taken from the record's position (1-based). A row
    /// only updates the state when both its key and its amount are valid; on
    /// error the state is left unchanged.
    pub fn add_record(
        &mut self,
        record: &StringRecord,
    ) -> Result<(), AggregateError> {
        let record_num = record
            .position()
            .map(|p| p.record() + 1)
            .unwrap_or(0);
        let key_idx = self.key_idx;
        let amount_idx = self.amount_idx;
        if key_idx >= record.len() || amount_idx >= record.len() {
            return Err(AggregateError::missing_columns(
                record_num,
                key_idx,
                amount_idx,
                record.len(),
            ));
        }
        let raw_key = &record[key_idx];
        let amount_str = &record[amount_idx];
        let key = self.config.key_strategy.normalize(raw_key);
        if key.is_empty() {
            return Err(AggregateError::empty_key(record_num));
        }
        if amount_str.is_empty() {
            return Err(AggregateError::empty_amount(record_num));
        }
        let units = parse_amount(amount_str, self.config.precision, record_num)?;
        self.add_entry(&key, units, 1, record_num)
    }

    /// Add a pre-computed entry (key, sum, count) to the state.
    ///
    /// This is atomic: all new values are computed before any mutation, so on
    /// error the state is unchanged.
    fn add_entry(
        &mut self,
        key: &str,
        sum: Amount,
        count: Count,
        record: u64,
    ) -> Result<(), AggregateError> {
        let normalized = self.config.key_strategy.normalize(key);
        // Compute new values before mutating, so that on error the state is
        // left unchanged.
        let (new_sum, new_count, is_new) = match self.map.get(&normalized) {
            Some(&idx) => {
                let new_sum = self.sums[idx]
                    .checked_add(sum)
                    .ok_or_else(|| AggregateError::overflow(record, key))?;
                let new_count = self.counts[idx]
                    .checked_add(count)
                    .ok_or_else(|| AggregateError::overflow(record, key))?;
                (new_sum, new_count, false)
            }
            None => {
                if self.keys.len() >= self.config.capacity {
                    return Err(AggregateError::capacity_exceeded(
                        record,
                        key,
                        self.config.capacity,
                    ));
                }
                (sum, count, true)
            }
        };
        let new_total_sum = self
            .total_sum
            .checked_add(sum)
            .ok_or_else(|| AggregateError::overflow(record, key))?;
        let new_total_count = self
            .total_count
            .checked_add(count)
            .ok_or_else(|| AggregateError::overflow(record, key))?;
        // Commit.
        if is_new {
            let idx = self.keys.len();
            self.keys.push(key.to_string());
            self.sums.push(new_sum);
            self.counts.push(new_count);
            self.map.insert(normalized, idx);
        } else {
            let idx = self.map[&normalized];
            self.sums[idx] = new_sum;
            self.counts[idx] = new_count;
        }
        self.total_sum = new_total_sum;
        self.total_count = new_total_count;
        Ok(())
    }

    /// Merge another aggregator into this one.
    ///
    /// Merging requires the same precision and key strategy. The merge is
    /// atomic: if any entry fails (e.g. a sum overflow or a capacity breach),
    /// the target state is left unchanged. A config mismatch also leaves the
    /// target unchanged.
    pub fn merge(&mut self, other: &Aggregator) -> Result<(), AggregateError> {
        if self.config.precision != other.config.precision
            || self.config.key_strategy != other.config.key_strategy
        {
            return Err(AggregateError::config_mismatch(
                self.config.clone(),
                other.config.clone(),
            ));
        }
        // Merge into a clone so that on error `self` is left unchanged.
        let mut merged = self.clone();
        for (key, sum, count) in other.entries() {
            merged.add_entry(key, sum, count, 0)?;
        }
        *self = merged;
        Ok(())
    }

    /// Return the configuration of this aggregator.
    pub fn config(&self) -> &AggregateConfig {
        &self.config
    }

    /// Return the number of distinct keys.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Return `true` if no keys have been aggregated.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Return the grand total sum in integer units.
    pub fn total_sum(&self) -> Amount {
        self.total_sum
    }

    /// Return the grand total count.
    pub fn total_count(&self) -> Count {
        self.total_count
    }

    /// Return an iterator over `(key, sum, count)` in first-valid-occurrence
    /// order.
    pub fn entries(&self) -> impl Iterator<Item = (&str, Amount, Count)> {
        self.keys
            .iter()
            .zip(self.sums.iter())
            .zip(self.counts.iter())
            .map(|((k, &s), &c)| (k.as_str(), s, c))
    }

    /// Return the sum and count for a single key, if present.
    pub fn get(&self, key: &str) -> Option<(Amount, Count)> {
        let normalized = self.config.key_strategy.normalize(key);
        self.map.get(&normalized).map(|&idx| {
            (self.sums[idx], self.counts[idx])
        })
    }

    /// Format an amount in integer units as a decimal string using the
    /// configured precision.
    pub fn format_amount(&self, units: Amount) -> String {
        format_units(units, self.config.precision)
    }

    /// Write the aggregated state as CSV rows of `key,sum,count`.
    ///
    /// The `sum` column is written in integer units. A header row
    /// `key,sum,count` is written first.
    pub fn write_csv<W: io::Write>(
        &self,
        wtr: &mut Writer<W>,
    ) -> Result<(), crate::Error> {
        wtr.write_record(["key", "sum", "count"])?;
        for (key, sum, count) in self.entries() {
            wtr.write_record([key, &sum.to_string(), &count.to_string()])?;
        }
        wtr.flush()?;
        Ok(())
    }
}

impl fmt::Display for Aggregator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "Aggregator(precision={}, capacity={}, keys={}, total_sum={}, total_count={})",
            self.config.precision,
            self.config.capacity,
            self.keys.len(),
            self.total_sum,
            self.total_count
        )?;
        for (key, sum, count) in self.entries() {
            writeln!(
                f,
                "  {:<24} sum={:<20} count={}",
                key,
                format_units(sum, self.config.precision),
                count
            )?;
        }
        Ok(())
    }
}

/// Parse a decimal amount string into integer units without floating point.
fn parse_amount(
    s: &str,
    precision: u32,
    record: u64,
) -> Result<Amount, AggregateError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(AggregateError::empty_amount(record));
    }
    // Sign.
    let (sign, rest) = match s.as_bytes()[0] {
        b'-' => (-1i128, &s[1..]),
        b'+' => (1i128, &s[1..]),
        _ => (1i128, s),
    };
    if rest.is_empty() {
        return Err(AggregateError::invalid_amount(record, s));
    }
    // Split on the decimal point.
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None => (rest, ""),
    };
    // Both parts must be ASCII digits (and at least one must be non-empty).
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(AggregateError::invalid_amount(record, s));
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(AggregateError::invalid_amount(record, s));
    }
    // Over-precision.
    if frac_part.len() > precision as usize {
        return Err(AggregateError::over_precision(record, s, precision));
    }
    // Scale.
    let scale = 10i128
        .checked_pow(precision)
        .ok_or_else(|| AggregateError::overflow(record, s))?;
    // Integer part.
    let int_val: Amount = if int_part.is_empty() {
        0
    } else {
        int_part
            .parse()
            .map_err(|_| AggregateError::overflow(record, s))?
    };
    // Fractional part padded to `precision`.
    let mut frac_padded = String::with_capacity(precision as usize);
    frac_padded.push_str(frac_part);
    while frac_padded.len() < precision as usize {
        frac_padded.push('0');
    }
    let frac_val: Amount = if frac_padded.is_empty() {
        0
    } else {
        frac_padded
            .parse()
            .map_err(|_| AggregateError::overflow(record, s))?
    };
    let units = int_val
        .checked_mul(scale)
        .and_then(|v| v.checked_add(frac_val))
        .ok_or_else(|| AggregateError::overflow(record, s))?;
    Ok(sign * units)
}

/// Format integer units as a decimal string with the given precision.
fn format_units(units: Amount, precision: u32) -> String {
    if precision == 0 {
        return units.to_string();
    }
    let negative = units < 0;
    let abs = units.unsigned_abs();
    let scale = 10u128.pow(precision);
    let int_part = abs / scale;
    let frac_part = abs % scale;
    format!(
        "{}{}.{:0>width$}",
        if negative { "-" } else { "" },
        int_part,
        frac_part,
        width = precision as usize
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Position, ReaderBuilder};

    fn aggregate(
        data: &str,
        config: AggregateConfig,
    ) -> Result<Aggregator, AggregateError> {
        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(data.as_bytes());
        Aggregator::from_reader(config, &mut rdr)
    }

    #[test]
    fn basic_aggregation() {
        let agg = aggregate(
            "a,0.10\na,0.20\n",
            AggregateConfig { precision: 2, ..Default::default() },
        )
        .unwrap();
        assert_eq!(agg.total_count(), 2);
        assert_eq!(agg.total_sum(), 30);
        assert_eq!(agg.get("a"), Some((30, 2)));
    }

    #[test]
    fn merge_two_chunks() {
        let mut a = aggregate(
            "a,0.10\na,0.20\n",
            AggregateConfig { precision: 2, ..Default::default() },
        )
        .unwrap();
        let b = aggregate(
            "a,0.30\nb,1.00\n",
            AggregateConfig { precision: 2, ..Default::default() },
        )
        .unwrap();
        a.merge(&b).unwrap();
        // Key "a" spans both chunks: 0.10 + 0.20 + 0.30 = 0.60, count 3.
        assert_eq!(a.get("a"), Some((60, 3)));
        assert_eq!(a.get("b"), Some((100, 1)));
        // Grand total across all keys: 0.60 + 1.00 = 1.60 -> 160 units.
        assert_eq!(a.total_sum(), 160);
        assert_eq!(a.total_count(), 4);
        let keys: Vec<&str> = a.entries().map(|(k, _, _)| k).collect();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    fn over_precision_rejected() {
        let mut agg = aggregate(
            "a,0.10\n",
            AggregateConfig { precision: 2, ..Default::default() },
        )
        .unwrap();
        let before = (agg.total_sum(), agg.total_count());
        let mut record = StringRecord::from(vec!["a", "0.001"]);
        record.set_position(Some(Position::new().set_record(0).clone()));
        let err = agg.add_record(&record).unwrap_err();
        assert_eq!(err.record(), Some(1));
        assert!(matches!(err.kind(), AggregateErrorKind::OverPrecision { .. }));
        assert_eq!((agg.total_sum(), agg.total_count()), before);
    }

    #[test]
    fn capacity_boundary() {
        let mut agg = Aggregator::new(AggregateConfig {
            precision: 2,
            capacity: 1,
            ..Default::default()
        });
        let r1 = StringRecord::from(vec!["a", "1.00"]);
        let r2 = StringRecord::from(vec!["b", "1.00"]);
        agg.add_record(&r1).unwrap();
        assert_eq!(agg.len(), 1);
        let err = agg.add_record(&r2).unwrap_err();
        assert!(matches!(
            err.kind(),
            AggregateErrorKind::CapacityExceeded { .. }
        ));
        assert_eq!(agg.len(), 1);
        assert_eq!(agg.get("a"), Some((100, 1)));
        assert_eq!(agg.get("b"), None);
    }

    #[test]
    fn negative_amounts() {
        let agg = aggregate(
            "a,-0.50\na,0.25\nb,-1.00\n",
            AggregateConfig { precision: 2, ..Default::default() },
        )
        .unwrap();
        assert_eq!(agg.get("a"), Some((-25, 2)));
        assert_eq!(agg.get("b"), Some((-100, 1)));
        assert_eq!(agg.total_sum(), -125);
        assert_eq!(agg.total_count(), 3);
    }

    #[test]
    fn overflow_rejected() {
        let mut agg = Aggregator::new(AggregateConfig {
            precision: 0,
            capacity: 4,
            ..Default::default()
        });
        let record = StringRecord::from(vec!["a", "999999999999999999999999999999999999999"]);
        let err = agg.add_record(&record).unwrap_err();
        assert!(matches!(err.kind(), AggregateErrorKind::Overflow { .. }));
        assert_eq!(agg.total_sum(), 0);
        assert_eq!(agg.total_count(), 0);
    }

    #[test]
    fn merge_overflow_atomic() {
        let mut target = Aggregator::new(AggregateConfig {
            precision: 0,
            capacity: 4,
            ..Default::default()
        });
        let r1 = StringRecord::from(vec!["a", "170000000000000000000000000000000000000"]);
        target.add_record(&r1).unwrap();
        let target_before = (target.total_sum(), target.total_count());
        let mut source = Aggregator::new(AggregateConfig {
            precision: 0,
            capacity: 4,
            ..Default::default()
        });
        let r2 = StringRecord::from(vec!["a", "170000000000000000000000000000000000000"]);
        source.add_record(&r2).unwrap();
        let err = target.merge(&source).unwrap_err();
        assert!(matches!(err.kind(), AggregateErrorKind::Overflow { .. }));
        assert_eq!((target.total_sum(), target.total_count()), target_before);
    }

    #[test]
    fn merge_config_mismatch_atomic() {
        let mut a = aggregate(
            "a,0.10\n",
            AggregateConfig { precision: 2, ..Default::default() },
        )
        .unwrap();
        let b = aggregate(
            "a,0.10\n",
            AggregateConfig { precision: 3, ..Default::default() },
        )
        .unwrap();
        let before = (a.total_sum(), a.total_count());
        let err = a.merge(&b).unwrap_err();
        assert!(matches!(err.kind(), AggregateErrorKind::ConfigMismatch { .. }));
        assert_eq!((a.total_sum(), a.total_count()), before);
    }

    #[test]
    fn merge_key_strategy_mismatch() {
        let mut a = Aggregator::new(AggregateConfig {
            precision: 2,
            key_strategy: KeyStrategy::Exact,
            ..Default::default()
        });
        let mut b = Aggregator::new(AggregateConfig {
            precision: 2,
            key_strategy: KeyStrategy::CaseInsensitive,
            ..Default::default()
        });
        let ra = StringRecord::from(vec!["a", "1.00"]);
        let rb = StringRecord::from(vec!["A", "1.00"]);
        a.add_record(&ra).unwrap();
        b.add_record(&rb).unwrap();
        let err = a.merge(&b).unwrap_err();
        assert!(matches!(err.kind(), AggregateErrorKind::ConfigMismatch { .. }));
    }

    #[test]
    fn invalid_rows_do_not_update_state() {
        let mut agg = Aggregator::new(AggregateConfig {
            precision: 2,
            capacity: 4,
            ..Default::default()
        });
        let good = StringRecord::from(vec!["a", "1.00"]);
        let bad = StringRecord::from(vec!["a", "abc"]);
        agg.add_record(&good).unwrap();
        let err = agg.add_record(&bad).unwrap_err();
        assert!(matches!(err.kind(), AggregateErrorKind::InvalidAmount { .. }));
        assert_eq!(agg.total_count(), 1);
        assert_eq!(agg.total_sum(), 100);
    }

    #[test]
    fn output_order_first_occurrence() {
        let agg = aggregate(
            "c,1.00\na,1.00\nb,1.00\na,1.00\n",
            AggregateConfig { precision: 2, ..Default::default() },
        )
        .unwrap();
        let keys: Vec<&str> = agg.entries().map(|(k, _, _)| k).collect();
        assert_eq!(keys, vec!["c", "a", "b"]);
    }

    #[test]
    fn case_insensitive_strategy() {
        let agg = aggregate(
            "A,1.00\na,2.00\n",
            AggregateConfig {
                precision: 2,
                key_strategy: KeyStrategy::CaseInsensitive,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(agg.len(), 1);
        assert_eq!(agg.get("a"), Some((300, 2)));
    }

    #[test]
    fn trimmed_strategy() {
        let agg = aggregate(
            " a ,1.00\na,2.00\n",
            AggregateConfig {
                precision: 2,
                key_strategy: KeyStrategy::Trimmed,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(agg.len(), 1);
        assert_eq!(agg.get("a"), Some((300, 2)));
    }

    #[test]
    fn format_amount_display() {
        let agg = Aggregator::new(AggregateConfig {
            precision: 2,
            ..Default::default()
        });
        assert_eq!(agg.format_amount(30), "0.30");
        assert_eq!(agg.format_amount(100), "1.00");
        assert_eq!(agg.format_amount(-25), "-0.25");
        assert_eq!(agg.format_amount(0), "0.00");
    }

    #[test]
    fn zero_precision() {
        let agg = aggregate(
            "a,1\na,2\n",
            AggregateConfig { precision: 0, ..Default::default() },
        )
        .unwrap();
        assert_eq!(agg.total_sum(), 3);
        assert_eq!(agg.total_count(), 2);
    }

    #[test]
    fn missing_columns_error() {
        let mut agg = Aggregator::new(AggregateConfig {
            precision: 2,
            ..Default::default()
        });
        let mut record = StringRecord::from(vec!["a"]);
        record.set_position(Some(Position::new().set_record(0).clone()));
        let err = agg.add_record(&record).unwrap_err();
        assert!(matches!(err.kind(), AggregateErrorKind::MissingColumns { .. }));
        assert_eq!(err.record(), Some(1));
    }

    #[test]
    fn empty_key_and_amount() {
        let mut agg = Aggregator::new(AggregateConfig {
            precision: 2,
            ..Default::default()
        });
        let empty_key = StringRecord::from(vec!["", "1.00"]);
        let empty_amount = StringRecord::from(vec!["a", ""]);
        assert!(matches!(
            agg.add_record(&empty_key).unwrap_err().kind(),
            AggregateErrorKind::EmptyKey
        ));
        assert!(matches!(
            agg.add_record(&empty_amount).unwrap_err().kind(),
            AggregateErrorKind::EmptyAmount
        ));
        assert_eq!(agg.len(), 0);
    }
}
