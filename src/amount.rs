//! Exact, fixed-precision aggregation of money amounts grouped by a
//! business key.
//!
//! This module sums a money column of CSV records by a key column without
//! ever routing the values through floating point. Every amount text is
//! converted directly into a signed integer number of "minor units" (for
//! example, cents when the precision is `2`), and all running totals are kept
//! as plain integers. This makes results exact: with a precision of `2`, the
//! records `a,0.10`, `a,0.20` and `a,0.30` sum to exactly `0.60` (stored as
//! `60`).
//!
//! # Overview
//!
//! The main type is [`crate::amount::AmountAggregator`]. It is configured
//! with an [`crate::amount::AmountConfig`] that names the key and amount
//! columns, fixes the decimal precision and defines how keys are compared.
//! Records are fed in one at a time with
//! [`crate::amount::AmountAggregator::add_record`]; only rows where *both*
//! the key and the amount are valid update the state. Two independently built
//! aggregators can be combined with
//! [`crate::amount::AmountAggregator::merge`], which is atomic: on failure
//! the target aggregator is left completely untouched.
//!
//! Results are produced by [`crate::amount::AmountAggregator::entries`] in
//! the stable order of each key first valid appearance.
//! [`crate::amount::AmountAggregator::format_amount`] turns an integer
//! minor-unit value back into fixed-point text.
//!
//! # Amount grammar
//!
//! An amount must consist of an optional sign followed by either one or more
//! digits with an optional decimal point and zero or more fractional digits,
//! or a decimal point followed by one or more digits. There must be no
//! whitespace or grouping characters. Exponents, `NaN`, `inf` and empty
//! fields are rejected. A number with more significant fractional digits than
//! the configured precision is rejected with
//! [`crate::amount::AggregateErrorKind::ExcessPrecision`]; amounts and totals
//! that do not fit in `i64` minor units are rejected with
//! [`crate::amount::AggregateErrorKind::Overflow`].
//!
//! # State format
//!
//! The aggregation state lives entirely in memory in an `AmountAggregator`
//! and is intentionally simple and deterministic:
//!
//! * `config`: the [`crate::amount::AmountConfig`] the aggregator was built
//!   with,
//! * `next_record`: the 1-based number assigned to the next attempted row,
//! * `order`: the distinct keys in first-valid-appearance order, and
//! * one [`crate::amount::Entry`] per key holding the normalized `key`, the
//!   valid-row `count` (`u64`) and the exact `sum` in minor units (`i64`).
//!
//! For example, feeding `a,0.10` then `a,0.20` at precision `2` gives
//! `order = ["a"]` and `Entry { key: "a", count: 2, sum: 20 }`, which formats
//! as `0.20`. Failed rows never appear in `order` and never alter any count
//! or sum.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;

use crate::string_record::StringRecord;

/// The largest supported number of digits after the decimal point.
pub const MAX_PRECISION: u8 = 18;

/// Configuration for an [`AmountAggregator`].
///
/// Use [`AmountConfig::new`] and the builder methods to construct one:
///
/// ```
/// use csv::amount::{AmountConfig, KeyNormalization};
///
/// let config = AmountConfig::new(0, 1, 2)
///     .key_normalization(KeyNormalization::AsciiLowercase)
///     .max_keys(1000);
/// assert_eq!(config.precision(), 2);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AmountConfig {
    key_column: usize,
    amount_column: usize,
    precision: u8,
    key_norm: KeyNormalization,
    max_keys: Option<usize>,
}

impl AmountConfig {
    /// Creates a configuration that reads the business key from
    /// `key_column` and the amount from `amount_column`, keeping
    /// `precision` digits after the decimal point.
    ///
    /// Column indices are zero based, matching the indices used by
    /// [`StringRecord::get`](crate::StringRecord::get). The precision must be
    /// at most [`MAX_PRECISION`]. Keys are compared verbatim
    /// ([`KeyNormalization::Verbatim`]) and the number of distinct keys is
    /// unbounded unless [`AmountConfig::max_keys`] is used.
    pub fn new(
        key_column: usize,
        amount_column: usize,
        precision: u8,
    ) -> AmountConfig {
        AmountConfig {
            key_column,
            amount_column,
            precision,
            key_norm: KeyNormalization::Verbatim,
            max_keys: None,
        }
    }

    /// Sets how raw key fields are turned into the keys used for grouping.
    #[must_use]
    pub fn key_normalization(
        mut self,
        norm: KeyNormalization,
    ) -> AmountConfig {
        self.key_norm = norm;
        self
    }

    /// Caps the number of distinct keys the aggregator will accept.
    ///
    /// A valid row that would introduce a brand new key once the cap is
    /// reached is rejected with
    /// [`AggregateErrorKind::KeyCapacityExceeded`]. Rows for keys that already
    /// exist are still accepted.
    #[must_use]
    pub fn max_keys(mut self, max: usize) -> AmountConfig {
        self.max_keys = Some(max);
        self
    }

    /// Returns the zero-based index of the business-key column.
    pub fn key_column(&self) -> usize {
        self.key_column
    }

    /// Returns the zero-based index of the amount column.
    pub fn amount_column(&self) -> usize {
        self.amount_column
    }

    /// Returns the number of digits kept after the decimal point.
    pub fn precision(&self) -> u8 {
        self.precision
    }

    /// Returns the key comparison strategy.
    pub fn key_normalization_strategy(&self) -> KeyNormalization {
        self.key_norm
    }

    /// Returns the cap on distinct keys, if any.
    pub fn max_key_capacity(&self) -> Option<usize> {
        self.max_keys
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.precision > MAX_PRECISION {
            return Err(ConfigError::PrecisionTooLarge(self.precision));
        }
        if self.key_column == self.amount_column {
            return Err(ConfigError::ColumnsCollide(self.key_column));
        }
        Ok(())
    }

    fn normalize_key(&self, raw: &str) -> String {
        match self.key_norm {
            KeyNormalization::Verbatim => raw.to_string(),
            KeyNormalization::AsciiLowercase => raw.to_ascii_lowercase(),
        }
    }
}

/// How raw key fields are normalized before they are grouped.
///
/// Two [`AmountAggregator`]s may only be merged when they use the same
/// normalization; see [`AmountAggregator::merge`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum KeyNormalization {
    /// Keys are compared byte for byte, exactly as they appear in the CSV.
    ///
    /// This is the default.
    #[default]
    Verbatim,
    /// ASCII uppercase letters in keys are folded to lowercase before
    /// comparison and storage. Non-ASCII characters are left untouched.
    AsciiLowercase,
}

/// An error returned when an [`AmountConfig`] is itself invalid.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConfigError {
    /// The requested precision is larger than [`MAX_PRECISION`].
    ///
    /// Contains the precision that was requested.
    PrecisionTooLarge(u8),
    /// The key and amount columns name the same column index.
    ///
    /// Contains the offending column index.
    ColumnsCollide(usize),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            ConfigError::PrecisionTooLarge(p) => write!(
                f,
                "decimal precision {p} is larger than the maximum {MAX_PRECISION}"
            ),
            ConfigError::ColumnsCollide(i) => {
                write!(f, "key column and amount column are both column {i}")
            }
        }
    }
}

impl StdError for ConfigError {}

/// A per-record failure from [`AmountAggregator::add_record`].
///
/// Failed rows do not consume a key slot and never change any count or sum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateError {
    kind: AggregateErrorKind,
    record: u64,
}

impl AggregateError {
    /// Returns the specific kind of failure.
    pub fn kind(&self) -> &AggregateErrorKind {
        &self.kind
    }

    /// Returns the 1-based record number at which the failure occurred.
    ///
    /// Record numbers count every attempted
    /// [`add_record`](AmountAggregator::add_record) call, including rejected
    /// rows, starting at `1`.
    pub fn record(&self) -> u64 {
        self.record
    }
}

impl fmt::Display for AggregateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            AggregateErrorKind::MissingColumn { column, fields } => write!(
                f,
                "record {} is missing column {} (record has {} field(s))",
                self.record, column, fields
            ),
            AggregateErrorKind::EmptyKey => {
                write!(f, "record {} has an empty business key", self.record)
            }
            AggregateErrorKind::InvalidAmount(raw) => {
                write!(f, "record {} has an invalid amount {:?}", self.record, raw)
            }
            AggregateErrorKind::ExcessPrecision { raw, precision } => write!(
                f,
                "record {} amount {:?} has more than {} digit(s) after the decimal point",
                self.record, raw, precision
            ),
            AggregateErrorKind::Overflow(raw) => write!(
                f,
                "record {} amount {:?} overflows the integer accumulator",
                self.record, raw
            ),
            AggregateErrorKind::KeyCapacityExceeded(key) => write!(
                f,
                "record {} introduces new key {:?} but the key capacity is full",
                self.record, key
            ),
        }
    }
}

impl StdError for AggregateError {}

/// The specific kind of an [`AggregateError`].
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AggregateErrorKind {
    /// A needed column is absent because the record is too short.
    MissingColumn {
        /// The zero-based column index that could not be read.
        column: usize,
        /// The number of fields the record actually had.
        fields: usize,
    },
    /// The key field was present but empty.
    EmptyKey,
    /// The amount text was not a well-formed fixed-point number.
    ///
    /// Contains the raw amount text.
    InvalidAmount(String),
    /// The amount had more significant fractional digits than the precision.
    ExcessPrecision {
        /// The raw amount text.
        raw: String,
        /// The configured precision.
        precision: u8,
    },
    /// The amount itself or the resulting running total does not fit in
    /// `i64` minor units.
    ///
    /// Contains the raw amount text for per-record errors, or an empty string
    /// when a running total overflowed.
    Overflow(String),
    /// A valid new key could not be stored because the configured key
    /// capacity was reached.
    ///
    /// Contains the normalized key that could not be added.
    KeyCapacityExceeded(String),
}

/// A failure while merging two aggregators.
///
/// Every merge failure is atomic: the target aggregator is unchanged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergeError {
    kind: MergeErrorKind,
}

impl MergeError {
    /// Returns the specific kind of merge failure.
    pub fn kind(&self) -> &MergeErrorKind {
        &self.kind
    }
}

impl fmt::Display for MergeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            MergeErrorKind::PrecisionMismatch { target, other } => write!(
                f,
                "cannot merge aggregators with precision {target} and {other}"
            ),
            MergeErrorKind::KeyNormalizationMismatch => write!(
                f,
                "cannot merge aggregators with different key normalization strategies"
            ),
            MergeErrorKind::KeyCapacityExceeded(key) => write!(
                f,
                "merging would introduce new key {key:?} but the target key capacity is full"
            ),
            MergeErrorKind::CountOverflow(key) => write!(
                f,
                "merging would overflow the row count for key {key:?}"
            ),
            MergeErrorKind::SumOverflow(key) => write!(
                f,
                "merging would overflow the amount total for key {key:?}"
            ),
        }
    }
}

impl StdError for MergeError {}

/// The specific kind of a [`MergeError`].
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MergeErrorKind {
    /// The aggregators were built with different precisions.
    PrecisionMismatch {
        /// The precision of the merge target.
        target: u8,
        /// The precision of the aggregator being merged in.
        other: u8,
    },
    /// The aggregators use different key comparison strategies.
    KeyNormalizationMismatch,
    /// A key from the other aggregator would exceed the target key capacity.
    ///
    /// Contains the offending normalized key.
    KeyCapacityExceeded(String),
    /// Adding the counts of a key would overflow `u64`.
    ///
    /// Contains the offending key.
    CountOverflow(String),
    /// Adding the sums of a key would overflow `i64` minor units.
    ///
    /// Contains the offending key.
    SumOverflow(String),
}

/// One key folded state: its exact total and the number of valid rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry {
    key: String,
    count: u64,
    sum: i64,
}

impl Entry {
    /// Returns the normalized business key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns how many valid rows have been folded into this key.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Returns the exact running total in minor units (integers scaled by
    /// `10^precision`).
    pub fn sum(&self) -> i64 {
        self.sum
    }
}

/// Accumulates per-key amounts parsed from CSV records.
///
/// See the [module documentation](self) for the state layout and a worked
/// example.
#[derive(Clone, Debug)]
pub struct AmountAggregator {
    config: AmountConfig,
    entries: Vec<Entry>,
    index: HashMap<String, usize>,
    next_record: u64,
}

impl AmountAggregator {
    /// Creates an empty aggregator from the given configuration.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] if the precision is larger than
    /// [`MAX_PRECISION`] or the key and amount columns coincide.
    pub fn new(config: AmountConfig) -> Result<AmountAggregator, ConfigError> {
        config.validate()?;
        let capacity = config.max_keys.unwrap_or(0);
        Ok(AmountAggregator {
            config,
            entries: Vec::with_capacity(capacity),
            index: HashMap::with_capacity(capacity),
            next_record: 1,
        })
    }

    /// Returns the configuration this aggregator was built with.
    pub fn config(&self) -> &AmountConfig {
        &self.config
    }

    /// Returns the number that will be assigned to the next record.
    ///
    /// This starts at `1` and advances by one for every
    /// [`add_record`](AmountAggregator::add_record) attempt, whether or not
    /// the row was accepted.
    pub fn record_number(&self) -> u64 {
        self.next_record
    }

    /// Feeds one record into the aggregator.
    ///
    /// The key and amount are taken from the columns named in the
    /// configuration. The state is updated only when *both* are valid; a
    /// rejected row leaves every count and sum untouched. The returned
    /// [`AggregateError`] carries the 1-based record number of the failure.
    pub fn add_record(
        &mut self,
        record: &StringRecord,
    ) -> Result<(), AggregateError> {
        let record_no = self.next_record;
        self.next_record = self.next_record.saturating_add(1);
        let fields: Vec<&str> = record.iter().collect();
        self.apply_fields(&fields, record_no)
    }

    /// Feeds one row given as a slice of string fields, assigning record
    /// numbers automatically.
    ///
    /// This is the non-`Reader` counterpart of
    /// [`add_record`](AmountAggregator::add_record) and applies exactly the
    /// same validation rules.
    pub fn add_fields<I, T>(&mut self, fields: I) -> Result<(), AggregateError>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let record_no = self.next_record;
        self.next_record = self.next_record.saturating_add(1);
        let owned: Vec<String> =
            fields.into_iter().map(|f| f.as_ref().to_string()).collect();
        let view: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
        self.apply_fields(&view, record_no)
    }

    fn apply_fields(
        &mut self,
        fields: &[&str],
        record_no: u64,
    ) -> Result<(), AggregateError> {
        let fail = |kind| AggregateError { kind, record: record_no };
        let needed =
            core::cmp::max(self.config.key_column, self.config.amount_column);
        if fields.len() <= needed {
            let missing = if fields.len() <= self.config.key_column {
                self.config.key_column
            } else {
                self.config.amount_column
            };
            return Err(fail(AggregateErrorKind::MissingColumn {
                column: missing,
                fields: fields.len(),
            }));
        }
        let key_raw = fields[self.config.key_column];
        let amount_raw = fields[self.config.amount_column];

        if key_raw.is_empty() {
            return Err(fail(AggregateErrorKind::EmptyKey));
        }
        let amount = match parse_amount(amount_raw, self.config.precision) {
            Ok(v) => v,
            Err(ParseError::Invalid) => {
                return Err(fail(AggregateErrorKind::InvalidAmount(
                    amount_raw.to_string(),
                )))
            }
            Err(ParseError::ExcessPrecision) => {
                return Err(fail(AggregateErrorKind::ExcessPrecision {
                    raw: amount_raw.to_string(),
                    precision: self.config.precision,
                }))
            }
            Err(ParseError::Overflow) => {
                return Err(fail(AggregateErrorKind::Overflow(
                    amount_raw.to_string(),
                )))
            }
        };

        // Normalization and all arithmetic below must succeed before the key
        // order vector grows, so a failing row never mutates observable state.
        let key = self.config.normalize_key(key_raw);
        self.apply(&key, amount).map_err(fail)
    }

    fn apply(
        &mut self,
        key: &str,
        amount: i64,
    ) -> Result<(), AggregateErrorKind> {
        if let Some(&i) = self.index.get(key) {
            let new_sum = match self.entries[i].sum.checked_add(amount) {
                Some(v) => v,
                None => {
                    return Err(AggregateErrorKind::Overflow(String::new()))
                }
            };
            let new_count = match self.entries[i].count.checked_add(1) {
                Some(v) => v,
                None => {
                    return Err(AggregateErrorKind::Overflow(String::new()))
                }
            };
            self.entries[i].sum = new_sum;
            self.entries[i].count = new_count;
            Ok(())
        } else {
            if let Some(max) = self.config.max_keys {
                if self.entries.len() >= max {
                    return Err(AggregateErrorKind::KeyCapacityExceeded(
                        key.to_string(),
                    ));
                }
            }
            self.index.insert(key.to_string(), self.entries.len());
            self.entries.push(Entry {
                key: key.to_string(),
                count: 1,
                sum: amount,
            });
            Ok(())
        }
    }

    /// Iterates over all keys in first-valid-appearance order.
    pub fn entries(&self) -> impl Iterator<Item = &Entry> {
        self.entries.iter()
    }

    /// Looks up the folded state of a key, applying this aggregator key
    /// normalization to the query.
    pub fn get(&self, key: &str) -> Option<&Entry> {
        let normalized = self.config.normalize_key(key);
        self.index.get(&normalized).map(|&i| &self.entries[i])
    }

    /// Returns the number of distinct keys currently stored.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if no valid record has been folded in yet.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Formats an integer minor-unit value as fixed-point text.
    ///
    /// `precision` digits are emitted after the decimal point, zero-padded as
    /// needed: `format_amount(60, 2) == "0.60"` and
    /// `format_amount(-5, 2) == "-0.05"`. With `precision == 0` no decimal
    /// point is emitted.
    pub fn format_amount(minor_units: i64, precision: u8) -> String {
        format_minor_units(minor_units, precision)
    }
}

impl AmountAggregator {
    /// Merges another aggregator state into this one.
    ///
    /// Both aggregators must use the same precision
    /// ([`AmountConfig::precision`]) and the same key normalization
    /// ([`AmountConfig::key_normalization`]); otherwise the merge is rejected
    /// before any state changes. The target key capacity still applies to
    /// keys introduced by `other`.
    ///
    /// The merge is atomic and conflict-safe: if any count addition, sum
    /// addition or new-key insertion would fail, this aggregator is left
    /// exactly as it was before the call. New keys are appended in the order
    /// in which they first appear in `other`.
    pub fn merge(
        &mut self,
        other: &AmountAggregator,
    ) -> Result<(), MergeError> {
        if self.config.precision != other.config.precision {
            return Err(MergeError {
                kind: MergeErrorKind::PrecisionMismatch {
                    target: self.config.precision,
                    other: other.config.precision,
                },
            });
        }
        if self.config.key_norm != other.config.key_norm {
            return Err(MergeError {
                kind: MergeErrorKind::KeyNormalizationMismatch,
            });
        }

        // Build the complete merged state off to the side and commit only if
        // every key merges cleanly.
        let mut merged_entries = self.entries.clone();
        let mut merged_index = self.index.clone();
        for src in &other.entries {
            match merged_index.get(&src.key).copied() {
                Some(i) => {
                    let dst = &mut merged_entries[i];
                    dst.count = dst.count.checked_add(src.count).ok_or(
                        MergeError {
                            kind: MergeErrorKind::CountOverflow(
                                src.key.clone(),
                            ),
                        },
                    )?;
                    dst.sum =
                        dst.sum.checked_add(src.sum).ok_or(MergeError {
                            kind: MergeErrorKind::SumOverflow(src.key.clone()),
                        })?;
                }
                None => {
                    if let Some(max) = self.config.max_keys {
                        if merged_entries.len() >= max {
                            return Err(MergeError {
                                kind: MergeErrorKind::KeyCapacityExceeded(
                                    src.key.clone(),
                                ),
                            });
                        }
                    }
                    merged_index.insert(src.key.clone(), merged_entries.len());
                    merged_entries.push(src.clone());
                }
            }
        }
        self.entries = merged_entries;
        self.index = merged_index;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ParseError {
    Invalid,
    ExcessPrecision,
    Overflow,
}

/// Converts a fixed-point amount text directly into signed integer minor
/// units, never touching floating point.
fn parse_amount(text: &str, precision: u8) -> Result<i64, ParseError> {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return Err(ParseError::Invalid);
    }
    let (sign, rest) = match bytes[0] {
        b'+' => (1i128, &bytes[1..]),
        b'-' => (-1i128, &bytes[1..]),
        _ => (1i128, bytes),
    };
    if rest.is_empty() {
        return Err(ParseError::Invalid);
    }

    let mut whole: i128 = 0;
    let mut frac: i128 = 0;
    let mut frac_digits: u8 = 0;
    let mut seen_dot = false;
    let mut seen_digit = false;
    for &b in rest {
        match b {
            b'0'..=b'9' => {
                seen_digit = true;
                if seen_dot {
                    if frac_digits >= precision {
                        // Significant digits beyond the allowed fractional
                        // width mean the value carries too much detail.
                        // Trailing zeros do not change the value.
                        if b != b'0' {
                            return Err(ParseError::ExcessPrecision);
                        }
                        continue;
                    }
                    frac = frac * 10 + i128::from(b - b'0');
                    frac_digits += 1;
                } else {
                    whole = whole
                        .checked_mul(10)
                        .and_then(|w| w.checked_add(i128::from(b - b'0')))
                        .ok_or(ParseError::Overflow)?;
                }
            }
            b'.' => {
                if seen_dot {
                    return Err(ParseError::Invalid);
                }
                seen_dot = true;
            }
            _ => return Err(ParseError::Invalid),
        }
    }
    if !seen_digit {
        return Err(ParseError::Invalid);
    }

    let mut scaled_frac = frac;
    for _ in frac_digits..precision {
        scaled_frac = scaled_frac.saturating_mul(10);
    }

    let value = scale_for(precision)
        .checked_mul(whole)
        .and_then(|w| w.checked_add(scaled_frac))
        .and_then(|v| v.checked_mul(sign))
        .ok_or(ParseError::Overflow)?;
    i64::try_from(value).map_err(|_| ParseError::Overflow)
}

fn scale_for(precision: u8) -> i128 {
    let mut scale: i128 = 1;
    for _ in 0..precision {
        scale *= 10;
    }
    scale
}

fn format_minor_units(value: i64, precision: u8) -> String {
    if precision == 0 {
        return value.to_string();
    }
    let neg = value.is_negative();
    let magnitude = value.unsigned_abs();
    let scale = 10u64.pow(u32::from(precision));
    let whole = magnitude / scale;
    let frac = magnitude % scale;
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    use std::fmt::Write as _;
    let _ = write!(out, "{whole}.");
    let mut width = 0u32;
    let mut probe = frac;
    while probe > 0 {
        width += 1;
        probe /= 10;
    }
    for _ in width..u32::from(precision) {
        out.push('0');
    }
    if frac != 0 {
        let _ = write!(out, "{frac}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_amount_examples() {
        assert_eq!(parse_amount("0.10", 2), Ok(10));
        assert_eq!(parse_amount("0.20", 2), Ok(20));
        assert_eq!(parse_amount("0.30", 2), Ok(30));
        assert_eq!(parse_amount("1.00", 2), Ok(100));
        assert_eq!(parse_amount("1", 2), Ok(100));
        assert_eq!(parse_amount(".5", 2), Ok(50));
        assert_eq!(parse_amount("-0.05", 2), Ok(-5));
        assert_eq!(parse_amount("+2", 0), Ok(2));
        assert_eq!(parse_amount("12.3", 2), Ok(1230));
        assert_eq!(parse_amount("0.0010", 3), Ok(1));
        assert_eq!(parse_amount("1.", 2), Ok(100));
    }

    #[test]
    fn test_parse_amount_rejects_garbage() {
        for bad in [
            "", "+", "-", ".", "x", " 1", "1 ", "1e2", "NaN", "inf", "1.2.3",
            "1,000",
        ] {
            assert_eq!(
                parse_amount(bad, 2),
                Err(ParseError::Invalid),
                "expected {bad:?} to be invalid"
            );
        }
    }

    #[test]
    fn test_parse_amount_excess_precision() {
        assert_eq!(parse_amount("0.001", 2), Err(ParseError::ExcessPrecision));
        assert_eq!(
            parse_amount("1.0001", 3),
            Err(ParseError::ExcessPrecision)
        );
        // Extra trailing zeros beyond the precision are exact, not excess.
        assert_eq!(parse_amount("0.100", 2), Ok(10));
        // Zero precision: a nonzero fraction is excess precision.
        assert_eq!(parse_amount("1.0", 0), Ok(1));
        assert_eq!(parse_amount("1.1", 0), Err(ParseError::ExcessPrecision));
    }

    #[test]
    fn test_parse_amount_overflow() {
        assert_eq!(
            parse_amount("92233720368547758.08", 2),
            Err(ParseError::Overflow)
        );
        assert_eq!(
            parse_amount("-92233720368547758.09", 2),
            Err(ParseError::Overflow)
        );
        // The i64 minor-unit extremes themselves are fine.
        assert_eq!(parse_amount("92233720368547758.07", 2), Ok(i64::MAX));
        assert_eq!(parse_amount("-92233720368547758.08", 2), Ok(i64::MIN));
    }

    #[test]
    fn test_format_minor_units() {
        assert_eq!(format_minor_units(60, 2), "0.60");
        assert_eq!(format_minor_units(100, 2), "1.00");
        assert_eq!(format_minor_units(-5, 2), "-0.05");
        assert_eq!(format_minor_units(0, 2), "0.00");
        assert_eq!(format_minor_units(7, 0), "7");
        assert_eq!(format_minor_units(-1234, 3), "-1.234");
    }

    #[test]
    fn test_config_validation() {
        assert!(AmountAggregator::new(AmountConfig::new(0, 1, 19)).is_err());
        assert!(AmountAggregator::new(AmountConfig::new(2, 2, 2)).is_err());
        assert!(AmountAggregator::new(AmountConfig::new(0, 1, 18)).is_ok());
    }
}
