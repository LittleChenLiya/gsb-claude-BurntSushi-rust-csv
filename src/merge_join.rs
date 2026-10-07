//! Library level merge joins over two sorted [`ByteRecord`]
//! streams.
//!
//! This module implements a *merge join*: both input streams must already be
//! sorted in non-decreasing byte order on one designated key column. Because
//! the inputs are sorted, matching key groups can be found with a single
//! streaming pass over each side.
//!
//! Two flavors of join are supported via [`JoinKind`]:
//!
//! * [`JoinKind::Inner`] only emits a row when a left and a right record share
//!   the same key.
//! * [`JoinKind::Left`] behaves like the inner join, but every left record
//!   without a matching right record is still emitted. Such a row carries no
//!   right record; see [`JoinRow::right`].
//!
//! When several records on one side share a key, the join emits the
//! [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) of the
//! two key groups. Rows are emitted in a stable order: they are first ordered
//! by the position of the left record, then by the position of the right
//! record within its key group.
//!
//! # Spilling
//!
//! Every matching pair of key groups is materialized before its cross product
//! is emitted. If the records accumulated for one side of a key group grow
//! beyond the configured [memory budget](MergeJoinBuilder::memory_budget), the
//! remaining records of that side of the group are spilled to a temporary file
//! inside the directory owned by a [`TempFiles`] value. The cross product is
//! then computed with an external nested-loop pass that preserves the stable
//! output order described above.
//!
//! Temporary files are always tracked through [`TempFiles`]. Files of finished
//! groups are reclaimed eagerly, and [`TempFiles::cleanup_all`] is called both
//! when a join finishes and when it fails (including sink failures, ordering
//! violations and the output limit). The returned [`CleanupReport`] makes the
//! reclamation observable. The directory itself is never deleted: it is owned
//! and managed by the caller.
//!
//! # Ordering diagnostics
//!
//! The join never silently tolerates unsorted input. If a record's key is
//! strictly smaller than the previous key on the same side, a
//! [`MergeJoinErrorKind::OutOfOrder`] error is returned that names the
//! offending [`JoinSide`] and, when available, the
//! [`Position`] of the record. Rows emitted before the
//! violation are delivered to the sink, but the caller must treat the result
//! as incomplete; the join does not pretend that the remainder of the output
//! exists.
//!
//! # Limits
//!
//! A maximum number of emitted rows can be configured with
//! [`MergeJoinBuilder::max_outputs`]. When the limit is reached, the join
//! stops reading and returns [`JoinStatus::Limit`] (with the number of rows
//! actually emitted and a cleanup report) instead of
//! [`JoinStatus::Done`].
//!
//! # Encoding
//!
//! Everything operates on [`ByteRecord`], so fields are not
//! required to be valid UTF-8. Keys are compared byte-for-byte.
//!
//! # Example
//!
//! ```
//! use csv::{
//!     merge_join::{JoinKind, MergeJoinBuilder, TempFiles},
//!     ReaderBuilder,
//! };
//!
//! // The duplicated key "a" on both sides expands into a Cartesian product.
//! let left = b"a,l0\na,l1\nb,l2\n";
//! let right = b"a,r0\na,r1\nc,r2\n";
//!
//! let mut left_rdr =
//!     ReaderBuilder::new().has_headers(false).from_reader(&left[..]);
//! let mut right_rdr =
//!     ReaderBuilder::new().has_headers(false).from_reader(&right[..]);
//!
//! let dir = std::env::temp_dir().join("csv-merge-join-doc-example");
//! let mut temps = TempFiles::new(&dir).unwrap();
//! // Rows of a left join may have different lengths, so enable flexible
//! // writing.
//! let mut out = csv::WriterBuilder::new()
//!     .flexible(true)
//!     .from_writer(vec![]);
//! let status = MergeJoinBuilder::new()
//!     .join_kind(JoinKind::Left)
//!     .run_to_writer(
//!         left_rdr.byte_records(),
//!         right_rdr.byte_records(),
//!         &mut temps,
//!         &mut out,
//!     )
//!     .unwrap();
//! assert_eq!(status.emitted(), 5);
//! assert!(status.report().cleanup().is_clean());
//!
//! let data = String::from_utf8(out.into_inner().unwrap()).unwrap();
//! assert_eq!(data, "a,l0,a,r0\na,l0,a,r1\na,l1,a,r0\na,l1,a,r1\nb,l2,\n");
//! // The caller owns the directory; the join only manages files inside it.
//! std::fs::remove_dir_all(&dir).unwrap();
//! ```

use std::{
    cmp::Ordering,
    error::Error as StdError,
    fmt,
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
};

use crate::{
    byte_record::{ByteRecord, Position},
    error::Error,
    reader::{Reader, ReaderBuilder},
    writer::{Writer, WriterBuilder},
};

/// The number of bytes charged to the memory budget for one buffered record in
/// addition to the length of its field data. This approximates allocator and
/// `ByteRecord` container overhead.
const GROUP_RECORD_OVERHEAD: usize = 64;

/// A process-wide counter used to give every spill file a unique sequence
/// number, even when several joins run concurrently.
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Which side of a merge join an input record, spill file or diagnostic
/// belongs to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum JoinSide {
    /// The left (outer, preserved in a left join) input.
    Left,
    /// The right (inner) input.
    Right,
}

impl JoinSide {
    /// Return a human readable name for this side.
    pub fn as_str(self) -> &'static str {
        match self {
            JoinSide::Left => "left",
            JoinSide::Right => "right",
        }
    }

    /// Return a short, file-name-safe tag for this side.
    pub(crate) fn as_tag(self) -> &'static str {
        match self {
            JoinSide::Left => "L",
            JoinSide::Right => "R",
        }
    }
}

impl fmt::Display for JoinSide {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The kind of merge join to perform.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum JoinKind {
    /// Emit a row only when a left record and a right record share the same
    /// key. Records on either side without a match are dropped.
    #[default]
    Inner,
    /// Like [`JoinKind::Inner`], but left records without a matching right
    /// record are emitted as well. Such rows have no right record attached.
    Left,
}

/// A single row emitted by a merge join.
///
/// The row borrows nothing from the join: both records (when present) are
/// owned, so values can be retained past the sink call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinRow {
    /// The left record.
    left: ByteRecord,
    /// The matched right record, or `None` when this is an unmatched left
    /// record emitted by a left join.
    right: Option<ByteRecord>,
}

impl JoinRow {
    /// Build a row for a matched pair of records.
    pub fn both(left: &ByteRecord, right: &ByteRecord) -> JoinRow {
        JoinRow {
            left: left.clone_truncated(),
            right: Some(right.clone_truncated()),
        }
    }

    /// Build a row for an unmatched left record of a left join.
    pub fn left_only(left: ByteRecord) -> JoinRow {
        JoinRow { left, right: None }
    }

    /// The left record of this joined row.
    pub fn left(&self) -> &ByteRecord {
        &self.left
    }

    /// The right record of this joined row, or `None` when this row was
    /// produced for an unmatched left record in a left join.
    pub fn right(&self) -> Option<&ByteRecord> {
        self.right.as_ref()
    }

    /// Consume this row and return the owned left record.
    pub fn into_left(self) -> ByteRecord {
        self.left
    }

    /// Consume this row and return the owned right record, if any.
    pub fn into_right(self) -> Option<ByteRecord> {
        self.right
    }

    /// Flatten this row into a single [`ByteRecord`].
    ///
    /// The returned record contains the fields of the left record followed by
    /// the fields of the right record. When the row has no right record (an
    /// unmatched left record in a left join), a single empty field is appended
    /// as the null marker. This mirrors the conventional CSV representation of
    /// a left join, in which unmatched right columns are written empty.
    pub fn to_byte_record(&self) -> ByteRecord {
        let cap = self.left.as_slice().len()
            + self.right.as_ref().map(|r| r.as_slice().len()).unwrap_or(0)
            + 1;
        let fields = self.left.len()
            + self.right.as_ref().map(|r| r.len()).unwrap_or(1);
        let mut out = ByteRecord::with_capacity(cap, fields);
        for field in self.left.iter() {
            out.push_field(field);
        }
        match self.right.as_ref() {
            Some(right) => {
                for field in right.iter() {
                    out.push_field(field);
                }
            }
            None => out.push_field(b""),
        }
        out
    }
}

/// Counters describing one call to [`TempFiles::cleanup_all`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CleanupReport {
    /// Number of spill files removed.
    pub removed: u64,
    /// Number of spill files that could not be removed.
    pub failures: u64,
    /// Number of spill files still known to the manager.
    pub remaining: usize,
}

impl CleanupReport {
    /// Return true if every registered spill file was removed without error.
    pub fn is_clean(&self) -> bool {
        self.failures == 0 && self.remaining == 0
    }
}

/// A summary of a finished merge join.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JoinReport {
    /// Number of rows handed to the output sink.
    emitted: u64,
    /// The cleanup result for the spill files created by the join.
    cleanup: CleanupReport,
}

impl JoinReport {
    /// The number of rows handed to the output sink.
    pub fn emitted(&self) -> u64 {
        self.emitted
    }

    /// The cleanup result for the spill files created by the join.
    pub fn cleanup(&self) -> CleanupReport {
        self.cleanup
    }
}

/// The terminal status of a merge join.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum JoinStatus {
    /// Every input record was consumed and the join ran to completion.
    Done(JoinReport),
    /// The join stopped early because the configured output limit was reached.
    /// The report only covers rows emitted before the limit.
    Limit(JoinReport),
}

impl JoinStatus {
    /// Borrow the report associated with this terminal status.
    pub fn report(&self) -> &JoinReport {
        match *self {
            JoinStatus::Done(ref report) | JoinStatus::Limit(ref report) => {
                report
            }
        }
    }

    /// The number of rows handed to the output sink.
    pub fn emitted(&self) -> u64 {
        self.report().emitted
    }

    /// Return true if this status is [`JoinStatus::Limit`].
    pub fn is_limit(&self) -> bool {
        matches!(*self, JoinStatus::Limit(_))
    }
}

/// An error that can occur while running a merge join.
///
/// Besides the underlying [`MergeJoinErrorKind`], a join error carries the
/// [`CleanupReport`] produced while reclaiming the join's spill files.
#[derive(Debug)]
pub struct MergeJoinError {
    kind: MergeJoinErrorKind,
    cleanup: Option<CleanupReport>,
}

impl MergeJoinError {
    /// Build an error from a kind.
    pub(crate) fn new(kind: MergeJoinErrorKind) -> MergeJoinError {
        MergeJoinError { kind, cleanup: None }
    }

    /// An error returned while reading an input stream.
    pub(crate) fn read(side: JoinSide, err: Error) -> MergeJoinError {
        let pos = err.position().cloned();
        MergeJoinError::new(MergeJoinErrorKind::Read { side, pos, err })
    }

    /// An error returned while re-reading a spill file.
    pub(crate) fn spill_read(side: JoinSide, err: Error) -> MergeJoinError {
        MergeJoinError::new(MergeJoinErrorKind::Spill { side, err })
    }

    /// An error returned while writing a spill file.
    pub(crate) fn spill_write(side: JoinSide, err: Error) -> MergeJoinError {
        MergeJoinError::new(MergeJoinErrorKind::Spill { side, err })
    }

    /// The input was not sorted in non-decreasing byte order.
    pub(crate) fn out_of_order(
        side: JoinSide,
        previous: &[u8],
        current: &[u8],
        pos: Option<Position>,
    ) -> MergeJoinError {
        MergeJoinError::new(MergeJoinErrorKind::OutOfOrder {
            side,
            pos,
            previous: previous.to_vec(),
            current: current.to_vec(),
        })
    }

    /// A record did not contain the configured key column.
    pub(crate) fn missing_key(
        side: JoinSide,
        column: usize,
        len: usize,
        pos: Option<Position>,
    ) -> MergeJoinError {
        MergeJoinError::new(MergeJoinErrorKind::MissingKey {
            side,
            column,
            len,
            pos,
        })
    }

    /// The caller provided output sink returned an error.
    pub(crate) fn sink<E>(err: E) -> MergeJoinError
    where
        E: StdError + Send + Sync + 'static,
    {
        MergeJoinError::new(MergeJoinErrorKind::Sink(Box::new(err)))
    }

    /// Attach the cleanup report produced while reclaiming spill files.
    pub(crate) fn with_cleanup(
        mut self,
        cleanup: CleanupReport,
    ) -> MergeJoinError {
        self.cleanup = Some(cleanup);
        self
    }

    /// The specific kind of this error.
    pub fn kind(&self) -> &MergeJoinErrorKind {
        &self.kind
    }

    /// Consume this error and return its kind.
    pub fn into_kind(self) -> MergeJoinErrorKind {
        self.kind
    }

    /// The side of the join responsible for this error, if the error is tied
    /// to one side. Sink errors return `None`.
    pub fn side(&self) -> Option<JoinSide> {
        match self.kind {
            MergeJoinErrorKind::Read { side, .. }
            | MergeJoinErrorKind::OutOfOrder { side, .. }
            | MergeJoinErrorKind::MissingKey { side, .. }
            | MergeJoinErrorKind::Spill { side, .. } => Some(side),
            MergeJoinErrorKind::Sink(_) => None,
        }
    }

    /// The position of the record responsible for this error, if known.
    pub fn position(&self) -> Option<&Position> {
        match self.kind {
            MergeJoinErrorKind::Read { ref pos, .. }
            | MergeJoinErrorKind::OutOfOrder { ref pos, .. }
            | MergeJoinErrorKind::MissingKey { ref pos, .. } => pos.as_ref(),
            MergeJoinErrorKind::Spill { .. } | MergeJoinErrorKind::Sink(_) => {
                None
            }
        }
    }

    /// The cleanup report recorded while reclaiming this join's spill files,
    /// if cleanup ran. Cleanup always runs for errors returned by
    /// [`MergeJoinBuilder::run`].
    pub fn cleanup_report(&self) -> Option<CleanupReport> {
        self.cleanup
    }
}

/// The specific kind of a [`MergeJoinError`].
#[derive(Debug)]
#[non_exhaustive]
pub enum MergeJoinErrorKind {
    /// Reading a record from one of the input streams failed.
    Read {
        /// The side whose reader failed.
        side: JoinSide,
        /// The position reported by the underlying reader, if available.
        pos: Option<Position>,
        /// The underlying CSV error.
        err: Error,
    },
    /// A record on one side was strictly smaller than the preceding record on
    /// that side, i.e. the input was not sorted in non-decreasing byte order.
    ///
    /// Any rows emitted before this violation are incomplete: the join cannot
    /// continue without violating its ordering contract.
    OutOfOrder {
        /// The side whose ordering was violated.
        side: JoinSide,
        /// The position of the offending record, if available.
        pos: Option<Position>,
        /// The key of the preceding record on that side.
        previous: Vec<u8>,
        /// The (smaller) key of the offending record.
        current: Vec<u8>,
    },
    /// A record did not contain the configured key column.
    MissingKey {
        /// The side whose record lacked the key column.
        side: JoinSide,
        /// The key column index that could not be found.
        column: usize,
        /// The number of fields in the offending record.
        len: usize,
        /// The position of the offending record, if available.
        pos: Option<Position>,
    },
    /// Reading or writing a spill file failed.
    Spill {
        /// The side whose spill file failed.
        side: JoinSide,
        /// The underlying CSV or I/O error.
        err: Error,
    },
    /// The caller provided output sink returned an error.
    Sink(Box<dyn StdError + Send + Sync>),
}

impl StdError for MergeJoinError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self.kind {
            MergeJoinErrorKind::Read { ref err, .. }
            | MergeJoinErrorKind::Spill { ref err, .. } => Some(err),
            MergeJoinErrorKind::Sink(ref err) => Some(&**err),
            MergeJoinErrorKind::OutOfOrder { .. }
            | MergeJoinErrorKind::MissingKey { .. } => None,
        }
    }
}

impl fmt::Display for MergeJoinError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.kind {
            MergeJoinErrorKind::Read { side, ref pos, ref err } => write!(
                f,
                "merge join: failed to read a record from the {} input at \
                 {}: {}",
                side,
                format_position(pos.as_ref()),
                err
            ),
            MergeJoinErrorKind::OutOfOrder {
                side,
                ref pos,
                ref previous,
                ref current,
            } => write!(
                f,
                "merge join: the {} input is not sorted in non-decreasing \
                 byte order at {}: key {:?} is smaller than the preceding \
                 key {:?}; rows emitted before this point are not the \
                 complete join result",
                side,
                format_position(pos.as_ref()),
                current,
                previous
            ),
            MergeJoinErrorKind::MissingKey { side, column, len, ref pos } => {
                write!(
                f,
                "merge join: the {} record at {} has no field at key column \
                 index {} (the record has {} field{})",
                side,
                format_position(pos.as_ref()),
                column,
                len,
                if len == 1 { "" } else { "s" }
            )
            }
            MergeJoinErrorKind::Spill { side, ref err } => write!(
                f,
                "merge join: spill file I/O failure on the {} side: {}",
                side, err
            ),
            MergeJoinErrorKind::Sink(ref err) => {
                write!(f, "merge join: the output sink failed: {}", err)
            }
        }
    }
}

/// Format an optional record position for diagnostics.
fn format_position(pos: Option<&Position>) -> String {
    match pos {
        Some(pos) => format!(
            "record {} (line {}, byte offset {})",
            pos.record(),
            pos.line(),
            pos.byte()
        ),
        None => "an unknown record position".to_string(),
    }
}
/// Tracks the spill files created by a merge join.
///
/// A `TempFiles` value owns a directory *location* (provided by the caller)
/// but never deletes the directory itself. Spill files are created inside
/// that directory, accounted for via
/// [`created`](TempFiles::created), [`removed`](TempFiles::removed) and
/// [`removal_failures`](TempFiles::removal_failures), and reclaimed either
/// eagerly when a key group is finished or through
/// [`cleanup_all`](TempFiles::cleanup_all) when the join ends or fails.
pub struct TempFiles {
    /// The caller provided directory in which spill files live.
    dir: PathBuf,
    /// Every spill file currently considered live, paired with its side.
    files: Vec<(PathBuf, JoinSide)>,
    /// Total number of spill files created over this manager's lifetime.
    created: u64,
    /// Total number of spill files successfully removed.
    removed: u64,
    /// Total number of spill file removals that failed.
    removal_failures: u64,
}

impl TempFiles {
    /// Create a new manager that places spill files in `dir`.
    ///
    /// The directory (and any missing parent directory) is created eagerly so
    /// that configuration errors surface before the join starts. The
    /// directory is never removed by this type; the caller owns its lifecycle.
    pub fn new<P: AsRef<Path>>(dir: P) -> io::Result<TempFiles> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        Ok(TempFiles {
            dir,
            files: Vec::new(),
            created: 0,
            removed: 0,
            removal_failures: 0,
        })
    }

    /// The directory in which spill files are created.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The total number of spill files created since this manager was built.
    pub fn created(&self) -> u64 {
        self.created
    }

    /// The total number of spill files successfully removed since this
    /// manager was built.
    pub fn removed(&self) -> u64 {
        self.removed
    }

    /// The total number of spill file removals that have failed.
    pub fn removal_failures(&self) -> u64 {
        self.removal_failures
    }

    /// The number of spill files currently tracked as live.
    pub fn live(&self) -> usize {
        self.files.len()
    }

    /// Allocate a fresh, unique spill file path for the given side.
    ///
    /// The file itself is not created or opened here. This is part of the
    /// observable cleanup interface: callers can account for files they create
    /// themselves in the managed directory, or inspect files created on their
    /// behalf by a join.
    pub fn create(&mut self, side: JoinSide) -> PathBuf {
        let sequence = TEMP_SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed);
        let path = self.dir.join(format!(
            "csv-merge-join-{}-{}-{}.tmp",
            std::process::id(),
            sequence,
            side.as_tag()
        ));
        self.files.push((path.clone(), side));
        self.created += 1;
        path
    }

    /// Delete a single tracked spill file and forget it.
    ///
    /// Return true on success. This is part of the observable cleanup
    /// interface and permits reclaiming files one at a time instead of through
    /// [`TempFiles::cleanup_all`].
    pub fn remove(&mut self, path: &Path) -> bool {
        if let Some(index) = self.files.iter().position(|(p, _)| p == path) {
            self.files.swap_remove(index);
        }
        match fs::remove_file(path) {
            Ok(()) => {
                self.removed += 1;
                true
            }
            Err(_) => {
                self.removal_failures += 1;
                false
            }
        }
    }

    /// Remove every spill file currently tracked and return a report.
    ///
    /// Calling this twice simply reports zero work on the second call, as long
    /// as the first call removed everything.
    pub fn cleanup_all(&mut self) -> CleanupReport {
        let files = std::mem::take(&mut self.files);
        for (path, side) in files {
            if fs::remove_file(&path).is_ok() {
                self.removed += 1;
            } else {
                self.removal_failures += 1;
                self.files.push((path, side));
            }
        }
        CleanupReport {
            removed: self.removed,
            failures: self.removal_failures,
            remaining: self.files.len(),
        }
    }
}

impl fmt::Debug for TempFiles {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TempFiles")
            .field("dir", &self.dir)
            .field("live", &self.files.len())
            .field("created", &self.created)
            .field("removed", &self.removed)
            .field("removal_failures", &self.removal_failures)
            .finish()
    }
}
/// A builder describing and running a merge join.
#[derive(Clone, Debug)]
pub struct MergeJoinBuilder {
    /// Zero-based index of the key column on both sides.
    key_col: usize,
    /// Inner or left join.
    kind: JoinKind,
    /// Per-side, per-key-group memory budget in bytes.
    memory_budget: usize,
    /// Maximum number of rows handed to the sink, or u64::MAX for no limit.
    max_outputs: u64,
}

impl Default for MergeJoinBuilder {
    fn default() -> MergeJoinBuilder {
        MergeJoinBuilder {
            key_col: 0,
            kind: JoinKind::Inner,
            memory_budget: 1 << 20,
            max_outputs: u64::MAX,
        }
    }
}

impl MergeJoinBuilder {
    /// Create a new builder with default settings: an inner join keyed on
    /// column `0`, a 1 MiB per-group memory budget and no output limit.
    pub fn new() -> MergeJoinBuilder {
        MergeJoinBuilder::default()
    }

    /// Set the zero-based column index used as the join key on both sides.
    ///
    /// Records lacking this column cause a
    /// [`MergeJoinErrorKind::MissingKey`] error.
    pub fn key_column(&mut self, column: usize) -> &mut MergeJoinBuilder {
        self.key_col = column;
        self
    }

    /// Set the kind of join to perform.
    pub fn join_kind(&mut self, kind: JoinKind) -> &mut MergeJoinBuilder {
        self.kind = kind;
        self
    }

    /// Select an inner join.
    pub fn inner(&mut self) -> &mut MergeJoinBuilder {
        self.join_kind(JoinKind::Inner)
    }

    /// Select a left join.
    pub fn left(&mut self) -> &mut MergeJoinBuilder {
        self.join_kind(JoinKind::Left)
    }

    /// Set the approximate per-side, per-key-group memory budget in bytes.
    ///
    /// While a key group fits in the budget its records stay in memory. Once
    /// the budget is exceeded, remaining records of that side of the group are
    /// streamed to a spill file. A budget of `0` forces every record past the
    /// first in a non-trivial group to spill; the first record of every group
    /// is always retained in memory so that empty groups are distinguishable.
    pub fn memory_budget(&mut self, bytes: usize) -> &mut MergeJoinBuilder {
        self.memory_budget = bytes;
        self
    }

    /// Set the maximum number of rows handed to the output sink.
    ///
    /// When the limit is reached, the join stops and returns
    /// [`JoinStatus::Limit`]. A limit of `0` stops before the first row is
    /// emitted. The default is effectively unlimited.
    pub fn max_outputs(&mut self, max: u64) -> &mut MergeJoinBuilder {
        self.max_outputs = max;
        self
    }

    /// Run the merge join, handing every [`JoinRow`] to `sink`.
    ///
    /// `left` and `right` must yield records sorted in non-decreasing byte
    /// order on the configured key column. Spill files are managed through
    /// `temps`.
    pub fn run<L, R, S, E>(
        &self,
        left: L,
        right: R,
        temps: &mut TempFiles,
        sink: &mut S,
    ) -> Result<JoinStatus, MergeJoinError>
    where
        L: IntoIterator<Item = Result<ByteRecord, Error>>,
        R: IntoIterator<Item = Result<ByteRecord, Error>>,
        S: FnMut(JoinRow) -> Result<(), E>,
        E: StdError + Send + Sync + 'static,
    {
        match self.run_impl(left, right, temps, sink) {
            Ok(status) => Ok(status),
            Err(err) => {
                // Locals of `run_impl` (including open spill writers) have been
                // dropped by the time we get here, so all file handles are
                // closed.
                let report = temps.cleanup_all();
                Err(err.with_cleanup(report))
            }
        }
    }

    /// Run the merge join and write flattened rows to a [`Writer`].
    ///
    /// This is a thin convenience wrapper around
    /// [`run`](MergeJoinBuilder::run): each [`JoinRow`] is converted with
    /// [`JoinRow::to_byte_record`] and written to `wtr`. Enable flexible
    /// writing on the writer if joined rows may have differing field counts.
    pub fn run_to_writer<L, R, W>(
        &self,
        left: L,
        right: R,
        temps: &mut TempFiles,
        wtr: &mut Writer<W>,
    ) -> Result<JoinStatus, MergeJoinError>
    where
        L: IntoIterator<Item = Result<ByteRecord, Error>>,
        R: IntoIterator<Item = Result<ByteRecord, Error>>,
        W: io::Write,
    {
        self.run(left, right, temps, &mut |row: JoinRow| {
            let out = row.to_byte_record();
            wtr.write_byte_record(&out)
        })
    }

    /// The actual join driver; its error return is cleaned up by `run`.
    fn run_impl<L, R, S, E>(
        &self,
        left: L,
        right: R,
        temps: &mut TempFiles,
        sink: &mut S,
    ) -> Result<JoinStatus, MergeJoinError>
    where
        L: IntoIterator<Item = Result<ByteRecord, Error>>,
        R: IntoIterator<Item = Result<ByteRecord, Error>>,
        S: FnMut(JoinRow) -> Result<(), E>,
        E: StdError + Send + Sync + 'static,
    {
        let mut left_side = SideState::new(left.into_iter(), self.key_col);
        let mut right_side = SideState::new(right.into_iter(), self.key_col);
        let mut emitted: u64 = 0;

        left_side.advance(JoinSide::Left)?;
        right_side.advance(JoinSide::Right)?;

        loop {
            match (left_side.key(), right_side.key()) {
                // No more left records: nothing else can match, and a left
                // join has no more unmatched left rows to emit.
                (None, _) => break,
                // Left records remain but the right side is exhausted: emit
                // (left join) or discard (inner join) each remaining left
                // group.
                (Some(lkey), None) => {
                    let lkey = lkey.to_vec();
                    if let Flow::Stop = self.process_left_only_group(
                        &lkey,
                        &mut left_side,
                        sink,
                        &mut emitted,
                    )? {
                        return Ok(self.finish_limit(emitted, temps));
                    }
                }
                (Some(lkey), Some(rkey)) => match lkey.cmp(rkey) {
                    Ordering::Equal => {
                        let key = lkey.to_vec();
                        let mut left_group =
                            Group::new(key.clone(), self.memory_budget);
                        let mut right_group =
                            Group::new(key.clone(), self.memory_budget);
                        left_side.read_group_into(
                            &mut left_group,
                            JoinSide::Left,
                            temps,
                        )?;
                        right_side.read_group_into(
                            &mut right_group,
                            JoinSide::Right,
                            temps,
                        )?;
                        left_group.finish(JoinSide::Left)?;
                        right_group.finish(JoinSide::Right)?;

                        let flow = emit_cross(
                            &left_group,
                            &right_group,
                            sink,
                            self.max_outputs,
                            &mut emitted,
                        )?;
                        left_group.cleanup(temps);
                        right_group.cleanup(temps);
                        if let Flow::Stop = flow {
                            return Ok(self.finish_limit(emitted, temps));
                        }
                    }
                    Ordering::Less => {
                        let lkey = lkey.to_vec();
                        if let Flow::Stop = self.process_left_only_group(
                            &lkey,
                            &mut left_side,
                            sink,
                            &mut emitted,
                        )? {
                            return Ok(self.finish_limit(emitted, temps));
                        }
                    }
                    Ordering::Greater => {
                        // The right group cannot match anything: all left keys
                        // are currently greater, and keys never shrink.
                        let rkey = rkey.to_vec();
                        skip_group(&rkey, &mut right_side, JoinSide::Right)?;
                    }
                },
            }
        }

        Ok(JoinStatus::Done(JoinReport {
            emitted,
            cleanup: temps.cleanup_all(),
        }))
    }

    /// Build a terminal [`JoinStatus::Limit`] with a fresh cleanup report.
    fn finish_limit(&self, emitted: u64, temps: &mut TempFiles) -> JoinStatus {
        JoinStatus::Limit(JoinReport { emitted, cleanup: temps.cleanup_all() })
    }

    /// Emit (left join) or discard (inner join) a group of left records whose
    /// key has no right match.
    fn process_left_only_group<I, S, E>(
        &self,
        key: &[u8],
        side: &mut SideState<I>,
        sink: &mut S,
        emitted: &mut u64,
    ) -> Result<Flow, MergeJoinError>
    where
        I: Iterator<Item = Result<ByteRecord, Error>>,
        S: FnMut(JoinRow) -> Result<(), E>,
        E: StdError + Send + Sync + 'static,
    {
        loop {
            let record = side.take();
            if self.kind == JoinKind::Left {
                if let Flow::Stop = emit_row(
                    JoinRow::left_only(record),
                    sink,
                    self.max_outputs,
                    emitted,
                )? {
                    return Ok(Flow::Stop);
                }
            }
            side.advance(JoinSide::Left)?;
            match side.key() {
                Some(next) if next == key => continue,
                _ => return Ok(Flow::Continue),
            }
        }
    }
}

/// Discard every remaining record belonging to the current key group.
fn skip_group<I>(
    key: &[u8],
    side: &mut SideState<I>,
    label: JoinSide,
) -> Result<(), MergeJoinError>
where
    I: Iterator<Item = Result<ByteRecord, Error>>,
{
    loop {
        side.take();
        side.advance(label)?;
        match side.key() {
            Some(next) if next == key => continue,
            _ => return Ok(()),
        }
    }
}

/// Internal flow control for the output limit.
enum Flow {
    Continue,
    Stop,
}

/// Hand one row to the sink, honoring the output limit.
fn emit_row<S, E>(
    row: JoinRow,
    sink: &mut S,
    max_outputs: u64,
    emitted: &mut u64,
) -> Result<Flow, MergeJoinError>
where
    S: FnMut(JoinRow) -> Result<(), E>,
    E: StdError + Send + Sync + 'static,
{
    if *emitted >= max_outputs {
        return Ok(Flow::Stop);
    }
    sink(row).map_err(MergeJoinError::sink)?;
    *emitted += 1;
    if *emitted >= max_outputs {
        Ok(Flow::Stop)
    } else {
        Ok(Flow::Continue)
    }
}

/// Emit the Cartesian product of a matched pair of key groups.
///
/// Left records drive the outer loop and right records the inner loop, which
/// yields the required stable order: left position first, then right position.
fn emit_cross<S, E>(
    left_group: &Group,
    right_group: &Group,
    sink: &mut S,
    max_outputs: u64,
    emitted: &mut u64,
) -> Result<Flow, MergeJoinError>
where
    S: FnMut(JoinRow) -> Result<(), E>,
    E: StdError + Send + Sync + 'static,
{
    for left in &left_group.mem {
        if let Flow::Stop =
            emit_right_for(left, right_group, sink, max_outputs, emitted)?
        {
            return Ok(Flow::Stop);
        }
    }
    if let Some(path) = left_group.spill_path.as_ref() {
        let mut reader = spill_reader(path, JoinSide::Left)?;
        let mut record = ByteRecord::new();
        while reader
            .read_byte_record(&mut record)
            .map_err(|err| MergeJoinError::spill_read(JoinSide::Left, err))?
        {
            let left = record.clone_truncated();
            if let Flow::Stop =
                emit_right_for(&left, right_group, sink, max_outputs, emitted)?
            {
                return Ok(Flow::Stop);
            }
        }
    }
    Ok(Flow::Continue)
}

/// Emit the cross product of one left record with the whole right group.
///
/// When the right group was spilled, each call reopens its spill file and
/// reads it once from the start. This external nested-loop strategy is what
/// keeps the emitted order stable (left position, then right position) without
/// buffering the join output.
fn emit_right_for<S, E>(
    left: &ByteRecord,
    right_group: &Group,
    sink: &mut S,
    max_outputs: u64,
    emitted: &mut u64,
) -> Result<Flow, MergeJoinError>
where
    S: FnMut(JoinRow) -> Result<(), E>,
    E: StdError + Send + Sync + 'static,
{
    for right in &right_group.mem {
        if let Flow::Stop =
            emit_row(JoinRow::both(left, right), sink, max_outputs, emitted)?
        {
            return Ok(Flow::Stop);
        }
    }
    if let Some(path) = right_group.spill_path.as_ref() {
        let mut reader = spill_reader(path, JoinSide::Right)?;
        let mut record = ByteRecord::new();
        while reader
            .read_byte_record(&mut record)
            .map_err(|err| MergeJoinError::spill_read(JoinSide::Right, err))?
        {
            let right = record.clone_truncated();
            if let Flow::Stop = emit_row(
                JoinRow::both(left, &right),
                sink,
                max_outputs,
                emitted,
            )? {
                return Ok(Flow::Stop);
            }
        }
    }
    Ok(Flow::Continue)
}

/// Open a spill file for reading with the same lenient settings used while
/// writing it.
fn spill_reader(
    path: &Path,
    side: JoinSide,
) -> Result<Reader<File>, MergeJoinError> {
    ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_path(path)
        .map_err(|err| MergeJoinError::spill_read(side, err))
}

/// One side of the join: its input iterator, the current record and its key,
/// and the key used for the preceding ordering check.
struct SideState<I> {
    iter: I,
    key_col: usize,
    current: Option<ByteRecord>,
    key: Option<Vec<u8>>,
    previous_key: Option<Vec<u8>>,
    seen_any: bool,
}

impl<I> SideState<I>
where
    I: Iterator<Item = Result<ByteRecord, Error>>,
{
    /// Build a new side state keyed on `key_col`.
    fn new(iter: I, key_col: usize) -> SideState<I> {
        SideState {
            iter,
            key_col,
            current: None,
            key: None,
            previous_key: None,
            seen_any: false,
        }
    }

    /// The key of the current record, or `None` at end of input.
    fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// Take ownership of the current record.
    fn take(&mut self) -> ByteRecord {
        self.key = None;
        self.current.take().expect("take called with no current record")
    }

    /// Pull the next record from the input, validating its key and ordering.
    fn advance(&mut self, side: JoinSide) -> Result<(), MergeJoinError> {
        match self.iter.next() {
            None => {
                self.current = None;
                self.key = None;
                Ok(())
            }
            Some(Err(err)) => Err(MergeJoinError::read(side, err)),
            Some(Ok(record)) => {
                let position = record.position().cloned();
                let key = match record.get(self.key_col) {
                    Some(key) => key.to_vec(),
                    None => {
                        let len = record.len();
                        return Err(MergeJoinError::missing_key(
                            side,
                            self.key_col,
                            len,
                            position,
                        ));
                    }
                };
                if self.seen_any {
                    let previous = self.previous_key.as_deref().unwrap();
                    if key.as_slice() < previous {
                        return Err(MergeJoinError::out_of_order(
                            side, previous, &key, position,
                        ));
                    }
                }
                self.seen_any = true;
                self.previous_key = Some(key.clone());
                self.key = Some(key);
                self.current = Some(record);
                Ok(())
            }
        }
    }

    /// Move every record sharing `group`'s key into `group`.
    fn read_group_into(
        &mut self,
        group: &mut Group,
        side: JoinSide,
        temps: &mut TempFiles,
    ) -> Result<(), MergeJoinError> {
        while let Some(key) = self.key() {
            if key != group.key {
                break;
            }
            let record = self.take();
            group.push(record, side, temps)?;
            self.advance(side)?;
        }
        Ok(())
    }
}

/// The records of one side of one matched key group, partly in memory and
/// optionally streamed into a spill file once the budget is exceeded.
struct Group {
    /// The shared key of the group.
    key: Vec<u8>,
    /// Memory budget in bytes.
    budget: usize,
    /// Approximate bytes consumed by the records kept in memory.
    mem_bytes: usize,
    /// Records retained in memory.
    mem: Vec<ByteRecord>,
    /// Lazily opened spill writer.
    spill: Option<Writer<File>>,
    /// Path of the spill file once created.
    spill_path: Option<PathBuf>,
}

impl Group {
    /// Create an empty group for `key` with the given memory budget.
    fn new(key: Vec<u8>, budget: usize) -> Group {
        Group {
            key,
            budget,
            mem_bytes: 0,
            mem: Vec::new(),
            spill: None,
            spill_path: None,
        }
    }

    /// Add a record to the group, spilling it if the budget is exceeded.
    ///
    /// The first record of a group is always kept in memory even when the
    /// budget is zero; this guarantees that groups of size one never create a
    /// spill file and that every group has at least one record buffered.
    fn push(
        &mut self,
        record: ByteRecord,
        side: JoinSide,
        temps: &mut TempFiles,
    ) -> Result<(), MergeJoinError> {
        let size = GROUP_RECORD_OVERHEAD + record.as_slice().len();
        if self.spill.is_none()
            && (self.mem.is_empty() || self.mem_bytes + size <= self.budget)
        {
            self.mem_bytes += size;
            self.mem.push(record);
            Ok(())
        } else {
            if self.spill.is_none() {
                let path = temps.create(side);
                let writer = WriterBuilder::new()
                    .has_headers(false)
                    .flexible(true)
                    .from_path(&path)
                    .map_err(|err| MergeJoinError::spill_write(side, err))?;
                self.spill_path = Some(path);
                self.spill = Some(writer);
            }
            self.spill
                .as_mut()
                .expect("spill writer initialized")
                .write_byte_record(&record)
                .map_err(|err| MergeJoinError::spill_write(side, err))
        }
    }

    /// Finish and close the spill writer, if any, so the file can be reopened
    /// for reading.
    fn finish(&mut self, side: JoinSide) -> Result<(), MergeJoinError> {
        if let Some(writer) = self.spill.take() {
            let file = writer.into_inner().map_err(|err| {
                MergeJoinError::spill_write(side, err.into_error().into())
            })?;
            drop(file);
        }
        Ok(())
    }

    /// Delete the group's spill file, if any.
    fn cleanup(&self, temps: &mut TempFiles) {
        if let Some(path) = self.spill_path.as_ref() {
            temps.remove(path);
        }
    }
}
