//! Merge-style joins over two sorted streams of
//! [`ByteRecord`](crate::ByteRecord)s.
//!
//! A *merge join* takes two input streams that are each sorted in ascending
//! byte order on one key column, and joins them on equal keys. Both
//! [`inner`](JoinKind::Inner) and [`left`](JoinKind::Left) joins are
//! supported. Records sharing a key produce the full cross product: every
//! left record is paired with every matching right record, emitted in a
//! stable order (left record order first, then right record order). A left
//! join additionally emits one row per unmatched left record, paired with
//! `None`.
//!
//! Keys and fields are compared and copied as raw bytes, so inputs never
//! need to be valid UTF-8.
//!
//! # Large groups and spilling
//!
//! A group of records with the same key can be arbitrarily large. The join
//! bounds the record bytes kept resident in memory for a single group on
//! each side (see [`MergeJoinBuilder::memory_budget`]). Once a group grows
//! beyond the budget, its remaining records are appended to a spill file
//! inside the caller-provided temporary directory
//! ([`MergeJoinBuilder::spill_dir`]). Spilled groups still produce rows in
//! the same stable order: the left group is streamed once, and the right
//! group is rescanned for every left record.
//!
//! Spill files are reclaimed when the join finishes, when it stops early
//! because of an output limit, and when it fails. Reclamation is observable:
//! every removed file is reported through
//! [`MergeJoinBuilder::with_cleanup_observer`] and summarized in the
//! [`CleanupReport`] returned by [`MergeJoin::finish`]. Dropping a
//! [`MergeJoin`] without calling `finish` also removes any remaining files.
//!
//! # Ordering violations
//!
//! Both streams are validated as they are consumed. If a record's key is
//! smaller than the preceding key on the same side, iteration stops with
//! [`JoinError::OutOfOrder`], which reports the offending side and the
//! record's position. The join deliberately does not continue, because the
//! remaining output could not be trusted to be complete or correctly
//! ordered.
//!
//! # Example
//!
//! ```
//! use csv::{ByteRecord, JoinKind, MergeJoinBuilder};
//!
//! let left = vec![
//!     ByteRecord::from(vec!["a", "l1"]),
//!     ByteRecord::from(vec!["a", "l2"]),
//!     ByteRecord::from(vec!["b", "l3"]),
//! ];
//! let right = vec![
//!     ByteRecord::from(vec!["a", "r1"]),
//!     ByteRecord::from(vec!["a", "r2"]),
//!     ByteRecord::from(vec!["c", "r3"]),
//! ];
//!
//! let dir = std::env::temp_dir().join(format!("csv-doc-{}", std::process::id()));
//! std::fs::create_dir_all(&dir).unwrap();
//! let mut join = MergeJoinBuilder::new()
//!     .kind(JoinKind::Left)
//!     .spill_dir(&dir)
//!     .memory_budget(0) // force every duplicate key to spill
//!     .build(left.into_iter().map(Ok), right.into_iter().map(Ok));
//!
//! let rows: Vec<(ByteRecord, Option<ByteRecord>)> =
//!     join.by_ref().map(|r| r.unwrap()).collect();
//! let status = join.finish().unwrap();
//! assert_eq!(status.rows_emitted(), 5);
//! assert_eq!(rows[0].0, vec!["a", "l1"]);
//! assert_eq!(rows[0].1.as_ref().unwrap(), &vec!["a", "r1"]);
//! assert_eq!(rows[1].0, vec!["a", "l1"]);
//! assert_eq!(rows[1].1.as_ref().unwrap(), &vec!["a", "r2"]);
//! assert_eq!(rows[2].0, vec!["a", "l2"]);
//! assert_eq!(rows[2].1.as_ref().unwrap(), &vec!["a", "r1"]);
//! assert_eq!(rows[3].0, vec!["a", "l2"]);
//! assert_eq!(rows[3].1.as_ref().unwrap(), &vec!["a", "r2"]);
//! // The unmatched "b" is emitted once with an empty right side.
//! assert_eq!(rows[4].0, vec!["b", "l3"]);
//! assert!(rows[4].1.is_none());
//! # std::fs::remove_dir_all(&dir).unwrap();
//! ```

use std::{
    cmp::Ordering,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use crate::{byte_record::Position, ByteRecord};

/// The kind of merge join to perform.
///
/// Configure it with [`MergeJoinBuilder::kind`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum JoinKind {
    /// An inner join: a row is emitted only when a left record and a right
    /// record share the same key.
    #[default]
    Inner,
    /// A left join: like an inner join, except every left record without a
    /// matching right record is emitted once with an empty right side.
    Left,
}

/// Which input side a record came from.
///
/// Reported by [`JoinError::OutOfOrder`] and [`JoinError::MissingKey`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum JoinSide {
    /// The left input stream.
    Left,
    /// The right input stream.
    Right,
}

impl JoinSide {
    fn label(self) -> &'static str {
        match self {
            JoinSide::Left => "left",
            JoinSide::Right => "right",
        }
    }
}

/// Why a join stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StopReason {
    /// Every group was processed and all rows were emitted.
    Completed,
    /// The limit configured with [`MergeJoinBuilder::max_outputs`] was
    /// reached; the output is a prefix of the complete join result.
    LimitReached,
}

/// A summary returned by [`MergeJoin::finish`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinStatus {
    rows: u64,
    reason: StopReason,
    cleanup: CleanupReport,
}

impl JoinStatus {
    /// The number of joined rows emitted.
    pub fn rows_emitted(&self) -> u64 {
        self.rows
    }

    /// Whether the join stopped because the output limit was reached.
    pub fn stopped_at_limit(&self) -> bool {
        self.reason == StopReason::LimitReached
    }

    /// Why the join stopped.
    pub fn stop_reason(&self) -> StopReason {
        self.reason
    }

    /// How spill files were reclaimed.
    pub fn cleanup(&self) -> &CleanupReport {
        &self.cleanup
    }
}

/// A report describing spill file reclamation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CleanupReport {
    removed: usize,
    missing: usize,
    failures: Vec<(PathBuf, String)>,
}

impl CleanupReport {
    /// Number of spill files removed successfully.
    pub fn removed(&self) -> usize {
        self.removed
    }

    /// Number of spill files that were already gone when reclaimed.
    pub fn missing(&self) -> usize {
        self.missing
    }

    /// Failures encountered while removing spill files, as
    /// `(path, error message)` pairs.
    pub fn failures(&self) -> &[(PathBuf, String)] {
        &self.failures
    }
}

/// An error produced while running a [`MergeJoin`].
#[derive(Debug)]
#[non_exhaustive]
pub enum JoinError {
    /// An input stream returned an error. The wrapped value is the original
    /// [`crate::Error`] (for example, a CSV parse or reader I/O error).
    Input(Box<crate::Error>),
    /// An I/O error occurred while writing or reading a spill file.
    Io(io::Error),
    /// A record did not contain the configured key column.
    MissingKey {
        /// The side whose record was missing the key column.
        side: JoinSide,
        /// Zero-based index of the record on that side.
        record_index: u64,
        /// Reader position of the record, if known.
        position: Option<Position>,
    },
    /// An input record was out of ascending key order. The join stops
    /// because the remainder of the result could not be trusted.
    OutOfOrder {
        /// The side on which the violation occurred.
        side: JoinSide,
        /// Zero-based index of the offending record on that side.
        record_index: u64,
        /// Reader position of the offending record, if known.
        position: Option<Position>,
    },
    /// A fatal error was already reported on this join; it is no longer
    /// usable.
    Consumed,
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            JoinError::Input(ref err) => {
                write!(f, "merge join input error: {}", err)
            }
            JoinError::Io(ref err) => {
                write!(f, "merge join spill file I/O error: {}", err)
            }
            JoinError::MissingKey { side, record_index, .. } => write!(
                f,
                "merge join error: {} record #{} is missing key column",
                side.label(),
                record_index
            ),
            JoinError::OutOfOrder { side, record_index, ref position } => {
                write!(
                    f,
                    "merge join error: {} input not sorted ascending by key \
                     bytes; out-of-order record #{}",
                    side.label(),
                    record_index
                )?;
                if let Some(pos) = position {
                    write!(
                        f,
                        " (reader record {}, line {}, byte offset {})",
                        pos.record(),
                        pos.line(),
                        pos.byte()
                    )?;
                }
                write!(f, "; output is incomplete")
            }
            JoinError::Consumed => {
                write!(f, "merge join error: join already failed")
            }
        }
    }
}

impl std::error::Error for JoinError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            JoinError::Input(ref err) => Some(&**err),
            JoinError::Io(ref err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for JoinError {
    fn from(err: io::Error) -> JoinError {
        JoinError::Io(err)
    }
}

/// Configures and builds a [`MergeJoin`].
///
/// # Example
///
/// ```
/// use csv::{JoinKind, MergeJoinBuilder};
///
/// let builder = MergeJoinBuilder::new()
///     .kind(JoinKind::Left)
///     .key(0)
///     .memory_budget(8 * 1024)
///     .spill_dir(std::env::temp_dir())
///     .max_outputs(1_000_000);
/// let _ = builder;
/// ```
pub struct MergeJoinBuilder {
    kind: JoinKind,
    left_key: usize,
    right_key: usize,
    memory_budget: usize,
    spill_dir: Option<PathBuf>,
    max_outputs: Option<u64>,
    observer: Option<Box<dyn FnMut(&Path)>>,
}

impl Default for MergeJoinBuilder {
    fn default() -> MergeJoinBuilder {
        MergeJoinBuilder::new()
    }
}

impl fmt::Debug for MergeJoinBuilder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("MergeJoinBuilder")
            .field("kind", &self.kind)
            .field("left_key", &self.left_key)
            .field("right_key", &self.right_key)
            .field("memory_budget", &self.memory_budget)
            .field("spill_dir", &self.spill_dir)
            .field("max_outputs", &self.max_outputs)
            .finish_non_exhaustive()
    }
}

impl MergeJoinBuilder {
    /// Create a new builder: an inner join on column 0 of both streams,
    /// a 1 MiB per-group memory budget, spill files in the system temp
    /// directory and no output limit.
    pub fn new() -> MergeJoinBuilder {
        MergeJoinBuilder {
            kind: JoinKind::Inner,
            left_key: 0,
            right_key: 0,
            memory_budget: 1 << 20,
            spill_dir: None,
            max_outputs: None,
            observer: None,
        }
    }

    /// Set the join kind.
    pub fn kind(mut self, kind: JoinKind) -> MergeJoinBuilder {
        self.kind = kind;
        self
    }

    /// Set the key column for both inputs. Use
    /// [`left_key`](MergeJoinBuilder::left_key) and
    /// [`right_key`](MergeJoinBuilder::right_key) when the columns differ.
    pub fn key(mut self, column: usize) -> MergeJoinBuilder {
        self.left_key = column;
        self.right_key = column;
        self
    }

    /// Set the left input key column.
    pub fn left_key(mut self, column: usize) -> MergeJoinBuilder {
        self.left_key = column;
        self
    }

    /// Set the right input key column.
    pub fn right_key(mut self, column: usize) -> MergeJoinBuilder {
        self.right_key = column;
        self
    }

    /// Set the approximate maximum resident size, in bytes, of one
    /// equal-key group on each side.
    ///
    /// When a group grows beyond the budget, its later records spill to a
    /// temporary file. A budget of 0 forces every record after the first in
    /// a duplicate-key group to spill. Budgets are independent per side.
    pub fn memory_budget(mut self, bytes: usize) -> MergeJoinBuilder {
        self.memory_budget = bytes;
        self
    }

    /// Set the directory in which spill files are created.
    ///
    /// The directory is created if missing. Spill file paths are derived
    /// from it and managed by the join; the directory itself belongs to the
    /// caller. Without this option, the system temporary directory is used.
    pub fn spill_dir<P: Into<PathBuf>>(mut self, dir: P) -> MergeJoinBuilder {
        self.spill_dir = Some(dir.into());
        self
    }

    /// Limit the number of joined rows emitted.
    ///
    /// On reaching the limit, iteration ends,
    /// [`JoinStatus::stop_reason`] is [`StopReason::LimitReached`] and every
    /// spill file is cleaned up. Remaining input is left unconsumed.
    pub fn max_outputs(mut self, max: u64) -> MergeJoinBuilder {
        self.max_outputs = Some(max);
        self
    }

    /// Register a callback invoked once per reclaimed spill file, whether
    /// the join completes, fails or stops at the output limit.
    pub fn with_cleanup_observer<F>(mut self, observer: F) -> MergeJoinBuilder
    where
        F: FnMut(&Path) + 'static,
    {
        self.observer = Some(Box::new(observer));
        self
    }

    /// Build the join from two iterators of `Result<ByteRecord, _>`.
    ///
    /// [`Reader::byte_records`](crate::Reader::byte_records) works directly,
    /// as does `vec![...].into_iter().map(Ok)`. Both streams must be sorted
    /// ascending (byte-wise) on their configured key columns.
    pub fn build<L, R, E>(
        self,
        left: L,
        right: R,
    ) -> MergeJoin<L::IntoIter, R::IntoIter>
    where
        L: IntoIterator<Item = Result<ByteRecord, E>>,
        R: IntoIterator<Item = Result<ByteRecord, E>>,
        E: Into<crate::Error>,
    {
        let spill_dir = self.spill_dir.unwrap_or_else(std::env::temp_dir);
        MergeJoin {
            kind: self.kind,
            left: Source::new(left.into_iter(), JoinSide::Left, self.left_key),
            right: Source::new(
                right.into_iter(),
                JoinSide::Right,
                self.right_key,
            ),
            emitter: None,
            temps: TempMgr {
                dir: spill_dir,
                next_id: 0,
                entries: Vec::new(),
                report: CleanupReport::default(),
                observer: self.observer,
            },
            memory_budget: self.memory_budget,
            max_outputs: self.max_outputs,
            emitted: 0,
            phase: Phase::Advance,
            failed: false,
        }
    }
}

/// The outer state of the join state machine.
#[derive(Debug)]
enum Phase {
    /// Pull the next equal-key groups from the inputs.
    Advance,
    /// Emit rows from the current pair of groups.
    Emitting,
    /// Nothing left to do.
    Done(StopReason),
}

/// A merge join over two sorted [`ByteRecord`] streams.
///
/// Get rows with iteration (`for row in &mut join`) or with
/// [`next_row`](MergeJoin::next_row), then call
/// [`finish`](MergeJoin::finish) to reclaim spill files and obtain the
/// [`JoinStatus`]. Dropping the join also removes remaining spill files.
pub struct MergeJoin<L, R> {
    kind: JoinKind,
    left: Source<L>,
    right: Source<R>,
    emitter: Option<Emitter>,
    temps: TempMgr,
    memory_budget: usize,
    max_outputs: Option<u64>,
    emitted: u64,
    phase: Phase,
    failed: bool,
}

impl<L, R> fmt::Debug for MergeJoin<L, R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("MergeJoin")
            .field("kind", &self.kind)
            .field("memory_budget", &self.memory_budget)
            .field("max_outputs", &self.max_outputs)
            .field("rows_emitted", &self.emitted)
            .field("phase", &self.phase)
            .finish_non_exhaustive()
    }
}

impl<L, R, E> MergeJoin<L, R>
where
    L: Iterator<Item = Result<ByteRecord, E>>,
    R: Iterator<Item = Result<ByteRecord, E>>,
    E: Into<crate::Error>,
{
    /// Advance the join and return the next joined row.
    ///
    /// A joined row is `(left_record, right_record)`; the right value is
    /// `None` for unmatched rows of a left join. `Ok(None)` marks the end.
    /// On error the join is finished and its spill files are reclaimed;
    /// later calls return [`JoinError::Consumed`].
    pub fn next_row(
        &mut self,
    ) -> Result<Option<(ByteRecord, Option<ByteRecord>)>, JoinError> {
        if self.failed {
            return Err(JoinError::Consumed);
        }
        if let Phase::Done(_) = self.phase {
            return Ok(None);
        }
        if let Some(max) = self.max_outputs {
            if self.emitted >= max {
                self.stop(StopReason::LimitReached);
                return Ok(None);
            }
        }
        match self.drive() {
            Ok(Some(row)) => {
                self.emitted += 1;
                Ok(Some(row))
            }
            Ok(None) => Ok(None),
            Err(err) => {
                self.fail();
                Err(err)
            }
        }
    }

    /// State-machine driver: install emitters from groups and pull rows.
    fn drive(
        &mut self,
    ) -> Result<Option<(ByteRecord, Option<ByteRecord>)>, JoinError> {
        loop {
            if let Some(emitter) = self.emitter.as_mut() {
                match emitter.next() {
                    Ok(Some(row)) => return Ok(Some(row)),
                    Ok(None) => {
                        self.emitter = None;
                        self.phase = Phase::Advance;
                    }
                    Err(err) => return Err(err),
                }
            }
            if !self.advance()? {
                self.phase = Phase::Done(StopReason::Completed);
                return Ok(None);
            }
            self.phase = Phase::Emitting;
        }
    }

    /// Compare the next group keys on each side and install the next
    /// emitter. Returns false when no more rows can be emitted.
    fn advance(&mut self) -> Result<bool, JoinError> {
        loop {
            self.left.fill_slot()?;
            self.right.fill_slot()?;
            match (self.left.slot.is_some(), self.right.slot.is_some()) {
                (false, false) => return Ok(false),
                (true, false) => {
                    if self.kind == JoinKind::Left {
                        let lg = self
                            .left
                            .take_group(&mut self.temps, self.memory_budget)?;
                        self.emitter = Some(Emitter::unmatched_left(lg)?);
                        return Ok(true);
                    }
                    return Ok(false);
                }
                (false, true) => return Ok(false),
                (true, true) => {
                    let ord = {
                        let lk = &self.left.slot.as_ref().unwrap().0;
                        let rk = &self.right.slot.as_ref().unwrap().0;
                        lk.cmp(rk)
                    };
                    match ord {
                        Ordering::Equal => {
                            let lg = self.left.take_group(
                                &mut self.temps,
                                self.memory_budget,
                            )?;
                            let rg = self.right.take_group(
                                &mut self.temps,
                                self.memory_budget,
                            )?;
                            self.emitter = Some(Emitter::matched(lg, rg)?);
                            return Ok(true);
                        }
                        Ordering::Less => {
                            let lg = self.left.take_group(
                                &mut self.temps,
                                self.memory_budget,
                            )?;
                            if self.kind == JoinKind::Left {
                                self.emitter =
                                    Some(Emitter::unmatched_left(lg)?);
                                return Ok(true);
                            }
                            drop(lg);
                        }
                        Ordering::Greater => {
                            self.right.skip_group()?;
                        }
                    }
                }
            }
        }
    }

    fn stop(&mut self, reason: StopReason) {
        self.emitter = None;
        self.phase = Phase::Done(reason);
        self.temps.cleanup_all();
    }

    fn fail(&mut self) {
        self.emitter = None;
        self.phase = Phase::Done(StopReason::Completed);
        self.failed = true;
        self.temps.cleanup_all();
    }

    /// Finish the join, reclaim all spill files and return its status.
    ///
    /// Safe to call after completion, an output-limit stop or an error;
    /// idempotent with respect to cleanup.
    pub fn finish(mut self) -> Result<JoinStatus, JoinError> {
        if self.failed {
            self.temps.cleanup_all();
            return Err(JoinError::Consumed);
        }
        let reason = match self.phase {
            Phase::Done(reason) => reason,
            _ => StopReason::Completed,
        };
        self.emitter = None;
        let cleanup = self.temps.cleanup_all();
        Ok(JoinStatus { rows: self.emitted, reason, cleanup })
    }

    /// Paths of spill files currently held by the join.
    pub fn spill_files(&self) -> Vec<PathBuf> {
        self.temps.paths()
    }
}

impl<L, R> Iterator for MergeJoin<L, R>
where
    L: Iterator<Item = Result<ByteRecord, crate::Error>>,
    R: Iterator<Item = Result<ByteRecord, crate::Error>>,
{
    type Item = Result<(ByteRecord, Option<ByteRecord>), JoinError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_row() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

impl<L, R> Drop for MergeJoin<L, R> {
    fn drop(&mut self) {
        self.emitter = None;
        self.temps.cleanup_all();
    }
}

/// Emits rows for one pair of groups, in stable left-then-right order.
///
/// The left group is streamed exactly once (in-memory prefix first, then
/// its spill file). For each left record, the right group is scanned in
/// full (in-memory prefix first, then its spill file), reopening the spill
/// for every left record when the right side overflowed memory.
struct Emitter {
    left: Group,
    right: Option<Group>,
    /// Open reader over the spilled tail of the left group.
    left_file: Option<FrameReader<File>>,
    /// Index of the next buffered left record.
    li: usize,
    /// The left record currently being paired against right records.
    cur_left: Option<ByteRecord>,
    /// Open reader over the spilled tail of the right group, reset for
    /// each left record.
    right_file: Option<FrameReader<File>>,
    /// Index of the next buffered right record, reset per left record.
    ri: usize,
    /// Whether the spilled right tail for the current left record was
    /// fully consumed.
    right_file_done: bool,
}

impl Emitter {
    fn matched(
        mut left: Group,
        mut right: Group,
    ) -> Result<Emitter, JoinError> {
        left.finish_spill()?;
        right.finish_spill()?;
        Ok(Emitter {
            left,
            right: Some(right),
            left_file: None,
            li: 0,
            cur_left: None,
            right_file: None,
            ri: 0,
            right_file_done: false,
        })
    }

    fn unmatched_left(mut left: Group) -> Result<Emitter, JoinError> {
        left.finish_spill()?;
        Ok(Emitter {
            left,
            right: None,
            left_file: None,
            li: 0,
            cur_left: None,
            right_file: None,
            ri: 0,
            right_file_done: false,
        })
    }

    fn next(
        &mut self,
    ) -> Result<Option<(ByteRecord, Option<ByteRecord>)>, JoinError> {
        loop {
            // Fetch the next left record on entry and after each right
            // group scan completes.
            if self.cur_left.is_none() {
                self.cur_left = self.next_left()?;
                self.ri = 0;
                self.right_file = None;
                self.right_file_done = false;
            }
            let left = match self.cur_left {
                Some(_) => self.cur_left.as_ref().unwrap().clone(),
                None => return Ok(None),
            };
            match self.right {
                None => {
                    self.cur_left = None;
                    return Ok(Some((left, None)));
                }
                Some(_) => match self.next_right()? {
                    Some(right) => return Ok(Some((left, Some(right)))),
                    None => {
                        // Whole right group paired with this left record;
                        // restart the scan for the next left record.
                        self.cur_left = None;
                    }
                },
            }
        }
    }

    fn next_left(&mut self) -> Result<Option<ByteRecord>, JoinError> {
        if let Some(rec) = self.left.records.get(self.li) {
            self.li += 1;
            return Ok(Some(rec.clone()));
        }
        if let Some(spill) = self.left.spill.as_ref() {
            if self.left_file.is_none() {
                self.left_file = Some(FrameReader::open(spill.path())?);
            }
            self.left_file.as_mut().unwrap().read_record().map_err(From::from)
        } else {
            Ok(None)
        }
    }

    fn next_right(&mut self) -> Result<Option<ByteRecord>, JoinError> {
        let rg = self.right.as_ref().unwrap();
        if self.ri < rg.records.len() {
            let rec = rg.records[self.ri].clone();
            self.ri += 1;
            return Ok(Some(rec));
        }
        if self.right_file_done || rg.spill.is_none() {
            return Ok(None);
        }
        if self.right_file.is_none() {
            self.right_file =
                Some(FrameReader::open(rg.spill.as_ref().unwrap().path())?);
        }
        let rec = self.right_file.as_mut().unwrap().read_record()?;
        if rec.is_none() {
            self.right_file_done = true;
        }
        Ok(rec)
    }
}
/// Records of one input side that share a key.
///
/// Records accumulate in `records` until `budget` resident bytes are
/// reached; later records append to `spill`. The in-memory prefix is always
/// emitted before spilled records.
struct Group {
    records: Vec<ByteRecord>,
    resident: usize,
    spill: Option<Spill>,
}

impl Group {
    fn new() -> Group {
        Group { records: Vec::new(), resident: 0, spill: None }
    }

    fn push(
        &mut self,
        rec: ByteRecord,
        budget: usize,
        temps: &mut TempMgr,
    ) -> Result<(), JoinError> {
        let size =
            rec.as_slice().len() + rec.len() * core::mem::size_of::<usize>();
        if self.spill.is_none()
            && !self.records.is_empty()
            && self.resident + size > budget
        {
            self.spill = Some(temps.create_spill()?);
        }
        match self.spill {
            Some(ref mut spill) => spill.write_record(&rec),
            None => {
                self.resident += size;
                self.records.push(rec);
                Ok(())
            }
        }
    }

    /// Flush and close the write side, if any, so the file can be reopened
    /// for reading.
    fn finish_spill(&mut self) -> Result<(), JoinError> {
        if let Some(spill) = self.spill.as_mut() {
            spill.finish_writing()?;
        }
        Ok(())
    }
}

/// A spill file that is either open for writing or finished (the writer is
/// dropped, flushing buffers, while the path stays registered for cleanup
/// and later reads).
struct Spill {
    path: PathBuf,
    wtr: Option<FrameWriter<File>>,
}

impl Spill {
    fn create(path: PathBuf) -> Result<Spill, JoinError> {
        let file =
            OpenOptions::new().write(true).create_new(true).open(&path)?;
        Ok(Spill { path, wtr: Some(FrameWriter::new(file)) })
    }

    fn write_record(&mut self, rec: &ByteRecord) -> Result<(), JoinError> {
        self.wtr.as_mut().unwrap().write_record(rec)?;
        Ok(())
    }

    /// Flush and close the write side so the file can be reopened for
    /// reading.
    fn finish_writing(&mut self) -> Result<(), JoinError> {
        if let Some(mut wtr) = self.wtr.take() {
            wtr.flush()?;
        }
        Ok(())
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Spill {
    fn drop(&mut self) {
        if let Some(mut wtr) = self.wtr.take() {
            let _ = wtr.flush();
        }
    }
}

/// Owns the directory, naming and reclamation of every spill file.
///
/// Every created spill is registered with a unique id. The `Spill` value
/// holds the open writer; after it is dropped (the file is closed), the
/// manager removes the path when asked, on every exit path.
struct TempMgr {
    dir: PathBuf,
    next_id: u32,
    entries: Vec<TempEntry>,
    report: CleanupReport,
    observer: Option<Box<dyn FnMut(&Path)>>,
}

/// One registered spill file.
struct TempEntry {
    path: PathBuf,
}

impl TempMgr {
    fn ensure_dir(&self) -> Result<(), JoinError> {
        if !self.dir.exists() {
            fs::create_dir_all(&self.dir)?;
        }
        Ok(())
    }

    fn create_spill(&mut self) -> Result<Spill, JoinError> {
        self.ensure_dir()?;
        loop {
            let id = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);
            let path = self.dir.join(format!(
                "csv-merge-join-{}-{}.spill",
                std::process::id(),
                id
            ));
            match Spill::create(path) {
                Ok(spill) => {
                    self.entries.push(TempEntry { path: spill.path.clone() });
                    return Ok(spill);
                }
                Err(JoinError::Io(ref err))
                    if err.kind() == io::ErrorKind::AlreadyExists =>
                {
                    continue
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn paths(&self) -> Vec<PathBuf> {
        self.entries.iter().map(|e| e.path.clone()).collect()
    }

    /// Remove every spill file that has not already been removed and
    /// accumulate the result into the report. The observer fires once for
    /// every path handled.
    fn cleanup_all(&mut self) -> CleanupReport {
        let entries = std::mem::take(&mut self.entries);
        for entry in entries {
            self.notify(&entry.path);
            match fs::remove_file(&entry.path) {
                Ok(()) => self.report.removed += 1,
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    self.report.missing += 1
                }
                Err(err) => self
                    .report
                    .failures
                    .push((entry.path.clone(), err.to_string())),
            }
        }
        self.report.clone()
    }

    fn notify(&mut self, path: &Path) {
        if let Some(cb) = self.observer.as_mut() {
            cb(path);
        }
    }
}

/// One input stream with one-record lookahead and ordering validation.
struct Source<I> {
    iter: I,
    side: JoinSide,
    key_col: usize,
    /// Next record with a copy of its key bytes.
    slot: Option<(Vec<u8>, ByteRecord)>,
    prev_key: Vec<u8>,
    started: bool,
    /// Zero-based index of records consumed from this side.
    index: u64,
}

impl<I> Source<I> {
    fn new(iter: I, side: JoinSide, key_col: usize) -> Source<I> {
        Source {
            iter,
            side,
            key_col,
            slot: None,
            prev_key: Vec::new(),
            started: false,
            index: 0,
        }
    }
}

impl<I, E> Source<I>
where
    I: Iterator<Item = Result<ByteRecord, E>>,
    E: Into<crate::Error>,
{
    /// Pull one record, validating the key column and ascending order.
    fn pull(&mut self) -> Result<Option<ByteRecord>, JoinError> {
        match self.iter.next() {
            None => Ok(None),
            Some(Err(err)) => Err(JoinError::Input(Box::new(err.into()))),
            Some(Ok(rec)) => {
                let position = rec.position().cloned();
                let key = match rec.get(self.key_col) {
                    Some(k) => k.to_vec(),
                    None => {
                        return Err(JoinError::MissingKey {
                            side: self.side,
                            record_index: self.index,
                            position,
                        })
                    }
                };
                if self.started && key.as_slice() < self.prev_key.as_slice() {
                    return Err(JoinError::OutOfOrder {
                        side: self.side,
                        record_index: self.index,
                        position,
                    });
                }
                self.started = true;
                self.prev_key.clear();
                self.prev_key.extend_from_slice(&key);
                self.index += 1;
                Ok(Some(rec))
            }
        }
    }

    fn fill_slot(&mut self) -> Result<(), JoinError> {
        if self.slot.is_none() {
            if let Some(rec) = self.pull()? {
                let key = rec.get(self.key_col).unwrap().to_vec();
                self.slot = Some((key, rec));
            }
        }
        Ok(())
    }

    /// Consume the whole equal-key group seeded by the slot record.
    fn take_group(
        &mut self,
        temps: &mut TempMgr,
        budget: usize,
    ) -> Result<Group, JoinError> {
        let key = self.slot.as_ref().unwrap().0.clone();
        let seed = self.slot.take().unwrap().1;
        let mut group = Group::new();
        group.push(seed, budget, temps)?;
        loop {
            match self.pull()? {
                None => break,
                Some(rec) => {
                    let k = rec.get(self.key_col).unwrap();
                    if k == key.as_slice() {
                        group.push(rec, budget, temps)?;
                    } else {
                        self.slot = Some((k.to_vec(), rec));
                        break;
                    }
                }
            }
        }
        Ok(group)
    }

    /// Consume and discard the group seeded by the slot record, still
    /// validating order. Avoids spilling data that will never be emitted.
    fn skip_group(&mut self) -> Result<(), JoinError> {
        let key = self.slot.take().unwrap().0;
        loop {
            match self.pull()? {
                None => break,
                Some(rec) => {
                    let k = rec.get(self.key_col).unwrap();
                    if k != key.as_slice() {
                        self.slot = Some((k.to_vec(), rec));
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

/// A tiny length-prefixed binary writer for spill records.
///
/// Framing keeps invalid UTF-8 bytes intact and avoids ambiguity from
/// re-parsing CSV (empty records, quotes, newlines). Per record:
/// `u32` field count, then for each field `u64` length plus raw bytes,
/// then `u8` position-present and, when set, three `u64`s (byte, line,
/// record).
struct FrameWriter<W> {
    inner: W,
}

impl<W: Write> FrameWriter<W> {
    fn new(inner: W) -> FrameWriter<W> {
        FrameWriter { inner }
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.inner.write_all(bytes)
    }

    fn write_record(&mut self, rec: &ByteRecord) -> io::Result<()> {
        self.write_all(&(rec.len() as u32).to_le_bytes())?;
        for field in rec.iter() {
            self.write_all(&(field.len() as u64).to_le_bytes())?;
            self.write_all(field)?;
        }
        match rec.position() {
            None => self.write_all(&[0]),
            Some(pos) => {
                self.write_all(&[1])?;
                self.write_all(&pos.byte().to_le_bytes())?;
                self.write_all(&pos.line().to_le_bytes())?;
                self.write_all(&pos.record().to_le_bytes())
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Reader for frames written by [`FrameWriter`].
struct FrameReader<R> {
    inner: R,
}

impl<R: Read> FrameReader<R> {
    fn new(inner: R) -> FrameReader<R> {
        FrameReader { inner }
    }

    fn read_full(&mut self, buf: &mut [u8]) -> io::Result<bool> {
        let mut filled = 0;
        while filled < buf.len() {
            match self.inner.read(&mut buf[filled..]) {
                Ok(0) => {
                    if filled == 0 {
                        return Ok(false);
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated spill file",
                    ));
                }
                Ok(n) => filled += n,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(err) => return Err(err),
            }
        }
        Ok(true)
    }

    fn read_u32(&mut self) -> io::Result<Option<u32>> {
        let mut buf = [0u8; 4];
        if !self.read_full(&mut buf)? {
            return Ok(None);
        }
        Ok(Some(u32::from_le_bytes(buf)))
    }

    fn read_u64(&mut self) -> io::Result<u64> {
        let mut buf = [0u8; 8];
        if !self.read_full(&mut buf)? {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated spill file",
            ));
        }
        Ok(u64::from_le_bytes(buf))
    }

    fn read_record(&mut self) -> io::Result<Option<ByteRecord>> {
        let nfields = match self.read_u32()? {
            None => return Ok(None),
            Some(n) => n as usize,
        };
        let mut rec = ByteRecord::with_capacity(0, nfields);
        for _ in 0..nfields {
            let len = self.read_u64()? as usize;
            let mut field = vec![0u8; len];
            if !self.read_full(&mut field)? {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated spill file",
                ));
            }
            rec.push_field(&field);
        }
        let mut present = [0u8; 1];
        if !self.read_full(&mut present)? {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated spill file",
            ));
        }
        if present[0] == 1 {
            let byte = self.read_u64()?;
            let line = self.read_u64()?;
            let record = self.read_u64()?;
            let mut pos = Position::new();
            pos.set_byte(byte).set_line(line).set_record(record);
            rec.set_position(Some(pos));
        }
        Ok(Some(rec))
    }
}

impl FrameReader<File> {
    fn open(path: &Path) -> Result<FrameReader<File>, JoinError> {
        let file = File::open(path)?;
        Ok(FrameReader::new(file))
    }
}
