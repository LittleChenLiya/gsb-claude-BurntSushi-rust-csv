/*!
External (on-disk) stable sorting of CSV byte records.

This module provides [`ExternalSorter`], a reusable building block that sorts
CSV data that may be larger than the memory budget available to hold it. The
caller supplies:

* the column indices that make up the sort key (compared in order),
* a byte-wise comparator for the key fields,
* a memory budget (in bytes) for buffering records in memory,
* a cap on how many sorted run files may be open at once during merging.

Records are handled as [`ByteRecord`]s end to end, so non-UTF-8 data is
preserved verbatim. The caller's [`ReaderBuilder`] and [`WriterBuilder`]
configurations (delimiter, quote, escape, terminator, headers, ...) are fully
honored: input is parsed with the caller's reader configuration and the final
output is written with the caller's writer configuration. Quoted newlines
inside fields are *not* treated as record boundaries (that is handled by the
regular CSV parser), and the header row, when present, is written exactly once
to the output and never participates in sorting.

# Algorithm

1. **Spill phase.** Records are read one at a time and buffered in memory.
   When the estimated memory in use would exceed the budget, the buffer is
   sorted with a stable in-memory sort and flushed to a temporary *run* file.
   A single record whose estimated size exceeds the budget is rejected with
   [`SortError::RecordTooLarge`]; the budget is never silently relaxed.
2. **Merge phase.** Sorted runs are merged k-way with at most
   `max_open_runs` runs open at a time. If more runs exist, they are merged
   in rounds (groups of `max_open_runs` runs are merged into intermediate
   runs) until a single final pass writes the sorted output. Ties between
   equal keys are broken by original record order, so the sort is stable
   end to end.
3. **Atomic publish.** When sorting into a file, the output is first written
   to a temporary file in the same directory as the target and only renamed
   over the target once everything succeeded. If parsing, I/O or the
   comparator fails at any step, all temporary files created by this call are
   removed and the original target file is left untouched.

# Memory budget accounting

The in-memory estimate for one buffered record is:

```text
estimate(record) = size_of::<ByteRecord>()           // handle in the Vec
                 + RECORD_STRUCT_OVERHEAD            // boxed inner struct
                 + record.as_slice().len()           // field data bytes
                 + record.len() * size_of::<usize>() // field bounds index
```

The running total of these estimates is compared against the budget. This
mirrors how `ByteRecord` actually stores data (one contiguous buffer of field
bytes plus one `usize` per field for the bounds index), so the estimate is a
faithful, explainable approximation of the memory held.

# Failure injection / two-phase use

For tests or pipelines that need to observe the intermediate state, the
phases are available separately: [`ExternalSorter::spill`] reads the input
and produces a [`SpilledInput`] (run files on disk or records in memory), and
[`ExternalSorter::merge_to_file`] / [`ExternalSorter::merge`] turn a
[`SpilledInput`] into sorted output. Deleting or corrupting a run file
between the two phases surfaces as a clean error with full temporary file
cleanup.
*/

use std::cmp::Ordering;
use std::error::Error as StdError;
use std::fmt;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use crate::{ByteRecord, Reader, ReaderBuilder, Writer, WriterBuilder};

/// The result type used by fallible comparators.
pub type CompareResult =
    Result<Ordering, Box<dyn StdError + Send + Sync + 'static>>;

type BoxCompare = Box<dyn FnMut(&[u8], &[u8]) -> CompareResult + Send>;

/// Default memory budget: 16 MiB of estimated record data.
const DEFAULT_MEMORY_BUDGET: usize = 16 * (1 << 20);

/// Default maximum number of run files merged at once.
const DEFAULT_MAX_OPEN_RUNS: usize = 8;

/// Fixed overhead approximating the boxed inner struct of a `ByteRecord`
/// (position, `Vec<u8>` of field data, `Vec<usize>` of bounds).
const RECORD_STRUCT_OVERHEAD: usize = 128;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// An error that can occur while externally sorting CSV data.
#[derive(Debug)]
pub enum SortError {
    /// A CSV parse or write error from the underlying reader/writer.
    Csv(crate::Error),
    /// A raw I/O error (temp files, publish step, ...).
    Io(io::Error),
    /// The user supplied comparator reported a failure.
    Compare(Box<dyn StdError + Send + Sync + 'static>),
    /// A single record's estimated size exceeds the memory budget. The
    /// budget is never silently relaxed; the caller must raise it.
    RecordTooLarge {
        /// The estimated size of the offending record in bytes.
        estimate: usize,
        /// The configured memory budget in bytes.
        budget: usize,
        /// The 0-based index of the record within the input (header excluded).
        record_index: u64,
    },
    /// A sort key column index is out of bounds for a record (or header).
    ColumnOutOfBounds {
        /// The offending column index.
        column: usize,
        /// The number of fields the record actually has.
        record_len: usize,
        /// The 0-based index of the record within the input (header excluded).
        record_index: u64,
    },
}

impl fmt::Display for SortError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SortError::Csv(err) => write!(f, "csv error: {}", err),
            SortError::Io(err) => write!(f, "i/o error: {}", err),
            SortError::Compare(err) => write!(f, "comparator error: {}", err),
            SortError::RecordTooLarge { estimate, budget, record_index } => {
                write!(
                    f,
                    "record #{} needs an estimated {} bytes, which exceeds \
                     the memory budget of {} bytes; raise the budget to \
                     sort this input",
                    record_index, estimate, budget
                )
            }
            SortError::ColumnOutOfBounds {
                column,
                record_len,
                record_index,
            } => {
                write!(
                    f,
                    "sort key column {} is out of bounds for record #{} \
                     with {} field(s)",
                    column, record_index, record_len
                )
            }
        }
    }
}

impl StdError for SortError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            SortError::Csv(err) => Some(err),
            SortError::Io(err) => Some(err),
            SortError::Compare(err) => Some(&**err),
            _ => None,
        }
    }
}

impl From<crate::Error> for SortError {
    fn from(err: crate::Error) -> SortError {
        SortError::Csv(err)
    }
}

impl From<io::Error> for SortError {
    fn from(err: io::Error) -> SortError {
        SortError::Io(err)
    }
}

/// Statistics describing a completed (or attempted) external sort.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SortStats {
    /// Number of data records read from the input (header excluded).
    pub records_read: u64,
    /// Number of data records written to the output (header excluded).
    pub records_written: u64,
    /// Number of sorted run files spilled to disk. `0` means the whole
    /// input fit in the memory budget and no disk runs were needed.
    pub runs_created: usize,
    /// Number of merge passes performed. Each reduction round over the run
    /// files counts as one pass, as does the final merge into the output.
    pub merge_rounds: usize,
    /// Total bytes written to run files during the spill phase.
    pub bytes_spilled: u64,
    /// Peak estimated bytes held in the in-memory buffer at once.
    pub peak_buffer_estimate: usize,
}

/// Estimates the memory held by one buffered [`ByteRecord`], in bytes.
///
/// See the module level documentation for the exact formula.
pub fn estimate_record_size(rec: &ByteRecord) -> usize {
    std::mem::size_of::<ByteRecord>()
        + RECORD_STRUCT_OVERHEAD
        + rec.as_slice().len()
        + rec.len() * std::mem::size_of::<usize>()
}

/// A reusable external sorter for CSV byte records.
///
/// Construct one with [`ExternalSorter::new`] (infallible comparator) or
/// [`ExternalSorter::new_fallible`], tune it with the builder methods and
/// then call one of the `sort*`/`spill`/`merge` methods. The same sorter can
/// be reused for any number of sort operations.
pub struct ExternalSorter {
    /// Column indices of the sort key, in decreasing priority.
    columns: Vec<usize>,
    /// Memory budget for the in-memory buffer, in estimated bytes.
    memory_budget: usize,
    /// Maximum number of run files open at once during a merge.
    max_open_runs: usize,
    /// The byte-wise comparator for key fields. Interior mutability lets
    /// all sort operations take `&self`.
    cmp: std::cell::RefCell<BoxCompare>,
}

impl ExternalSorter {
    /// Creates a sorter ordering records by `columns` using the infallible
    /// byte-wise comparator `cmp`.
    ///
    /// Columns are compared in the order given; the first column is the
    /// primary key. Records that compare equal on all key columns keep their
    /// original relative order (the sort is stable).
    pub fn new<F>(columns: Vec<usize>, cmp: F) -> ExternalSorter
    where
        F: FnMut(&[u8], &[u8]) -> Ordering + Send + 'static,
    {
        let mut cmp = cmp;
        ExternalSorter::new_fallible(columns, move |a, b| Ok(cmp(a, b)))
    }

    /// Creates a sorter with a fallible comparator. If the comparator fails
    /// at any point, the sort aborts, all temporary files are cleaned up and
    /// the error is returned as [`SortError::Compare`].
    pub fn new_fallible<F>(columns: Vec<usize>, cmp: F) -> ExternalSorter
    where
        F: FnMut(&[u8], &[u8]) -> CompareResult + Send + 'static,
    {
        ExternalSorter {
            columns,
            memory_budget: DEFAULT_MEMORY_BUDGET,
            max_open_runs: DEFAULT_MAX_OPEN_RUNS,
            cmp: std::cell::RefCell::new(Box::new(cmp)),
        }
    }

    /// Sets the memory budget (in estimated bytes) for the in-memory record
    /// buffer. Defaults to 16 MiB. A single record larger than the budget is
    /// rejected with [`SortError::RecordTooLarge`].
    pub fn memory_budget(mut self, bytes: usize) -> ExternalSorter {
        self.memory_budget = bytes;
        self
    }

    /// Sets the maximum number of run files that may be open at once during
    /// merging. If more runs exist, merging proceeds in multiple rounds.
    /// The minimum effective value is 2. Defaults to 8.
    pub fn max_open_runs(mut self, n: usize) -> ExternalSorter {
        self.max_open_runs = n.max(2);
        self
    }

    /// Sorts CSV data read from `rdr` and writes it with a writer built from
    /// `wtr_builder` around `wtr`. Temporary run files (if any) are created
    /// in `run_dir` and always removed before this returns.
    ///
    /// If the reader was built with `has_headers(true)`, the header row is
    /// written to the output exactly once and does not participate in
    /// sorting.
    pub fn sort<R: io::Read, W: io::Write>(
        &self,
        rdr: &mut Reader<R>,
        wtr_builder: &WriterBuilder,
        wtr: W,
        run_dir: &Path,
    ) -> Result<SortStats, SortError> {
        let spilled = self.spill(rdr, run_dir)?;
        self.merge(&spilled, wtr_builder, wtr, run_dir)
    }

    /// Sorts the CSV file at `input` into the file at `output`, atomically.
    ///
    /// The input is parsed with `rdr_builder` and the output written with
    /// `wtr_builder`, so all delimiter/quote/escape/terminator/header
    /// settings apply as usual. The sorted output is first written to a
    /// temporary file in the same directory as `output` and renamed over it
    /// only after everything succeeded. On any failure the temporary files
    /// are removed and a pre-existing `output` file is left untouched.
    pub fn sort_file<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        rdr_builder: &ReaderBuilder,
        input: P,
        wtr_builder: &WriterBuilder,
        output: Q,
    ) -> Result<SortStats, SortError> {
        let out_path = output.as_ref();
        let dir = output_dir(out_path);
        let file = File::open(input.as_ref())?;
        let mut rdr = rdr_builder.from_reader(file);
        let spilled = self.spill(&mut rdr, &dir)?;
        self.merge_to_file(&spilled, wtr_builder, out_path, &dir)
    }

    /// Phase 1: reads all records from `rdr` and returns them either as an
    /// in-memory sorted buffer (when they fit the budget) or as a set of
    /// sorted run files in `run_dir`.
    ///
    /// The returned [`SpilledInput`] owns the run files: they are deleted
    /// when it is dropped or consumed by a merge, whichever comes first.
    pub fn spill<R: io::Read>(
        &self,
        rdr: &mut Reader<R>,
        run_dir: &Path,
    ) -> Result<SpilledInput, SortError> {
        fs::create_dir_all(run_dir)?;
        let headers = if rdr.has_headers() {
            let h = rdr.byte_headers()?.clone();
            // An empty header record means empty input; there is nothing to
            // validate against (and nothing will be written back).
            if !h.is_empty() {
                for &col in &self.columns {
                    if col >= h.len() {
                        return Err(SortError::ColumnOutOfBounds {
                            column: col,
                            record_len: h.len(),
                            record_index: 0,
                        });
                    }
                }
            }
            Some(h)
        } else {
            None
        };
        let mut runs = RunRegistry::new();
        let mut buf: Vec<ByteRecord> = vec![];
        let mut used = 0usize;
        let mut peak = 0usize;
        let mut bytes_spilled = 0u64;
        let mut count = 0u64;
        let mut rec = ByteRecord::new();
        while rdr.read_byte_record(&mut rec)? {
            let estimate = estimate_record_size(&rec);
            if estimate > self.memory_budget {
                return Err(SortError::RecordTooLarge {
                    estimate,
                    budget: self.memory_budget,
                    record_index: count,
                });
            }
            for &col in &self.columns {
                if col >= rec.len() {
                    return Err(SortError::ColumnOutOfBounds {
                        column: col,
                        record_len: rec.len(),
                        record_index: count,
                    });
                }
            }
            if used + estimate > self.memory_budget && !buf.is_empty() {
                bytes_spilled +=
                    self.write_run(&mut buf, &mut runs, run_dir)?;
                used = 0;
            }
            used += estimate;
            peak = peak.max(used);
            buf.push(rec.clone());
            count += 1;
        }
        if runs.is_empty() {
            // Everything fit in memory: sort once, no disk runs at all.
            self.sort_buffer(&mut buf)?;
            Ok(SpilledInput {
                headers,
                records: count,
                runs: vec![],
                memory: Some(buf),
                bytes_spilled: 0,
                peak_buffer_estimate: peak,
            })
        } else {
            if !buf.is_empty() {
                bytes_spilled +=
                    self.write_run(&mut buf, &mut runs, run_dir)?;
            }
            Ok(SpilledInput {
                headers,
                records: count,
                runs: runs.disarm(),
                memory: None,
                bytes_spilled,
                peak_buffer_estimate: peak,
            })
        }
    }

    /// Phase 2: merges a [`SpilledInput`] into sorted output written with a
    /// writer built from `wtr_builder` around `wtr`.
    ///
    /// This consumes the run files referenced by `spilled`: they are removed
    /// whether the merge succeeds or fails. Intermediate runs created by
    /// multi-round merging are written to `run_dir` and likewise removed.
    pub fn merge<W: io::Write>(
        &self,
        spilled: &SpilledInput,
        wtr_builder: &WriterBuilder,
        wtr: W,
        run_dir: &Path,
    ) -> Result<SortStats, SortError> {
        let mut stats = SortStats {
            records_read: spilled.records,
            runs_created: spilled.runs.len(),
            bytes_spilled: spilled.bytes_spilled,
            peak_buffer_estimate: spilled.peak_buffer_estimate,
            ..SortStats::default()
        };
        let result = self.merge_inner(spilled, wtr_builder, wtr, run_dir);
        // The run files are consumed by the merge in all cases.
        for path in &spilled.runs {
            let _ = fs::remove_file(path);
        }
        let (mut wtr, rounds, written) = result?;
        wtr.flush()?;
        stats.merge_rounds = rounds;
        stats.records_written = written;
        Ok(stats)
    }

    /// Phase 2 variant publishing to a file: merges into a temporary file in
    /// the same directory as `output` and renames it over `output` only on
    /// full success. On failure the temporary file and all run files are
    /// removed and a pre-existing `output` is preserved.
    pub fn merge_to_file(
        &self,
        spilled: &SpilledInput,
        wtr_builder: &WriterBuilder,
        output: &Path,
        run_dir: &Path,
    ) -> Result<SortStats, SortError> {
        let dir = output_dir(output);
        let tmp = temp_path(&dir, "out");
        let result = (|| {
            let file = File::create(&tmp)?;
            let stats = self.merge(spilled, wtr_builder, file, run_dir)?;
            publish(&tmp, output)?;
            Ok(stats)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&tmp);
        }
        result
    }

    /// The actual merge. Returns the writer (for flushing), the number of
    /// merge passes and the number of data records written.
    fn merge_inner<W: io::Write>(
        &self,
        spilled: &SpilledInput,
        wtr_builder: &WriterBuilder,
        wtr: W,
        run_dir: &Path,
    ) -> Result<(Writer<W>, usize, u64), SortError> {
        let mut wtr = wtr_builder.from_writer(wtr);
        let mut written = 0u64;
        if let Some(headers) = &spilled.headers {
            // The header is written exactly once and never sorted. An empty
            // header only occurs for empty input; writing it would fabricate
            // a record, so it is skipped.
            if !headers.is_empty() {
                wtr.write_byte_record(headers)?;
            }
        }
        let mut rounds = 0usize;
        if let Some(records) = &spilled.memory {
            for rec in records {
                wtr.write_byte_record(rec)?;
                written += 1;
            }
        } else {
            let mut runs = spilled.runs.clone();
            let mut intermediate = RunRegistry::new();
            // Reduce in rounds until at most `max_open_runs` runs remain.
            while runs.len() > self.max_open_runs {
                let mut next =
                    Vec::with_capacity(runs.len() / self.max_open_runs + 1);
                for chunk in runs.chunks(self.max_open_runs) {
                    if chunk.len() == 1 {
                        next.push(chunk[0].clone());
                        continue;
                    }
                    let path = intermediate.new_path(run_dir);
                    {
                        let file = File::create(&path)?;
                        let mut run_wtr =
                            run_writer_builder().from_writer(file);
                        self.merge_kway(chunk, &mut run_wtr)?;
                        run_wtr.flush()?;
                    }
                    for old in chunk {
                        let _ = fs::remove_file(old);
                    }
                    next.push(path);
                }
                rounds += 1;
                runs = next;
            }
            // Final merge pass straight into the output.
            if !runs.is_empty() {
                written += self.merge_kway(&runs, &mut wtr)?;
                rounds += 1;
            }
        }
        Ok((wtr, rounds, written))
    }

    /// Merges the sorted run files at `paths` into `wtr`, preserving the
    /// original record order for equal keys (earlier runs win ties).
    fn merge_kway<W: io::Write>(
        &self,
        paths: &[PathBuf],
        wtr: &mut Writer<W>,
    ) -> Result<u64, SortError> {
        let mut readers = Vec::with_capacity(paths.len());
        let mut heads: Vec<Option<ByteRecord>> =
            Vec::with_capacity(paths.len());
        for path in paths {
            let file = File::open(path)?;
            let mut rdr = run_reader_builder().from_reader(file);
            let mut rec = ByteRecord::new();
            let has = rdr.read_byte_record(&mut rec)?;
            readers.push(rdr);
            heads.push(if has { Some(rec) } else { None });
        }
        let mut written = 0u64;
        loop {
            // Linear scan for the smallest head. `max_open_runs` bounds the
            // number of heads, and scanning keeps the fallible comparator
            // and the stability rule (lowest run index wins ties) trivially
            // correct.
            let mut min: Option<usize> = None;
            for (i, head) in heads.iter().enumerate() {
                if head.is_none() {
                    continue;
                }
                match min {
                    None => min = Some(i),
                    Some(m) => {
                        let ord = self.compare_records(
                            heads[i].as_ref().unwrap(),
                            heads[m].as_ref().unwrap(),
                        )?;
                        if ord == Ordering::Less {
                            min = Some(i);
                        }
                    }
                }
            }
            let m = match min {
                None => break,
                Some(m) => m,
            };
            let rec = heads[m].take().unwrap();
            wtr.write_byte_record(&rec)?;
            written += 1;
            let mut next = ByteRecord::new();
            if readers[m].read_byte_record(&mut next)? {
                heads[m] = Some(next);
            }
        }
        Ok(written)
    }

    /// Sorts `buf` in place with a stable sort and the fallible comparator.
    fn sort_buffer(&self, buf: &mut [ByteRecord]) -> Result<(), SortError> {
        let mut err: Option<SortError> = None;
        // `sort_by` is a stable sort, so equal keys keep their original
        // relative order. A comparator failure is smuggled out via `err`.
        buf.sort_by(|a, b| {
            if err.is_some() {
                return Ordering::Equal;
            }
            match self.compare_records(a, b) {
                Ok(ord) => ord,
                Err(e) => {
                    err = Some(e);
                    Ordering::Equal
                }
            }
        });
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Compares two records column by column with the user comparator.
    fn compare_records(
        &self,
        a: &ByteRecord,
        b: &ByteRecord,
    ) -> Result<Ordering, SortError> {
        // Column bounds were validated when the records were read, so a
        // missing field here can only mean a short record in a flexible
        // merge input; treat it as empty.
        for &col in &self.columns {
            let fa = a.get(col).unwrap_or(b"");
            let fb = b.get(col).unwrap_or(b"");
            let ord = {
                let mut cmp = self.cmp.borrow_mut();
                cmp(fa, fb).map_err(SortError::Compare)?
            };
            match ord {
                Ordering::Equal => continue,
                ord => return Ok(ord),
            }
        }
        Ok(Ordering::Equal)
    }

    /// Sorts `buf` and writes it as a new run file in `run_dir`. Returns the
    /// number of bytes written. The buffer is cleared for reuse.
    fn write_run(
        &self,
        buf: &mut Vec<ByteRecord>,
        runs: &mut RunRegistry,
        run_dir: &Path,
    ) -> Result<u64, SortError> {
        self.sort_buffer(buf)?;
        let path = runs.new_path(run_dir);
        let file = File::create(&path)?;
        let mut wtr = run_writer_builder().from_writer(file);
        for rec in buf.drain(..) {
            wtr.write_byte_record(&rec)?;
        }
        wtr.flush()?;
        let bytes = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        Ok(bytes)
    }
}

/// The result of the spill phase: either an in-memory sorted buffer or a set
/// of sorted run files on disk, plus the header row if present.
///
/// Run files are temporary: they are removed when this value is dropped or
/// when it is consumed by a merge, whichever happens first.
#[derive(Debug)]
pub struct SpilledInput {
    headers: Option<ByteRecord>,
    records: u64,
    runs: Vec<PathBuf>,
    memory: Option<Vec<ByteRecord>>,
    bytes_spilled: u64,
    peak_buffer_estimate: usize,
}

impl SpilledInput {
    /// The header row read from the input, if the reader has headers.
    pub fn headers(&self) -> Option<&ByteRecord> {
        self.headers.as_ref()
    }

    /// The number of data records read (header excluded).
    pub fn record_count(&self) -> u64 {
        self.records
    }

    /// Paths of the sorted run files on disk. Empty when everything fit in
    /// the memory budget.
    pub fn run_files(&self) -> &[PathBuf] {
        &self.runs
    }

    /// Whether the whole input fit in the memory budget.
    pub fn is_in_memory(&self) -> bool {
        self.memory.is_some()
    }
}

impl Drop for SpilledInput {
    fn drop(&mut self) {
        for path in &self.runs {
            let _ = fs::remove_file(path);
        }
    }
}

/// Tracks temporary run files and deletes them on drop unless disarmed.
/// This is the cleanup guard backing the "no temp files left behind" rule.
#[derive(Debug)]
struct RunRegistry {
    paths: Vec<PathBuf>,
    armed: bool,
}

impl RunRegistry {
    fn new() -> RunRegistry {
        RunRegistry { paths: vec![], armed: true }
    }

    fn new_path(&mut self, dir: &Path) -> PathBuf {
        let path = temp_path(dir, "run");
        self.paths.push(path.clone());
        path
    }

    fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    /// Hands ownership of the tracked paths to the caller without deleting
    /// them.
    fn disarm(mut self) -> Vec<PathBuf> {
        self.armed = false;
        std::mem::take(&mut self.paths)
    }
}

impl Drop for RunRegistry {
    fn drop(&mut self) {
        if self.armed {
            for path in &self.paths {
                let _ = fs::remove_file(path);
            }
        }
    }
}

/// Generates a unique temporary file name inside `dir`.
fn temp_path(dir: &Path, kind: &str) -> PathBuf {
    let n = TEMP_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
    dir.join(format!(".csv-extsort-{}-{}-{}.tmp", std::process::id(), n, kind))
}

/// The directory temporary files and the final rename happen in.
fn output_dir(output: &Path) -> PathBuf {
    match output.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// Renames `tmp` over `out`, replacing an existing destination (on Windows
/// `rename` refuses to overwrite, so remove the destination first).
fn publish(tmp: &Path, out: &Path) -> io::Result<()> {
    match fs::rename(tmp, out) {
        Ok(()) => Ok(()),
        Err(err) => {
            if out.exists() {
                fs::remove_file(out)?;
                fs::rename(tmp, out)
            } else {
                Err(err)
            }
        }
    }
}

/// The canonical writer configuration for run files. Run files are internal
/// to the sort; a fixed, always-round-trippable format is used regardless of
/// the caller's input/output dialect. The default format quotes any field
/// containing the delimiter, quote or a line break, so arbitrary bytes
/// (including non-UTF-8 data and embedded newlines) survive the round trip.
fn run_writer_builder() -> WriterBuilder {
    let mut builder = WriterBuilder::new();
    builder.has_headers(false).flexible(true);
    builder
}

/// The reader configuration matching [`run_writer_builder`].
fn run_reader_builder() -> ReaderBuilder {
    let mut builder = ReaderBuilder::new();
    builder.has_headers(false).flexible(true);
    builder
}
