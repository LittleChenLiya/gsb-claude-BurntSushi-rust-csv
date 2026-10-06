/*!
External (on-disk) stable sorting of CSV data.

This module provides [`ExternalSorter`], a reusable building block that sorts
CSV records that may not fit into memory. The caller supplies:

* the **column indices** that make up the sort key,
* a **byte-wise comparator** for individual fields, and
* a **memory budget** plus a **fan-in limit** (`max_open_runs`).

Records are handled as [`ByteRecord`]s end to end, so data that is not valid
UTF-8 is preserved verbatim. The caller's [`ReaderBuilder`] and
[`WriterBuilder`] are honored for every file touched (input, intermediate
runs and output): custom `delimiter`, `quote`, `escape`, `terminator` and
header configuration all keep working, quoted newlines are *not* treated as
record boundaries, and the header (if any) is written exactly once and never
participates in sorting.

# Algorithm

1. **Run generation.** Records are read until the in-memory budget would be
   exceeded, sorted with a *stable* sort, and written to a temporary "run"
   file next to the output. This repeats until the input is exhausted.
2. **Multi-way merge.** Runs are merged with a k-way merge where
   `k <= max_open_runs`. If more runs exist than can be opened at once,
   merging proceeds in multiple rounds, producing intermediate runs until a
   final round can write the sorted output. Ties between runs are broken by
   run id (earlier runs first), which combined with the stable in-memory sort
   makes the whole sort **stable**: records with equal keys keep their
   original relative order.
3. **Atomic publish.** The sorted output is first written to a temporary
   file in the *same directory* as the target and only renamed over the
   target once everything (parsing, comparing, reading, writing) has
   succeeded. On any error, all temporary files created by this call are
   removed and a pre-existing target file is left untouched.

# Memory budget accounting

The budget only needs to cover what this module itself allocates, so the
estimate is deliberately simple and explainable. The in-memory cost of one
buffered record is estimated as:

```text
size_of::<ByteRecord>()               // the struct itself (its Vec slot)
+ record.as_slice().len()             // all field bytes (one buffer)
+ (record.len() + 1) * size_of::<usize>()  // the field-boundary index
```

A run is flushed to disk as soon as adding the next record would push the
sum of these estimates over the budget. If a *single* record's estimate
already exceeds the budget, [`SortError::RecordExceedsBudget`] is returned;
the budget is never silently relaxed.

Not counted (because they are bounded independently): the fixed-size I/O
buffers of the `Reader`/`Writer` (8 KiB each by default, at most
`max_open_runs + 1` of them open during a merge round) and the run file
paths.

Use [`record_memory_estimate`] to compute the estimate for a record, e.g. to
pick a budget that fits a known workload.

# Failure injection

[`ExternalSorter::on_event`] registers a callback that observes run creation
and merge rounds. The callback may return an error (or delete a run file it
is told about) to inject failures; the sorter will abort, clean up its
temporary files and preserve any pre-existing output. This is primarily
useful for tests.

# Example

```no_run
use csv::{ReaderBuilder, WriterBuilder};
use csv::external_sort::ExternalSorter;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let stats = ExternalSorter::new()
    .columns(&[0, 2])
    .memory_budget(1 << 20) // 1 MiB
    .max_open_runs(8)
    .compare_by(|a: &[u8], b: &[u8]| a.cmp(b))
    .sort_path(
        "input.csv",
        "sorted.csv",
        &ReaderBuilder::new(),
        &WriterBuilder::new(),
    )?;
println!("sorted {} records in {} runs / {} merge rounds",
         stats.records, stats.runs, stats.merge_rounds);
# Ok(())
# }
```
*/

use std::cmp::Ordering;
use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::io;
use std::mem::size_of;
use std::path::{Path, PathBuf};

use crate::{ByteRecord, Error, ReaderBuilder, WriterBuilder};

/// The default memory budget: 64 MiB.
const DEFAULT_BUDGET: usize = 64 * (1 << 20);
/// The default maximum number of run files merged at once.
const DEFAULT_MAX_OPEN_RUNS: usize = 16;

/// Estimates the in-memory cost of buffering one record while sorting.
///
/// This is the exact formula the sorter uses for budget accounting; see the
/// [module level documentation](self) for the rationale.
pub fn record_memory_estimate(rec: &ByteRecord) -> usize {
    size_of::<ByteRecord>()
        + rec.as_slice().len()
        + (rec.len() + 1) * size_of::<usize>()
}

/// A reusable external CSV sorter.
///
/// Created with [`ExternalSorter::new`], configured with the builder-style
/// methods and executed with [`ExternalSorter::sort_path`]. The same sorter
/// can be reused for multiple sorts.
pub struct ExternalSorter {
    columns: Vec<usize>,
    budget: usize,
    max_open_runs: usize,
    cmp: Option<Box<dyn Fn(&[u8], &[u8]) -> Ordering>>,
    hook: Option<Box<dyn FnMut(SortEvent) -> Result<(), SortError>>>,
}

impl fmt::Debug for ExternalSorter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalSorter")
            .field("columns", &self.columns)
            .field("budget", &self.budget)
            .field("max_open_runs", &self.max_open_runs)
            .finish()
    }
}

impl Default for ExternalSorter {
    fn default() -> ExternalSorter {
        ExternalSorter::new()
    }
}

impl ExternalSorter {
    /// Creates a new sorter with a 64 MiB budget, a fan-in of 16, no sort
    /// columns and the default lexicographic byte comparator.
    pub fn new() -> ExternalSorter {
        ExternalSorter {
            columns: vec![],
            budget: DEFAULT_BUDGET,
            max_open_runs: DEFAULT_MAX_OPEN_RUNS,
            cmp: None,
            hook: None,
        }
    }

    /// Sets the column indices that make up the sort key.
    ///
    /// Columns are compared in the order given; the first differing column
    /// decides the order. Every record (and the header, if present) must
    /// have at least `max(columns) + 1` fields, otherwise
    /// [`SortError::MissingColumn`] is returned.
    pub fn columns(&mut self, columns: &[usize]) -> &mut ExternalSorter {
        self.columns = columns.to_vec();
        self
    }

    /// Sets the memory budget in bytes.
    ///
    /// See the [module level documentation](self) for how memory use is
    /// estimated. A single record whose estimate exceeds the budget is
    /// rejected with [`SortError::RecordExceedsBudget`].
    pub fn memory_budget(&mut self, bytes: usize) -> &mut ExternalSorter {
        self.budget = bytes;
        self
    }

    /// Sets the maximum number of run files that may be open at once.
    ///
    /// If more runs are produced, they are merged in multiple rounds. Must
    /// be at least 2.
    pub fn max_open_runs(&mut self, n: usize) -> &mut ExternalSorter {
        self.max_open_runs = n;
        self
    }

    /// Sets the comparator used to order individual key fields.
    ///
    /// The comparator is applied column by column (in the order given to
    /// [`ExternalSorter::columns`]); the overall record order is the
    /// lexicographic order of the column comparisons. If no comparator is
    /// set, fields compare as raw byte strings (`Ord for [u8]`).
    pub fn compare_by<F>(&mut self, cmp: F) -> &mut ExternalSorter
    where
        F: Fn(&[u8], &[u8]) -> Ordering + 'static,
    {
        self.cmp = Some(Box::new(cmp));
        self
    }

    /// Registers a callback that is invoked at key points of the sort.
    ///
    /// Returning an error from the callback aborts the sort (after cleaning
    /// up temporary files). This is intended for observability and for
    /// injecting failures in tests.
    pub fn on_event<F>(&mut self, hook: F) -> &mut ExternalSorter
    where
        F: FnMut(SortEvent) -> Result<(), SortError> + 'static,
    {
        self.hook = Some(Box::new(hook));
        self
    }

    /// Sorts the CSV file at `input` and writes the result to `output`.
    ///
    /// `rdr` and `wtr` configure how the input is parsed and how the output
    /// (and all intermediate run files) are written; they are used exactly
    /// as if the caller had built the `Reader`/`Writer` themselves, except
    /// that run files are always read back with `has_headers(false)` since
    /// headers are never written to run files.
    ///
    /// If `rdr` has headers enabled, the header is written to the output
    /// exactly once, before all data records, and does not participate in
    /// sorting.
    ///
    /// The output is first written to a temporary file in the same
    /// directory as `output` and renamed over `output` only after the sort
    /// fully succeeded. On any error, temporary files are removed and a
    /// pre-existing `output` is preserved.
    pub fn sort_path<P: AsRef<Path>>(
        &mut self,
        input: P,
        output: P,
        rdr: &ReaderBuilder,
        wtr: &WriterBuilder,
    ) -> Result<SortStats, SortError> {
        let input = input.as_ref();
        let output = output.as_ref();
        self.validate_config()?;

        let out_dir =
            output.parent().map(Path::to_path_buf).unwrap_or_default();
        let out_name =
            output.file_name().unwrap_or_else(|| OsStr::new("output"));
        let pid = std::process::id();
        let tmp_output = out_dir.join(format!(
            ".{}.csv-sort-{}.tmp",
            out_name.to_string_lossy(),
            pid
        ));
        let mut guard = TempGuard::default();
        guard.add(tmp_output.clone());

        // Phase 1: read the input and spill sorted runs.
        let mut run_counter = 0u64;
        let mut new_run_path = || {
            run_counter += 1;
            out_dir.join(format!(
                ".{}.csv-sort-{}-{}.run",
                out_name.to_string_lossy(),
                pid,
                run_counter
            ))
        };
        let mut stats = SortStats::default();
        stats.has_headers = rdr_has_headers(rdr);
        let mut reader = rdr.clone().from_path(input)?;
        let headers = if stats.has_headers {
            let headers = reader.byte_headers()?.clone();
            self.check_columns(&headers, 0)?;
            Some(headers)
        } else {
            None
        };
        let mut runs: Vec<PathBuf> = vec![];
        let mut buf: Vec<ByteRecord> = vec![];
        let mut used = 0usize;
        let mut rec = ByteRecord::new();
        while reader.read_byte_record(&mut rec)? {
            stats.records += 1;
            let estimate = record_memory_estimate(&rec);
            if estimate > self.budget {
                return Err(SortError::RecordExceedsBudget {
                    record: stats.records,
                    estimate,
                    budget: self.budget,
                });
            }
            self.check_columns(&rec, stats.records)?;
            if used + estimate > self.budget && !buf.is_empty() {
                let path = new_run_path();
                guard.add(path.clone());
                self.flush_run(&mut buf, &path, wtr, &mut stats)?;
                used = 0;
                runs.push(path);
            }
            used += estimate;
            buf.push(rec.clone());
        }
        if runs.is_empty() {
            // Everything (possibly nothing) fits in memory: sort and write
            // the output directly, no merge rounds needed.
            buf.sort_by(|a, b| self.compare_records(a, b));
            let mut w = wtr.clone().from_path(&tmp_output)?;
            if let Some(h) = &headers {
                w.write_byte_record(h)?;
            }
            for r in &buf {
                w.write_byte_record(r)?;
            }
            w.flush()?;
            drop(w);
            self.emit(SortEvent::OutputReady {
                tmp_path: tmp_output.clone(),
            })?;
            publish(&tmp_output, output, &mut guard)?;
            guard.clear();
            return Ok(stats);
        }
        if !buf.is_empty() {
            let path = new_run_path();
            guard.add(path.clone());
            self.flush_run(&mut buf, &path, wtr, &mut stats)?;
            runs.push(path);
        }

        // Phase 2: merge rounds until one sorted stream remains.
        let mut current = runs;
        let mut round = 0u64;
        loop {
            round += 1;
            stats.merge_rounds = round;
            if current.len() <= self.max_open_runs {
                // Final round: merge everything into the output temp file.
                self.emit(SortEvent::MergeRoundStart {
                    round,
                    inputs: current.clone(),
                    output: tmp_output.clone(),
                })?;
                self.merge_into(
                    &current,
                    &tmp_output,
                    headers.as_ref(),
                    rdr,
                    wtr,
                )?;
                for p in &current {
                    remove_file_quiet(p);
                    guard.remove(p);
                }
                break;
            }
            // Intermediate round: merge groups of up to `max_open_runs`
            // runs into new runs. Single leftover runs carry over as-is.
            let mut next: Vec<PathBuf> = vec![];
            for group in current.chunks(self.max_open_runs) {
                if group.len() == 1 {
                    next.push(group[0].clone());
                    continue;
                }
                let out = new_run_path();
                guard.add(out.clone());
                self.emit(SortEvent::MergeRoundStart {
                    round,
                    inputs: group.to_vec(),
                    output: out.clone(),
                })?;
                self.merge_into(group, &out, None, rdr, wtr)?;
                for p in group {
                    remove_file_quiet(p);
                    guard.remove(p);
                }
                next.push(out);
            }
            current = next;
        }

        // Phase 3: publish.
        self.emit(SortEvent::OutputReady { tmp_path: tmp_output.clone() })?;
        publish(&tmp_output, output, &mut guard)?;
        guard.clear();
        Ok(stats)
    }

    fn validate_config(&self) -> Result<(), SortError> {
        if self.columns.is_empty() {
            return Err(SortError::InvalidConfig(
                "at least one sort column is required".to_string(),
            ));
        }
        if self.budget == 0 {
            return Err(SortError::InvalidConfig(
                "memory budget must be greater than zero".to_string(),
            ));
        }
        if self.max_open_runs < 2 {
            return Err(SortError::InvalidConfig(
                "max_open_runs must be at least 2".to_string(),
            ));
        }
        Ok(())
    }

    fn compare_fields(&self, a: &[u8], b: &[u8]) -> Ordering {
        match &self.cmp {
            Some(cmp) => cmp(a, b),
            None => a.cmp(b),
        }
    }

    fn compare_records(&self, a: &ByteRecord, b: &ByteRecord) -> Ordering {
        for &col in &self.columns {
            let ord = self.compare_fields(&a[col], &b[col]);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }

    fn check_columns(
        &self,
        rec: &ByteRecord,
        record: u64,
    ) -> Result<(), SortError> {
        for &col in &self.columns {
            if col >= rec.len() {
                return Err(SortError::MissingColumn {
                    record,
                    column: col,
                    record_len: rec.len(),
                });
            }
        }
        Ok(())
    }

    fn emit(&mut self, event: SortEvent) -> Result<(), SortError> {
        if let Some(hook) = &mut self.hook {
            hook(event)?;
        }
        Ok(())
    }

    /// Stably sorts `buf`, writes it to `path` as a run file and records
    /// the run in `stats`.
    fn flush_run(
        &mut self,
        buf: &mut Vec<ByteRecord>,
        path: &Path,
        wtr: &WriterBuilder,
        stats: &mut SortStats,
    ) -> Result<(), SortError> {
        // Vec::sort_by is a stable sort, so equal keys keep their original
        // (input) order within the run.
        buf.sort_by(|a, b| self.compare_records(a, b));
        let mut w = wtr.clone().from_path(path)?;
        for r in buf.iter() {
            w.write_byte_record(r)?;
        }
        w.flush()?;
        drop(w);
        let bytes = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        stats.runs += 1;
        stats.run_bytes += bytes;
        self.emit(SortEvent::RunCreated {
            path: path.to_path_buf(),
            records: buf.len() as u64,
            bytes,
        })?;
        buf.clear();
        Ok(())
    }

    /// Merges the sorted run files in `inputs` into the file at `output`.
    ///
    /// If `header` is given, it is written first (used only for the final
    /// merge into the output temp file). Runs are read with the caller's
    /// reader configuration, except that headers are disabled.
    fn merge_into(
        &mut self,
        inputs: &[PathBuf],
        output: &Path,
        header: Option<&ByteRecord>,
        rdr: &ReaderBuilder,
        wtr: &WriterBuilder,
    ) -> Result<(), SortError> {
        let mut w = wtr.clone().from_path(output)?;
        if let Some(h) = header {
            w.write_byte_record(h)?;
        }
        let mut readers = vec![];
        for path in inputs {
            let r = rdr.clone().has_headers(false).from_path(path)?;
            readers.push(r);
        }
        let mut current: Vec<Option<ByteRecord>> = vec![];
        for r in &mut readers {
            current.push(read_one(r)?);
        }
        loop {
            // Pick the smallest head record. Ties go to the earliest run,
            // which preserves the original input order (stability).
            let mut best: Option<usize> = None;
            for (i, rec) in current.iter().enumerate() {
                if rec.is_none() {
                    continue;
                }
                best = Some(match best {
                    None => i,
                    Some(j) => {
                        let ord = self.compare_records(
                            current[i].as_ref().unwrap(),
                            current[j].as_ref().unwrap(),
                        );
                        if ord == Ordering::Less {
                            i
                        } else {
                            j
                        }
                    }
                });
            }
            let i = match best {
                None => break,
                Some(i) => i,
            };
            let rec = current[i].take().unwrap();
            w.write_byte_record(&rec)?;
            current[i] = read_one(&mut readers[i])?;
        }
        w.flush()?;
        Ok(())
    }
}

fn read_one<R: io::Read>(
    r: &mut crate::Reader<R>,
) -> Result<Option<ByteRecord>, SortError> {
    let mut rec = ByteRecord::new();
    if r.read_byte_record(&mut rec)? {
        Ok(Some(rec))
    } else {
        Ok(None)
    }
}

fn rdr_has_headers(rdr: &ReaderBuilder) -> bool {
    // ReaderBuilder does not expose its configuration, but a reader built
    // from it does. Build a throwaway reader over an empty byte slice to
    // read the flag back.
    rdr.clone().from_reader(io::empty()).has_headers()
}

/// Renames `tmp` over `target`, preserving the original `target` if the
/// rename cannot be completed.
fn publish(
    tmp: &Path,
    target: &Path,
    guard: &mut TempGuard,
) -> Result<(), SortError> {
    #[cfg(unix)]
    {
        // On Unix, rename atomically replaces the target.
        fs::rename(tmp, target)?;
        guard.remove(tmp);
        Ok(())
    }
    #[cfg(not(unix))]
    {
        // Windows refuses to rename over an existing file, so move the
        // original aside first and restore it if the publish fails.
        if target.exists() {
            let backup = target.with_extension("csv-sort-backup");
            guard.add(backup.clone());
            fs::rename(target, &backup)?;
            match fs::rename(tmp, target) {
                Ok(()) => {
                    guard.remove(tmp);
                    remove_file_quiet(&backup);
                    guard.remove(&backup);
                    Ok(())
                }
                Err(err) => {
                    let _ = fs::rename(&backup, target);
                    guard.remove(&backup);
                    Err(err.into())
                }
            }
        } else {
            fs::rename(tmp, target)?;
            guard.remove(tmp);
            Ok(())
        }
    }
}

fn remove_file_quiet(path: &Path) {
    let _ = fs::remove_file(path);
}

/// Deletes temporary files on drop unless they have been removed from the
/// guard (successful publish) or the guard was cleared.
#[derive(Default)]
struct TempGuard {
    paths: Vec<PathBuf>,
}

impl TempGuard {
    fn add(&mut self, path: PathBuf) {
        self.paths.push(path);
    }

    fn remove(&mut self, path: &Path) {
        self.paths.retain(|p| p != path);
    }

    fn clear(&mut self) {
        self.paths.clear();
    }
}

impl Drop for TempGuard {
    fn drop(&mut self) {
        for path in &self.paths {
            remove_file_quiet(path);
        }
    }
}

/// Statistics about a completed sort, returned by
/// [`ExternalSorter::sort_path`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SortStats {
    /// The number of data records sorted (excluding the header).
    pub records: u64,
    /// The number of initial sorted runs written to disk. Zero when all
    /// records fit into the memory budget.
    pub runs: u64,
    /// The number of merge rounds performed. Zero when no runs were
    /// spilled; one or more otherwise.
    pub merge_rounds: u64,
    /// The total size in bytes of the initial run files.
    pub run_bytes: u64,
    /// Whether the input was read with a header row.
    pub has_headers: bool,
}

/// Events reported to the callback registered with
/// [`ExternalSorter::on_event`].
#[derive(Clone, Debug)]
pub enum SortEvent {
    /// A sorted run file was written to disk.
    RunCreated {
        /// The path of the run file.
        path: PathBuf,
        /// The number of records in the run.
        records: u64,
        /// The size of the run file in bytes.
        bytes: u64,
    },
    /// A merge round is about to merge `inputs` into `output`.
    MergeRoundStart {
        /// The 1-based merge round number.
        round: u64,
        /// The run files that will be merged in this round.
        inputs: Vec<PathBuf>,
        /// The file the merged records will be written to (an intermediate
        /// run, or the temporary output file in the final round).
        output: PathBuf,
    },
    /// The sorted output temp file is complete and about to be published
    /// (renamed over the target).
    OutputReady {
        /// The temporary output file.
        tmp_path: PathBuf,
    },
}

/// An error returned by [`ExternalSorter::sort_path`].
#[derive(Debug)]
pub enum SortError {
    /// An error from the underlying CSV reader or writer (parse errors,
    /// I/O errors while reading or writing CSV data, ...).
    Csv(Error),
    /// A plain I/O error (e.g. renaming the output into place).
    Io(io::Error),
    /// The sorter was configured incorrectly.
    InvalidConfig(String),
    /// A single record's estimated memory cost exceeds the budget. The
    /// budget is never silently relaxed; sort less data per run or raise
    /// the budget.
    RecordExceedsBudget {
        /// The 1-based data record number (header excluded).
        record: u64,
        /// The record's estimated in-memory cost in bytes.
        estimate: usize,
        /// The configured budget in bytes.
        budget: usize,
    },
    /// A record (or the header, record number 0) does not have the
    /// requested sort column.
    MissingColumn {
        /// The 1-based data record number, or 0 for the header.
        record: u64,
        /// The missing column index.
        column: usize,
        /// The number of fields the record actually has.
        record_len: usize,
    },
    /// The sort was aborted by the [`ExternalSorter::on_event`] callback.
    Aborted(String),
}

impl fmt::Display for SortError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use self::SortError::*;
        match self {
            Csv(err) => write!(f, "csv error: {}", err),
            Io(err) => write!(f, "io error: {}", err),
            InvalidConfig(msg) => write!(f, "invalid sorter config: {}", msg),
            RecordExceedsBudget { record, estimate, budget } => write!(
                f,
                "record {} needs {} bytes, which exceeds the memory \
                 budget of {} bytes",
                record, estimate, budget
            ),
            MissingColumn { record, column, record_len } => write!(
                f,
                "record {} has {} fields, but sort column {} was requested",
                record, record_len, column
            ),
            Aborted(msg) => write!(f, "sort aborted: {}", msg),
        }
    }
}

impl std::error::Error for SortError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use self::SortError::*;
        match self {
            Csv(err) => Some(err),
            Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<Error> for SortError {
    fn from(err: Error) -> SortError {
        SortError::Csv(err)
    }
}

impl From<io::Error> for SortError {
    fn from(err: io::Error) -> SortError {
        SortError::Io(err)
    }
}
