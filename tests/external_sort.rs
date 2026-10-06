//! Integration tests for `csv::external_sort`.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use csv::external_sort::{
    record_memory_estimate, ExternalSorter, SortError, SortEvent, SortStats,
};
use csv::{ByteRecord, ReaderBuilder, Terminator, WriterBuilder};

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> TempDir {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let n = NEXT.fetch_add(1, AtomicOrdering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "csv-external-sort-test-{}-{}-{}",
            tag,
            std::process::id(),
            n
        ));
        fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn byte_record(fields: &[&[u8]]) -> ByteRecord {
    let mut rec = ByteRecord::new();
    for field in fields {
        rec.push_field(field);
    }
    rec
}

/// Builds 12 records with duplicate keys on column 0, a quoted newline, an
/// empty field and non-UTF-8 bytes. Column 2 records the original input
/// order so stability can be verified. Value fields are padded so record
/// size estimates are nearly uniform.
fn tricky_records() -> Vec<ByteRecord> {
    let keys: &[&[u8]] = &[
        b"pear", b"apple", b"kiwi", b"apple", b"pear", b"apple", b"kiwi",
        b"banana", b"apple", b"banana", b"kiwi", b"apple",
    ];
    let mut records = vec![];
    for (i, key) in keys.iter().enumerate() {
        let val: &[u8] = match i {
            1 => b"line1\nline2",      // quoted newline
            4 => b"",                  // empty field
            7 => b"\xff\xfe-not-utf8", // non-UTF-8 bytes
            _ => b"normal",
        };
        let mut padded = val.to_vec();
        padded.resize(16, b'#');
        records.push(byte_record(&[
            key,
            &padded,
            format!("{:04}", i).as_bytes(),
        ]));
    }
    records
}

fn write_input(path: &Path, header: &[&[u8]], records: &[ByteRecord]) {
    let mut wtr = WriterBuilder::new().from_path(path).unwrap();
    wtr.write_record(header).unwrap();
    for rec in records {
        wtr.write_byte_record(rec).unwrap();
    }
    wtr.flush().unwrap();
}

fn read_all(
    path: &Path,
    has_headers: bool,
) -> (Option<ByteRecord>, Vec<ByteRecord>) {
    let mut rb = ReaderBuilder::new();
    rb.has_headers(has_headers);
    let mut rdr = rb.from_path(path).unwrap();
    let header = if has_headers {
        Some(rdr.byte_headers().unwrap().clone())
    } else {
        None
    };
    let mut records = vec![];
    let mut rec = ByteRecord::new();
    while rdr.read_byte_record(&mut rec).unwrap() {
        records.push(rec.clone());
    }
    (header, records)
}

fn assert_no_temp_files(dir: &TempDir) {
    let leftovers: Vec<_> = fs::read_dir(&dir.0)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains("csv-sort"))
        .collect();
    assert!(leftovers.is_empty(), "leftover temp files: {:?}", leftovers);
}

#[test]
fn stable_sort_with_runs_and_merge_rounds_matches_in_memory() {
    let dir = TempDir::new("stable");
    let input = dir.path("input.csv");
    let output = dir.path("sorted.csv");
    let records = tricky_records();
    write_input(&input, &[b"key", b"value", b"tag"], &records);

    // Two records per run: 12 records -> 6 runs; fan-in 3 -> 2 merge rounds.
    let budget = 2 * records.iter().map(record_memory_estimate).max().unwrap();
    let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));
    let events2 = Arc::clone(&events);
    let stats = ExternalSorter::new()
        .columns(&[0])
        .memory_budget(budget)
        .max_open_runs(3)
        .compare_by(|a: &[u8], b: &[u8]| a.cmp(b))
        .on_event(move |event| {
            events2.lock().unwrap().push(format!("{:?}", event));
            Ok(())
        })
        .sort_path(
            &input,
            &output,
            &ReaderBuilder::new(),
            &WriterBuilder::new(),
        )
        .unwrap();

    assert_eq!(stats.records, records.len() as u64);
    assert!(stats.runs >= 3, "expected >= 3 runs, got {:?}", stats);
    assert!(
        stats.merge_rounds >= 2,
        "expected >= 2 merge rounds, got {:?}",
        stats
    );
    assert!(stats.run_bytes > 0);
    assert!(stats.has_headers);
    let events = events.lock().unwrap();
    assert!(events.iter().any(|e| e.contains("RunCreated")));
    assert!(events.iter().any(|e| e.contains("MergeRoundStart")));
    assert!(events.iter().any(|e| e.contains("OutputReady")));
    drop(events);

    // Reference: in-memory stable sort by column 0.
    let mut expected = records.clone();
    expected.sort_by(|a, b| a.get(0).cmp(&b.get(0)));

    let (header, got) = read_all(&output, true);
    // The header is written exactly once and did not participate in
    // sorting ("key" would sort between "kiwi" and "pear" as data).
    let header = header.unwrap();
    assert_eq!(header, byte_record(&[b"key", b"value", b"tag"]));
    assert!(got.iter().all(|r| r != &header));
    // Field-by-field comparison with the in-memory stable sort; this also
    // proves stability (tag column preserves input order for equal keys),
    // and that quoted newlines, empty fields and non-UTF-8 bytes survive.
    assert_eq!(got.len(), expected.len());
    for (g, e) in got.iter().zip(expected.iter()) {
        assert_eq!(g, e);
    }
    assert_no_temp_files(&dir);
}

#[test]
fn custom_dialect_configs_are_honored() {
    let dir = TempDir::new("dialect");
    let input = dir.path("input.csv");
    let output = dir.path("sorted.csv");

    let mut rb = ReaderBuilder::new();
    rb.delimiter(b';')
        .quote(b'\'')
        .double_quote(false)
        .escape(Some(b'\\'))
        .terminator(Terminator::CRLF);
    let mut wb = WriterBuilder::new();
    wb.delimiter(b';')
        .quote(b'\'')
        .double_quote(false)
        .escape(b'\\')
        .terminator(Terminator::CRLF);

    // Fields exercising the dialect: the delimiter, the quote char and a
    // CRLF sequence inside a quoted field.
    let records = vec![
        byte_record(&[b"b", b"has;delimiter"]),
        byte_record(&[b"a", b"it's quoted"]),
        byte_record(&[b"c", b"multi\r\nline"]),
        byte_record(&[b"a", b"second a"]),
    ];
    let mut wtr = wb.clone().from_path(&input).unwrap();
    wtr.write_record(["k", "v"]).unwrap();
    for rec in &records {
        wtr.write_byte_record(rec).unwrap();
    }
    wtr.flush().unwrap();
    drop(wtr);

    let stats = ExternalSorter::new()
        .columns(&[0, 1])
        .memory_budget(1 << 20)
        .sort_path(&input, &output, &rb, &wb)
        .unwrap();
    assert_eq!(stats.records, 4);
    assert_eq!(stats.runs, 0, "everything fits in memory");

    // Read back with the same dialect.
    let mut rdr = rb.from_path(&output).unwrap();
    assert_eq!(
        rdr.byte_headers().unwrap().clone(),
        byte_record(&[b"k", b"v"])
    );
    let mut got = vec![];
    let mut rec = ByteRecord::new();
    while rdr.read_byte_record(&mut rec).unwrap() {
        got.push(rec.clone());
    }
    let mut expected = records.clone();
    expected
        .sort_by(|a, b| a.get(0).cmp(&b.get(0)).then(a.get(1).cmp(&b.get(1))));
    assert_eq!(got, expected);

    // The raw output uses CRLF terminators.
    let raw = fs::read(&output).unwrap();
    assert!(raw.windows(2).any(|w| w == b"\r\n"));
    assert_no_temp_files(&dir);
}

#[test]
fn custom_comparator_is_used() {
    let dir = TempDir::new("cmp");
    let input = dir.path("input.csv");
    let output = dir.path("sorted.csv");
    let records = vec![
        byte_record(&[b"10", b"x"]),
        byte_record(&[b"9", b"x"]),
        byte_record(&[b"100", b"x"]),
        byte_record(&[b"2", b"x"]),
    ];
    write_input(&input, &[b"n", b"v"], &records);

    let numeric = |a: &[u8], b: &[u8]| {
        let pa = String::from_utf8_lossy(a).parse::<u64>().unwrap();
        let pb = String::from_utf8_lossy(b).parse::<u64>().unwrap();
        pa.cmp(&pb)
    };
    ExternalSorter::new()
        .columns(&[0])
        .compare_by(numeric)
        .sort_path(
            &input,
            &output,
            &ReaderBuilder::new(),
            &WriterBuilder::new(),
        )
        .unwrap();

    let (_, got) = read_all(&output, true);
    let keys: Vec<&[u8]> = got.iter().map(|r| r.get(0).unwrap()).collect();
    assert_eq!(keys, vec![b"2".as_slice(), b"9", b"10", b"100"]);
}

#[test]
fn merge_read_failure_preserves_target_and_cleans_temp_files() {
    let dir = TempDir::new("mergefail");
    let input = dir.path("input.csv");
    let output = dir.path("sorted.csv");
    let records = tricky_records();
    write_input(&input, &[b"key", b"value", b"tag"], &records);
    fs::write(&output, b"original-contents").unwrap();

    // One record per run -> many runs; fan-in 2 -> several merge rounds.
    let budget = records.iter().map(record_memory_estimate).max().unwrap();
    let result = ExternalSorter::new()
        .columns(&[0])
        .memory_budget(budget)
        .max_open_runs(2)
        .on_event(|event| {
            // Inject a merge read failure: delete a run file just before
            // the first merge round opens it.
            if let SortEvent::MergeRoundStart { round: 1, inputs, .. } = event
            {
                fs::remove_file(&inputs[0]).unwrap();
            }
            Ok(())
        })
        .sort_path(
            &input,
            &output,
            &ReaderBuilder::new(),
            &WriterBuilder::new(),
        );
    let err = result.unwrap_err();
    assert!(
        matches!(err, SortError::Csv(_) | SortError::Io(_)),
        "unexpected error: {:?}",
        err
    );
    // The pre-existing target is untouched and temp files are gone.
    assert_eq!(fs::read(&output).unwrap(), b"original-contents");
    assert_no_temp_files(&dir);
}

#[test]
fn aborted_hook_preserves_target_and_cleans_temp_files() {
    let dir = TempDir::new("abort");
    let input = dir.path("input.csv");
    let output = dir.path("sorted.csv");
    let records = tricky_records();
    write_input(&input, &[b"key", b"value", b"tag"], &records);
    fs::write(&output, b"original-contents").unwrap();

    let result = ExternalSorter::new()
        .columns(&[0])
        .on_event(|event| {
            if let SortEvent::OutputReady { .. } = event {
                return Err(SortError::Aborted("injected abort".to_string()));
            }
            Ok(())
        })
        .sort_path(
            &input,
            &output,
            &ReaderBuilder::new(),
            &WriterBuilder::new(),
        );
    assert!(matches!(result.unwrap_err(), SortError::Aborted(_)));
    assert_eq!(fs::read(&output).unwrap(), b"original-contents");
    assert_no_temp_files(&dir);
}

#[test]
fn parse_failure_preserves_target_and_cleans_temp_files() {
    let dir = TempDir::new("parsefail");
    let input = dir.path("input.csv");
    let output = dir.path("sorted.csv");
    // Unequal record lengths with a non-flexible reader: the parse fails
    // on the second record.
    fs::write(&input, b"a,b\n1,2\n3,4,5\n").unwrap();
    fs::write(&output, b"original-contents").unwrap();

    let result = ExternalSorter::new().columns(&[0]).sort_path(
        &input,
        &output,
        &ReaderBuilder::new(),
        &WriterBuilder::new(),
    );
    assert!(matches!(result.unwrap_err(), SortError::Csv(_)));
    assert_eq!(fs::read(&output).unwrap(), b"original-contents");
    assert_no_temp_files(&dir);
}

#[test]
fn empty_input_produces_empty_output() {
    let dir = TempDir::new("empty");
    let input = dir.path("input.csv");
    let output = dir.path("sorted.csv");
    fs::write(&input, b"").unwrap();

    let mut rb = ReaderBuilder::new();
    rb.has_headers(false);
    let stats: SortStats = ExternalSorter::new()
        .columns(&[0])
        .sort_path(&input, &output, &rb, &WriterBuilder::new())
        .unwrap();
    assert_eq!(
        stats,
        SortStats {
            records: 0,
            runs: 0,
            merge_rounds: 0,
            run_bytes: 0,
            has_headers: false,
        }
    );
    assert_eq!(fs::read(&output).unwrap(), b"");
    assert_no_temp_files(&dir);
}

#[test]
fn header_only_input_produces_header_only_output() {
    let dir = TempDir::new("headeronly");
    let input = dir.path("input.csv");
    let output = dir.path("sorted.csv");
    fs::write(&input, b"alpha,beta,gamma\n").unwrap();

    let stats = ExternalSorter::new()
        .columns(&[1])
        .sort_path(
            &input,
            &output,
            &ReaderBuilder::new(),
            &WriterBuilder::new(),
        )
        .unwrap();
    assert_eq!(stats.records, 0);
    assert!(stats.has_headers);
    assert_eq!(fs::read(&output).unwrap(), b"alpha,beta,gamma\n");
    assert_no_temp_files(&dir);
}

#[test]
fn out_of_bounds_column_is_an_error() {
    let dir = TempDir::new("oob");
    let input = dir.path("input.csv");
    let output = dir.path("sorted.csv");
    fs::write(&input, b"a,b,c\n1,2,3\n").unwrap();

    let result = ExternalSorter::new().columns(&[5]).sort_path(
        &input,
        &output,
        &ReaderBuilder::new(),
        &WriterBuilder::new(),
    );
    match result.unwrap_err() {
        SortError::MissingColumn { record: 0, column: 5, record_len: 3 } => {}
        err => panic!("unexpected error: {:?}", err),
    }
    // The header is checked first (record 0), so this also covers the
    // header-only case. Nothing may be published.
    assert!(!output.exists());
    assert_no_temp_files(&dir);
}

#[test]
fn oversized_single_record_is_rejected() {
    let dir = TempDir::new("toobig");
    let input = dir.path("input.csv");
    let output = dir.path("sorted.csv");
    let records = tricky_records();
    write_input(&input, &[b"key", b"value", b"tag"], &records);
    fs::write(&output, b"original-contents").unwrap();

    // One byte less than the smallest record needs: the budget is not
    // silently relaxed, the record is rejected.
    let budget = records.iter().map(record_memory_estimate).min().unwrap() - 1;
    let result =
        ExternalSorter::new().columns(&[0]).memory_budget(budget).sort_path(
            &input,
            &output,
            &ReaderBuilder::new(),
            &WriterBuilder::new(),
        );
    match result.unwrap_err() {
        SortError::RecordExceedsBudget { record: 1, estimate, budget: b } => {
            assert_eq!(estimate, record_memory_estimate(&records[0]));
            assert_eq!(b, budget);
        }
        err => panic!("unexpected error: {:?}", err),
    }
    assert_eq!(fs::read(&output).unwrap(), b"original-contents");
    assert_no_temp_files(&dir);
}

#[test]
fn invalid_configuration_is_rejected() {
    let dir = TempDir::new("config");
    let input = dir.path("input.csv");
    let output = dir.path("sorted.csv");
    fs::write(&input, b"a\n1\n").unwrap();

    // No sort columns.
    let result = ExternalSorter::new().sort_path(
        &input,
        &output,
        &ReaderBuilder::new(),
        &WriterBuilder::new(),
    );
    assert!(matches!(result.unwrap_err(), SortError::InvalidConfig(_)));

    // Fan-in below 2.
    let result =
        ExternalSorter::new().columns(&[0]).max_open_runs(1).sort_path(
            &input,
            &output,
            &ReaderBuilder::new(),
            &WriterBuilder::new(),
        );
    assert!(matches!(result.unwrap_err(), SortError::InvalidConfig(_)));

    // Zero budget.
    let result =
        ExternalSorter::new().columns(&[0]).memory_budget(0).sort_path(
            &input,
            &output,
            &ReaderBuilder::new(),
            &WriterBuilder::new(),
        );
    assert!(matches!(result.unwrap_err(), SortError::InvalidConfig(_)));
    assert!(!output.exists());
    assert_no_temp_files(&dir);
}
