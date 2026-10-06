//! Integration tests for the reusable external sort module (`csv::extsort`).
//!
//! Run with: cargo test --test extsort

use std::cmp::Ordering;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use csv::extsort::{
    estimate_record_size, ExternalSorter, SortError, SortStats,
};
use csv::{ByteRecord, ReaderBuilder, WriterBuilder};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique temporary directory that is removed on drop.
struct TestDir(PathBuf);

impl TestDir {
    fn new(name: &str) -> TestDir {
        let n = TEST_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "csv-extsort-test-{}-{}-{}",
            std::process::id(),
            n,
            name
        ));
        fs::create_dir_all(&dir).unwrap();
        TestDir(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }

    /// Lists files left in the directory (used to check temp cleanup).
    fn leftover_files(&self) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(&self.0)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| {
                n.contains("extsort")
                    || n.ends_with(".tmp")
                    || n.ends_with(".run")
            })
            .collect();
        names.sort();
        names
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn write_input(path: &Path, headers: &[&str], rows: &[Vec<&[u8]>]) {
    let mut wtr = WriterBuilder::new().from_path(path).unwrap();
    if !headers.is_empty() {
        wtr.write_byte_record(&ByteRecord::from(
            headers.iter().map(|s| s.as_bytes()).collect::<Vec<_>>(),
        ))
        .unwrap();
    }
    for row in rows {
        wtr.write_byte_record(&ByteRecord::from(row.clone())).unwrap();
    }
    wtr.flush().unwrap();
}

fn read_all(
    path: &Path,
    has_headers: bool,
) -> (Option<ByteRecord>, Vec<ByteRecord>) {
    let mut rdr =
        ReaderBuilder::new().has_headers(has_headers).from_path(path).unwrap();
    let headers = if has_headers {
        Some(rdr.byte_headers().unwrap().clone())
    } else {
        None
    };
    let mut records = vec![];
    let mut rec = ByteRecord::new();
    while rdr.read_byte_record(&mut rec).unwrap() {
        records.push(rec.clone());
    }
    (headers, records)
}

fn lex(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}

/// The tricky acceptance input: duplicate keys, quoted newlines, empty
/// fields and non-UTF-8 bytes. Column 0 is the key, column 2 a sequence
/// number recording the original order.
fn tricky_rows() -> Vec<Vec<&'static [u8]>> {
    vec![
        vec![b"pear", b"first", b"0"],
        vec![b"apple", b"red", b"1"],
        vec![b"pear", b"second\nwith newline", b"2"],
        vec![b"banana", b"", b"3"],
        vec![b"apple", b"green", b"4"],
        vec![b"\xff\xfe raw", b"non-utf8 \x80 bytes", b"5"],
        vec![b"banana", b"yellow", b"6"],
        vec![b"pear", b"third", b"7"],
        vec![b"", b"empty key", b"8"],
        vec![b"apple", b"", b"9"],
        vec![b"qu\"ote", b"embedded quote", b"10"],
        vec![b"banana", b"last", b"11"],
    ]
}

#[test]
fn sorts_tricky_data_with_runs_and_rounds() {
    let dir = TestDir::new("tricky");
    let input = dir.path().join("input.csv");
    let output = dir.path().join("output.csv");
    write_input(&input, &["key", "payload", "seq"], &tricky_rows());

    // Budget of 400 bytes: each record estimates to ~170 bytes, so at most
    // two records fit per run -> at least 6 runs for 12 records.
    let sorter =
        ExternalSorter::new(vec![0], lex).memory_budget(400).max_open_runs(2);
    let stats = sorter
        .sort_file(
            &ReaderBuilder::new(),
            &input,
            &WriterBuilder::new(),
            &output,
        )
        .unwrap();

    assert!(
        stats.runs_created >= 3,
        "expected at least 3 runs, got {}",
        stats.runs_created
    );
    assert!(
        stats.merge_rounds >= 2,
        "expected at least 2 merge rounds, got {}",
        stats.merge_rounds
    );
    assert_eq!(stats.records_read, 12);
    assert_eq!(stats.records_written, 12);

    // Compare field-by-field against an in-memory stable sort.
    let mut expected: Vec<ByteRecord> =
        tricky_rows().into_iter().map(ByteRecord::from).collect();
    expected.sort_by(|a, b| a[0].cmp(&b[0]));
    let (headers, got) = read_all(&output, true);
    assert_eq!(
        headers.unwrap(),
        ByteRecord::from(vec!["key", "payload", "seq"])
    );
    assert_eq!(got.len(), expected.len());
    for (i, (g, e)) in got.iter().zip(expected.iter()).enumerate() {
        assert_eq!(g.len(), e.len(), "record {} field count", i);
        for (j, (gf, ef)) in g.iter().zip(e.iter()).enumerate() {
            assert_eq!(gf, ef, "record {} field {}", i, j);
        }
    }

    // Stability: equal keys must keep their original relative order.
    let pears: Vec<&[u8]> =
        got.iter().filter(|r| &r[0] == b"pear").map(|r| &r[2]).collect();
    assert_eq!(pears, vec![b"0", b"2", b"7"]);
    let apples: Vec<&[u8]> =
        got.iter().filter(|r| &r[0] == b"apple").map(|r| &r[2]).collect();
    assert_eq!(apples, vec![b"1", b"4", b"9"]);

    // Non-UTF-8 data survived byte-for-byte.
    let raw = got.iter().find(|r| r[0].starts_with(b"\xff\xfe")).unwrap();
    assert_eq!(&raw[1], b"non-utf8 \x80 bytes");
    // Quoted newline survived inside one record.
    let nl = got.iter().find(|r| r[1].contains(&b'\n')).unwrap();
    assert_eq!(&nl[1], b"second\nwith newline");

    // All temporary files are gone.
    assert_eq!(dir.leftover_files(), Vec::<String>::new());
}

#[test]
fn empty_input_produces_empty_output() {
    let dir = TestDir::new("empty");
    let input = dir.path().join("input.csv");
    let output = dir.path().join("output.csv");
    fs::write(&input, b"").unwrap();

    // Without headers.
    let sorter = ExternalSorter::new(vec![0], lex).memory_budget(128);
    let mut rdr_builder = ReaderBuilder::new();
    rdr_builder.has_headers(false);
    let stats = sorter
        .sort_file(&rdr_builder, &input, &WriterBuilder::new(), &output)
        .unwrap();
    assert_eq!(
        stats,
        SortStats {
            records_read: 0,
            records_written: 0,
            runs_created: 0,
            merge_rounds: 0,
            bytes_spilled: 0,
            peak_buffer_estimate: 0,
        }
    );
    assert_eq!(fs::read(&output).unwrap(), b"");

    // With headers enabled, empty input must not fabricate a record.
    let stats = sorter
        .sort_file(
            &ReaderBuilder::new(),
            &input,
            &WriterBuilder::new(),
            &output,
        )
        .unwrap();
    assert_eq!(stats.records_written, 0);
    assert_eq!(fs::read(&output).unwrap(), b"");
    assert_eq!(dir.leftover_files(), Vec::<String>::new());
}

#[test]
fn header_only_input_writes_header_once() {
    let dir = TestDir::new("header-only");
    let input = dir.path().join("input.csv");
    let output = dir.path().join("output.csv");
    // Header sorts after any data key on purpose ("zzz" > everything), to
    // prove it is not taking part in the sort.
    write_input(&input, &["zzz", "yyy"], &[]);

    let sorter = ExternalSorter::new(vec![0], lex).memory_budget(64);
    let stats = sorter
        .sort_file(
            &ReaderBuilder::new(),
            &input,
            &WriterBuilder::new(),
            &output,
        )
        .unwrap();
    assert_eq!(stats.records_read, 0);
    assert_eq!(stats.records_written, 0);

    let (headers, records) = read_all(&output, true);
    assert_eq!(headers.unwrap(), ByteRecord::from(vec!["zzz", "yyy"]));
    assert!(records.is_empty());
    // The raw file contains exactly one line: the header, once.
    let raw = fs::read_to_string(&output).unwrap();
    assert_eq!(raw, "zzz,yyy\n");
}

#[test]
fn header_is_not_sorted_with_data() {
    let dir = TestDir::new("header-not-sorted");
    let input = dir.path().join("input.csv");
    let output = dir.path().join("output.csv");
    // "zzz" header would sort last if treated as data.
    write_input(
        &input,
        &["zzz", "h2"],
        &[vec![b"aaa", b"1"], vec![b"bbb", b"2"]],
    );

    let sorter = ExternalSorter::new(vec![0], lex).memory_budget(1 << 20);
    sorter
        .sort_file(
            &ReaderBuilder::new(),
            &input,
            &WriterBuilder::new(),
            &output,
        )
        .unwrap();
    let raw = fs::read_to_string(&output).unwrap();
    assert_eq!(raw, "zzz,h2\naaa,1\nbbb,2\n");
}

#[test]
fn column_out_of_bounds_is_rejected() {
    let dir = TestDir::new("column-oob");
    let input = dir.path().join("input.csv");
    let output = dir.path().join("output.csv");
    write_input(&input, &["a", "b"], &[vec![b"x", b"1"]]);

    // Column 5 does not exist in the header: fails fast.
    let sorter = ExternalSorter::new(vec![5], lex);
    let err = sorter
        .sort_file(
            &ReaderBuilder::new(),
            &input,
            &WriterBuilder::new(),
            &output,
        )
        .unwrap_err();
    match err {
        SortError::ColumnOutOfBounds { column, record_len, .. } => {
            assert_eq!(column, 5);
            assert_eq!(record_len, 2);
        }
        other => panic!("expected ColumnOutOfBounds, got {:?}", other),
    }
    assert!(!output.exists(), "no output may be published on failure");

    // Without headers, a short record triggers the same error mid-stream.
    let mut rdr_builder = ReaderBuilder::new();
    rdr_builder.has_headers(false);
    let err = sorter
        .sort_file(&rdr_builder, &input, &WriterBuilder::new(), &output)
        .unwrap_err();
    assert!(matches!(err, SortError::ColumnOutOfBounds { .. }));
    assert!(!output.exists());
    assert_eq!(dir.leftover_files(), Vec::<String>::new());
}

#[test]
fn oversized_single_record_is_rejected() {
    let dir = TestDir::new("oversized");
    let input = dir.path().join("input.csv");
    let output = dir.path().join("output.csv");
    let big = vec![b'x'; 4096];
    write_input(
        &input,
        &["k", "v"],
        &[vec![b"a", b"small"], vec![b"b", big.as_slice()]],
    );

    // Budget comfortably fits the small record but not the 4 KiB one.
    let budget = estimate_record_size(&ByteRecord::from(vec![
        b"a".as_slice(),
        b"small".as_slice(),
    ])) + 1;
    let sorter = ExternalSorter::new(vec![0], lex).memory_budget(budget);
    let err = sorter
        .sort_file(
            &ReaderBuilder::new(),
            &input,
            &WriterBuilder::new(),
            &output,
        )
        .unwrap_err();
    match err {
        SortError::RecordTooLarge { estimate, budget: b, record_index } => {
            assert!(estimate > b);
            assert_eq!(b, budget);
            assert_eq!(record_index, 1);
        }
        other => panic!("expected RecordTooLarge, got {:?}", other),
    }
    assert!(!output.exists());
    assert_eq!(dir.leftover_files(), Vec::<String>::new());
}

#[test]
fn merge_read_failure_preserves_target_and_cleans_temp() {
    let dir = TestDir::new("merge-failure");
    let input = dir.path().join("input.csv");
    let output = dir.path().join("output.csv");
    write_input(&input, &["key", "payload", "seq"], &tricky_rows());

    // Phase 1: spill to runs.
    let sorter =
        ExternalSorter::new(vec![0], lex).memory_budget(400).max_open_runs(2);
    let mut rdr = ReaderBuilder::new().from_path(&input).unwrap();
    let spilled = sorter.spill(&mut rdr, dir.path()).unwrap();
    assert!(spilled.run_files().len() >= 3);
    for run in spilled.run_files() {
        assert!(run.exists());
    }

    // Inject a merge read failure: destroy one run file.
    fs::remove_file(&spilled.run_files()[1]).unwrap();

    // The target pre-exists with sentinel content and must be preserved.
    fs::write(&output, b"original target content\n").unwrap();
    let err = sorter
        .merge_to_file(&spilled, &WriterBuilder::new(), &output, dir.path())
        .unwrap_err();
    assert!(
        matches!(err, SortError::Io(_)),
        "expected io error, got {:?}",
        err
    );

    // Original target untouched, all temp files (runs + output tmp) gone.
    assert_eq!(fs::read(&output).unwrap(), b"original target content\n");
    assert_eq!(
        dir.leftover_files(),
        Vec::<String>::new(),
        "temp files must be cleaned after a merge failure"
    );
}

#[test]
fn parse_failure_preserves_target_and_cleans_temp() {
    let dir = TestDir::new("parse-failure");
    let input = dir.path().join("input.csv");
    let output = dir.path().join("output.csv");
    // Ragged input: the third record has a different field count, which the
    // default (non-flexible) reader rejects mid-stream, after some runs may
    // already have been spilled.
    fs::write(&input, b"k,seq\na,1\nb,2\nc,3,oops\nd,4\ne,5\nf,6\ng,7\nh,8\n")
        .unwrap();
    fs::write(&output, b"original target content\n").unwrap();

    let sorter =
        ExternalSorter::new(vec![0], lex).memory_budget(300).max_open_runs(2);
    let err = sorter
        .sort_file(
            &ReaderBuilder::new(),
            &input,
            &WriterBuilder::new(),
            &output,
        )
        .unwrap_err();
    assert!(
        matches!(err, SortError::Csv(_)),
        "expected csv error, got {:?}",
        err
    );
    assert_eq!(fs::read(&output).unwrap(), b"original target content\n");
    assert_eq!(dir.leftover_files(), Vec::<String>::new());
}

#[test]
fn comparator_failure_preserves_target_and_cleans_temp() {
    let dir = TestDir::new("cmp-failure");
    let input = dir.path().join("input.csv");
    let output = dir.path().join("output.csv");
    write_input(&input, &["key", "payload", "seq"], &tricky_rows());
    fs::write(&output, b"original target content\n").unwrap();

    // A comparator that works a few times and then fails.
    let calls = std::cell::Cell::new(0usize);
    let sorter =
        ExternalSorter::new_fallible(vec![0], move |a: &[u8], b: &[u8]| {
            calls.set(calls.get() + 1);
            if calls.get() > 3 {
                return Err("injected comparator failure".into());
            }
            Ok(a.cmp(b))
        })
        .memory_budget(400)
        .max_open_runs(2);
    let err = sorter
        .sort_file(
            &ReaderBuilder::new(),
            &input,
            &WriterBuilder::new(),
            &output,
        )
        .unwrap_err();
    match err {
        SortError::Compare(msg) => {
            assert!(msg.to_string().contains("injected comparator failure"));
        }
        other => panic!("expected Compare error, got {:?}", other),
    }
    assert_eq!(fs::read(&output).unwrap(), b"original target content\n");
    assert_eq!(dir.leftover_files(), Vec::<String>::new());
}

#[test]
fn write_failure_preserves_target_and_cleans_temp() {
    let dir = TestDir::new("write-failure");
    let input = dir.path().join("input.csv");
    let output = dir.path().join("output.csv");
    write_input(&input, &["key", "payload", "seq"], &tricky_rows());
    fs::write(&output, b"original target content\n").unwrap();

    // Sort into a writer that fails immediately.
    struct FailingWriter;
    impl std::io::Write for FailingWriter {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "injected write failure",
            ))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let sorter =
        ExternalSorter::new(vec![0], lex).memory_budget(400).max_open_runs(2);
    let mut rdr = ReaderBuilder::new().from_path(&input).unwrap();
    let err = sorter
        .sort(&mut rdr, &WriterBuilder::new(), FailingWriter, dir.path())
        .unwrap_err();
    // The failing writer surfaces either while writing CSV (Csv error) or
    // at the final flush (Io error); both must clean up identically.
    assert!(
        matches!(err, SortError::Csv(_) | SortError::Io(_)),
        "unexpected error: {:?}",
        err
    );
    assert_eq!(fs::read(&output).unwrap(), b"original target content\n");
    assert_eq!(dir.leftover_files(), Vec::<String>::new());
}

#[test]
fn custom_dialect_configs_are_honored() {
    let dir = TestDir::new("dialect");
    let input = dir.path().join("input.csv");
    let output = dir.path().join("output.csv");

    // Semicolon delimiter, single-quote quoting, CRLF terminator, escape.
    let mut rdr_builder = ReaderBuilder::new();
    rdr_builder
        .delimiter(b';')
        .quote(b'\'')
        .escape(Some(0x5c))
        .terminator(csv::Terminator::CRLF)
        .has_headers(true);
    let mut wtr_builder = WriterBuilder::new();
    wtr_builder
        .delimiter(b';')
        .quote(b'\'')
        .escape(0x5c)
        .terminator(csv::Terminator::CRLF)
        .has_headers(true);

    // Write the input in that dialect, including a quoted newline and a
    // field containing the delimiter.
    {
        let mut wtr = wtr_builder.from_path(&input).unwrap();
        wtr.write_byte_record(&ByteRecord::from(vec!["key", "val"])).unwrap();
        wtr.write_byte_record(&ByteRecord::from(vec!["b", "two\nlines"]))
            .unwrap();
        wtr.write_byte_record(&ByteRecord::from(vec!["a", "semi;colon"]))
            .unwrap();
        wtr.write_byte_record(&ByteRecord::from(vec!["c", "it's quoted"]))
            .unwrap();
        wtr.flush().unwrap();
    }

    let sorter =
        ExternalSorter::new(vec![0], lex).memory_budget(180).max_open_runs(2);
    let stats =
        sorter.sort_file(&rdr_builder, &input, &wtr_builder, &output).unwrap();
    assert!(stats.runs_created >= 2);

    // Parse the output back with the same dialect.
    let mut rdr = rdr_builder.from_path(&output).unwrap();
    assert_eq!(
        rdr.byte_headers().unwrap().clone(),
        ByteRecord::from(vec!["key", "val"])
    );
    let mut got = vec![];
    let mut rec = ByteRecord::new();
    while rdr.read_byte_record(&mut rec).unwrap() {
        got.push(rec.clone());
    }
    assert_eq!(
        got,
        vec![
            ByteRecord::from(vec!["a", "semi;colon"]),
            ByteRecord::from(vec!["b", "two\nlines"]),
            ByteRecord::from(vec!["c", "it's quoted"]),
        ]
    );
    // CRLF terminators were actually used.
    let raw = fs::read(&output).unwrap();
    assert!(raw.windows(2).any(|w| w == b"\r\n"));
    assert_eq!(dir.leftover_files(), Vec::<String>::new());
}

#[test]
fn multi_column_key_and_custom_comparator() {
    let dir = TestDir::new("multi-key");
    let input = dir.path().join("input.csv");
    let output = dir.path().join("output.csv");
    let rows: Vec<Vec<&[u8]>> = vec![
        vec![b"g1", b"10", b"0"],
        vec![b"g2", b"2", b"1"],
        vec![b"g1", b"9", b"2"],
        vec![b"g2", b"10", b"3"],
        vec![b"g1", b"10", b"4"],
    ];
    write_input(&input, &["grp", "n", "seq"], &rows);

    // One comparator is applied to every key column, so make it handle
    // both: numeric when both fields parse as integers ("9" < "10"),
    // lexicographic otherwise ("g1" < "g2").
    let numeric_or_lex = |a: &[u8], b: &[u8]| {
        let pa = String::from_utf8_lossy(a).parse::<u64>();
        let pb = String::from_utf8_lossy(b).parse::<u64>();
        match (pa, pb) {
            (Ok(x), Ok(y)) => x.cmp(&y),
            _ => a.cmp(b),
        }
    };
    let sorter = ExternalSorter::new(vec![0, 1], numeric_or_lex)
        .memory_budget(200)
        .max_open_runs(2);
    sorter
        .sort_file(
            &ReaderBuilder::new(),
            &input,
            &WriterBuilder::new(),
            &output,
        )
        .unwrap();
    let (_, got) = read_all(&output, true);
    let seqs: Vec<&[u8]> = got.iter().map(|r| &r[2]).collect();
    // g1: 9 (seq 2), then 10 twice (seq 0 before 4, stable); then g2: 2, 10.
    assert_eq!(seqs, vec![b"2", b"0", b"4", b"1", b"3"]);
}

#[test]
fn in_memory_sort_when_everything_fits() {
    let dir = TestDir::new("in-memory");
    let input = dir.path().join("input.csv");
    let output = dir.path().join("output.csv");
    write_input(&input, &["k"], &[vec![b"b"], vec![b"a"], vec![b"c"]]);

    let sorter = ExternalSorter::new(vec![0], lex);
    let stats = sorter
        .sort_file(
            &ReaderBuilder::new(),
            &input,
            &WriterBuilder::new(),
            &output,
        )
        .unwrap();
    assert_eq!(stats.runs_created, 0);
    assert_eq!(stats.merge_rounds, 0);
    assert_eq!(stats.bytes_spilled, 0);
    let (_, got) = read_all(&output, true);
    assert_eq!(
        got,
        vec![
            ByteRecord::from(vec!["a"]),
            ByteRecord::from(vec!["b"]),
            ByteRecord::from(vec!["c"]),
        ]
    );
}

#[test]
fn no_headers_mode_sorts_first_row_as_data() {
    let dir = TestDir::new("no-headers");
    let input = dir.path().join("input.csv");
    let output = dir.path().join("output.csv");
    fs::write(&input, b"b,2\na,1\n").unwrap();

    let mut rdr_builder = ReaderBuilder::new();
    rdr_builder.has_headers(false);
    let mut wtr_builder = WriterBuilder::new();
    wtr_builder.has_headers(false);
    let sorter = ExternalSorter::new(vec![0], lex);
    sorter.sort_file(&rdr_builder, &input, &wtr_builder, &output).unwrap();
    assert_eq!(fs::read(&output).unwrap(), b"a,1\nb,2\n");
}
