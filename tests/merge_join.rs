use std::{
    error::Error as StdError,
    fmt, fs, io,
    path::{Path, PathBuf},
};

use csv::{
    merge_join::{
        JoinKind, JoinSide, JoinStatus, MergeJoinBuilder, MergeJoinErrorKind,
        TempFiles,
    },
    ByteRecord, ReaderBuilder,
};

/// A process-wide counter keeping scratch directories unique even when tests
/// share a name or run in parallel.
static TEST_DIR_SEQ: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// A unique, empty scratch directory for a single test.
struct TestDir(PathBuf);

impl TestDir {
    fn new(test_name: &str) -> TestDir {
        let seq =
            TEST_DIR_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "rust-csv-merge-join-{}-{}-{}",
            test_name,
            std::process::id(),
            seq
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        TestDir(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }

    /// List the file names currently in the scratch directory.
    fn files(&self) -> Vec<String> {
        let mut names = fs::read_dir(&self.0)
            .unwrap()
            .map(|entry| {
                entry.unwrap().file_name().to_str().unwrap().to_string()
            })
            .collect::<Vec<_>>();
        names.sort();
        names
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Build a no-headers, flexible CSV reader over in-memory bytes.
fn reader(data: &[u8]) -> csv::Reader<&[u8]> {
    ReaderBuilder::new().has_headers(false).flexible(true).from_reader(data)
}

/// Run a join to a string with full control over the builder.
fn run_join(
    data: (&str, &str),
    configure: impl FnOnce(&mut MergeJoinBuilder),
) -> (String, JoinStatus, TempFiles) {
    let dir = TestDir::new("run");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let mut left_rdr = reader(data.0.as_bytes());
    let mut right_rdr = reader(data.1.as_bytes());
    let mut builder = MergeJoinBuilder::new();
    configure(&mut builder);
    let mut wtr = csv::WriterBuilder::new().flexible(true).from_writer(vec![]);
    let status = builder
        .run_to_writer(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut wtr,
        )
        .unwrap();
    let data = String::from_utf8(wtr.into_inner().unwrap()).unwrap();
    (data, status, temps)
}

#[test]
fn inner_join_basic_cartesian_product() {
    // a/a/b against a/a/c.
    let (data, status, _temps) = run_join(("a\na\nb\n", "a\na\nc\n"), |_| {});
    assert_eq!(data, "a,a\na,a\na,a\na,a\n");
    assert!(!status.is_limit());
    assert_eq!(status.emitted(), 4);
    assert_eq!(status.report().cleanup().removed, 0);
    assert!(status.report().cleanup().is_clean());
}

#[test]
fn left_join_emits_unmatched_left_with_null_marker() {
    let (data, status, _temps) = run_join(("a\na\nb\n", "a\na\nc\n"), |b| {
        b.join_kind(JoinKind::Left);
    });
    assert_eq!(
        data, "a,a\na,a\na,a\na,a\nb,\n",
        "the unmatched b row carries one empty right marker field"
    );
    assert_eq!(status.emitted(), 5);
}

#[test]
fn stable_order_left_then_right_with_full_fields() {
    let left = "a,L0\na,L1\nb,L2\n";
    let right = "a,R0\na,R1\nc,R2\n";

    let (data, status, _temps) = run_join((left, right), |b| {
        b.join_kind(JoinKind::Left);
    });
    let expected = "\
a,L0,a,R0
a,L0,a,R1
a,L1,a,R0
a,L1,a,R1
b,L2,
";
    assert_eq!(data, expected);
    assert_eq!(status.emitted(), 5);
    assert!(!status.is_limit());
}

#[test]
fn spill_duplicated_large_group_matches_in_memory_result() {
    let n = 20usize;
    let make = |prefix: char| {
        (0..n).map(|i| format!("k,{}{}\n", prefix, i)).collect::<String>()
    };
    let left = make('L');
    let right = make('R');

    // Reference result with the default budget (no spilling).
    let (reference, ref_status, ref_temps) = run_join((&left, &right), |_| {});
    assert_eq!(ref_status.emitted(), (n * n) as u64);
    assert_eq!(ref_temps.created(), 0);

    // Budget zero forces every record past the first on each side to spill.
    let dir = TestDir::new("spill-big");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let mut left_rdr = reader(left.as_bytes());
    let mut right_rdr = reader(right.as_bytes());
    let mut wtr = csv::WriterBuilder::new().flexible(true).from_writer(vec![]);
    let status = MergeJoinBuilder::new()
        .memory_budget(0)
        .run_to_writer(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut wtr,
        )
        .unwrap();

    let spilled = String::from_utf8(wtr.into_inner().unwrap()).unwrap();
    assert_eq!(spilled, reference);
    assert_eq!(status.emitted(), (n * n) as u64);
    // Both sides spilled, and the spill files were eagerly reclaimed.
    assert_eq!(temps.created(), 2);
    assert_eq!(temps.removed(), 2);
    assert_eq!(temps.live(), 0);
    assert!(status.report().cleanup().is_clean());
    assert!(
        dir.files().is_empty(),
        "no spill files left behind: {:?}",
        dir.files()
    );

    // Check the row order line by line.
    let mut row = 0;
    for (li, line) in spilled.lines().enumerate() {
        let l = li / n;
        let r = li % n;
        assert_eq!(line, format!("k,L{},k,R{}", l, r), "row {}", row);
        row += 1;
    }
}

#[test]
fn spill_left_join_unmatched_groups_need_no_spill() {
    let n = 15usize;
    // Zero pad so that lexicographic byte order matches numeric order.
    let left = (0..n).map(|i| format!("l{:02}\n", i)).collect::<String>();
    let right = (0..n).map(|i| format!("r{:02}\n", i)).collect::<String>();

    let dir = TestDir::new("spill-left-unmatched");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let mut left_rdr = reader(left.as_bytes());
    let mut right_rdr = reader(right.as_bytes());
    let mut rows: Vec<String> = Vec::new();
    let status = MergeJoinBuilder::new()
        .join_kind(JoinKind::Left)
        .memory_budget(0)
        .run(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut |row| {
                let mut fields: Vec<String> = row
                    .left()
                    .iter()
                    .map(|f| String::from_utf8_lossy(f).into_owned())
                    .collect();
                fields.push(
                    row.right()
                        .map(|r| {
                            r.iter()
                                .map(|f| {
                                    String::from_utf8_lossy(f).into_owned()
                                })
                                .collect::<Vec<_>>()
                                .join(",")
                        })
                        .unwrap_or_default(),
                );
                rows.push(fields.join(","));
                Ok::<_, csv::Error>(())
            },
        )
        .unwrap();
    assert_eq!(status.emitted(), n as u64);
    assert_eq!(
        rows,
        (0..n).map(|i| format!("l{:02},", i)).collect::<Vec<_>>()
    );
    // No key group ever matched, so no spill files were necessary.
    assert_eq!(temps.created(), 0);
    assert!(temps.cleanup_all().is_clean());
}

#[test]
fn spill_with_multiple_distinct_matching_groups() {
    let make = |key: &str, n: usize, tag: char| {
        (0..n).map(|i| format!("{},{}{}\n", key, tag, i)).collect::<String>()
    };
    let left = format!("{}{}", make("a", 6, 'L'), make("b", 4, 'L'));
    let right = format!("{}{}", make("a", 5, 'R'), make("b", 7, 'R'));

    let dir = TestDir::new("spill-multi");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let mut left_rdr = reader(left.as_bytes());
    let mut right_rdr = reader(right.as_bytes());
    let mut wtr = csv::WriterBuilder::new().flexible(true).from_writer(vec![]);
    let status = MergeJoinBuilder::new()
        .memory_budget(0)
        .run_to_writer(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut wtr,
        )
        .unwrap();
    let data = String::from_utf8(wtr.into_inner().unwrap()).unwrap();
    let lines: Vec<&str> = data.lines().collect();

    assert_eq!(lines.len(), 6 * 5 + 4 * 7);
    let mut expected = Vec::new();
    for l in 0..6 {
        for r in 0..5 {
            expected.push(format!("a,L{},a,R{}", l, r));
        }
    }
    for l in 0..4 {
        for r in 0..7 {
            expected.push(format!("b,L{},b,R{}", l, r));
        }
    }
    assert_eq!(lines, expected.iter().map(|s| s.as_str()).collect::<Vec<_>>());
    // Four spilled groups, all cleaned up by the end.
    assert_eq!(temps.created(), 4);
    assert_eq!(temps.removed(), 4);
    assert!(temps.live() == 0);
    assert!(status.report().cleanup().is_clean());
    assert!(dir.files().is_empty());
}

#[test]
fn large_duplicate_group_stress_fully_spilled() {
    let n = 100usize;
    let left = (0..n).map(|i| format!("k,L{:03}\n", i)).collect::<String>();
    let right = (0..n).map(|i| format!("k,R{:03}\n", i)).collect::<String>();

    let dir = TestDir::new("stress");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let mut left_rdr = reader(left.as_bytes());
    let mut right_rdr = reader(right.as_bytes());
    let mut count = 0u64;
    let status = MergeJoinBuilder::new()
        .memory_budget(0)
        .run(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut |row| {
                // Verify stable ordering for every emitted row.
                let li = count as usize / n;
                let ri = count as usize % n;
                assert_eq!(
                    row.left().get(1).unwrap(),
                    format!("L{:03}", li).as_bytes()
                );
                assert_eq!(
                    row.right().unwrap().get(1).unwrap(),
                    format!("R{:03}", ri).as_bytes()
                );
                count += 1;
                Ok::<_, csv::Error>(())
            },
        )
        .unwrap();
    assert_eq!(count, (n * n) as u64);
    assert_eq!(status.emitted(), (n * n) as u64);
    assert_eq!(temps.created(), 2);
    assert_eq!(temps.removed(), 2);
    assert!(temps.live() == 0);
    assert!(dir.files().is_empty());
}

#[test]
fn out_of_order_left_is_reported_with_side_and_position() {
    let dir = TestDir::new("ooo-left");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let mut left_rdr = reader(b"a\nc\nb\n");
    let mut right_rdr = reader(b"a\n");
    let mut emitted = 0u64;
    let err = MergeJoinBuilder::new()
        .run(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut |_| {
                emitted += 1;
                Ok::<_, csv::Error>(())
            },
        )
        .unwrap_err();

    assert_eq!(err.side(), Some(JoinSide::Left));
    let pos = err.position().expect("a position from the CSV reader");
    assert_eq!(pos.record(), 2, "records are zero indexed");
    assert_eq!(pos.line(), 3);
    match err.into_kind() {
        MergeJoinErrorKind::OutOfOrder { previous, current, .. } => {
            assert_eq!(previous, b"c");
            assert_eq!(current, b"b");
        }
        other => panic!("expected OutOfOrder, got {:?}", other),
    }
    // The matching "a" pair was delivered before the violation. The caller is
    // told the result is incomplete rather than receiving a fake completion.
    assert_eq!(emitted, 1);
}

#[test]
fn out_of_order_before_any_match_emits_nothing() {
    let dir = TestDir::new("ooo-early");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let mut left_rdr = reader(b"b\na\n");
    let mut right_rdr = reader(b"a\n");
    let mut emitted = 0u64;
    let err = MergeJoinBuilder::new()
        .run(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut |_| {
                emitted += 1;
                Ok::<_, csv::Error>(())
            },
        )
        .unwrap_err();
    assert_eq!(err.side(), Some(JoinSide::Left));
    assert_eq!(emitted, 0);
}

#[test]
fn out_of_order_right_is_reported_and_prior_rows_remain() {
    let dir = TestDir::new("ooo-right");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let mut left_rdr = reader(b"a\nz\n");
    let mut right_rdr = reader(b"a\nb\na\n");
    let mut rows: Vec<String> = Vec::new();
    let err = MergeJoinBuilder::new()
        .join_kind(JoinKind::Left)
        .run(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut |row| {
                let l = String::from_utf8_lossy(row.left().get(0).unwrap())
                    .into_owned();
                rows.push(l);
                Ok::<_, csv::Error>(())
            },
        )
        .unwrap_err();

    assert_eq!(err.side(), Some(JoinSide::Right));
    assert_eq!(err.position().unwrap().record(), 2);
    // The matched "a" pair was emitted before the right-side violation; the
    // caller is told the result is incomplete rather than getting a fake
    // completion.
    assert_eq!(rows, vec!["a".to_string()]);
    let report = err.cleanup_report().expect("cleanup ran on error");
    assert!(report.is_clean());
}

#[test]
fn non_utf8_keys_and_fields_round_trip() {
    let dir = TestDir::new("non-utf8");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let mut left_data: Vec<u8> = b"k\xff,L0\n".to_vec();
    left_data.extend_from_slice(b"k\xff,L1\n");
    left_data.extend_from_slice(b"m,L\n");
    let mut left_rdr = reader(&left_data);
    let mut right_rdr = reader(b"k\xff,R0\nk\xff,R1\nn,R\n");
    let mut rows: Vec<ByteRecord> = Vec::new();
    let status = MergeJoinBuilder::new()
        .join_kind(JoinKind::Left)
        .memory_budget(0)
        .run(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut |row| {
                rows.push(row.to_byte_record());
                Ok::<_, csv::Error>(())
            },
        )
        .unwrap();
    assert_eq!(status.emitted(), 5);
    assert_eq!(
        rows.iter().map(|r| r.clone()).collect::<Vec<ByteRecord>>(),
        vec![
            ByteRecord::from(vec![&b"k\xff"[..], b"L0", b"k\xff", b"R0"]),
            ByteRecord::from(vec![&b"k\xff"[..], b"L0", b"k\xff", b"R1"]),
            ByteRecord::from(vec![&b"k\xff"[..], b"L1", b"k\xff", b"R0"]),
            ByteRecord::from(vec![&b"k\xff"[..], b"L1", b"k\xff", b"R1"]),
            ByteRecord::from(vec![&b"m"[..], b"L", b""]),
        ]
    );
    // The invalid bytes survived the spill files.
    assert_eq!(temps.created(), 2);
    assert!(temps.cleanup_all().is_clean());
    assert!(dir.files().is_empty());
}

#[test]
fn missing_key_column_is_an_error() {
    let dir = TestDir::new("missing-key");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let mut left_rdr = reader(b"a,b\n");
    let mut right_rdr = reader(b"a\n");
    let err = MergeJoinBuilder::new()
        .key_column(3)
        .run(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut |row| {
                drop(row);
                Ok::<_, csv::Error>(())
            },
        )
        .unwrap_err();
    assert_eq!(err.side(), Some(JoinSide::Left));
    match err.into_kind() {
        MergeJoinErrorKind::MissingKey { column, len, .. } => {
            assert_eq!(column, 3);
            assert_eq!(len, 2);
        }
        other => panic!("expected MissingKey, got {:?}", other),
    }
}

#[test]
fn output_limit_stops_early_and_cleans_up() {
    let dir = TestDir::new("limit");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let mut left_rdr = reader(b"a\na\na\n");
    let mut right_rdr = reader(b"a\na\na\n");
    let mut count = 0u64;
    let status = MergeJoinBuilder::new()
        .max_outputs(4)
        .memory_budget(0)
        .run(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut |_| {
                count += 1;
                Ok::<_, csv::Error>(())
            },
        )
        .unwrap();
    assert!(status.is_limit());
    assert_eq!(count, 4);
    assert_eq!(status.emitted(), 4);
    assert!(status.report().cleanup().is_clean());
    assert_eq!(temps.live(), 0);
    assert!(dir.files().is_empty());
}

#[test]
fn zero_limit_emits_nothing() {
    let dir = TestDir::new("limit-zero");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let mut left_rdr = reader(b"a\n");
    let mut right_rdr = reader(b"a\n");
    let mut count = 0u64;
    let status = MergeJoinBuilder::new()
        .max_outputs(0)
        .run(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut |_| {
                count += 1;
                Ok::<_, csv::Error>(())
            },
        )
        .unwrap();
    assert!(status.is_limit());
    assert_eq!(count, 0);
}

/// A writer that always fails with a distinct I/O error.
#[derive(Debug)]
struct FailWriter;

impl io::Write for FailWriter {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "boom"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "boom"))
    }
}

#[test]
fn sink_write_failure_is_returned_and_temp_files_reclaimed() {
    let dir = TestDir::new("sink-fail");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let mut left_rdr = reader(b"a\na\n");
    let mut right_rdr = reader(b"a\na\n");
    let mut wtr = csv::WriterBuilder::new()
        .flexible(true)
        .buffer_capacity(4)
        .from_writer(FailWriter);
    let err = MergeJoinBuilder::new()
        .memory_budget(0)
        .run_to_writer(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut wtr,
        )
        .unwrap_err();
    assert!(matches!(err.kind(), MergeJoinErrorKind::Sink(_)));
    assert_eq!(err.to_string(), "merge join: the output sink failed: boom");
    let report = err.cleanup_report().expect("cleanup report attached");
    assert!(
        report.is_clean(),
        "sink failure must still reclaim spills: {:?}",
        report
    );
    assert_eq!(temps.live(), 0);
    assert!(dir.files().is_empty());
}

#[test]
fn custom_sink_errors_are_reported() {
    #[derive(Debug)]
    struct CustomError;
    impl fmt::Display for CustomError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("custom sink error")
        }
    }
    impl StdError for CustomError {}

    let dir = TestDir::new("custom-sink-fail");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let mut left_rdr = reader(b"a\n");
    let mut right_rdr = reader(b"a\n");
    let err = MergeJoinBuilder::new()
        .run(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut |_| Err(CustomError),
        )
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "merge join: the output sink failed: custom sink error"
    );
    assert!(err.cleanup_report().unwrap().is_clean());
}

#[test]
fn inner_join_discards_unmatched_groups() {
    let (data, status, _temps) =
        run_join(("a\nb\nb\nc\n", "b\nb\nz\n"), |_| {});
    assert_eq!(data, "b,b\nb,b\nb,b\nb,b\n");
    assert_eq!(status.emitted(), 4);
}

#[test]
fn empty_inputs_emit_nothing_and_cleanup_is_clean() {
    let (data_left, status_left, mut temps_left) =
        run_join(("", "a\n"), |b| {
            b.join_kind(JoinKind::Left);
        });
    assert_eq!(data_left, "");
    assert_eq!(status_left.emitted(), 0);
    assert!(temps_left.cleanup_all().is_clean());

    let (data_both, status_both, _temps) = run_join(("", ""), |_| {});
    assert_eq!(data_both, "");
    assert_eq!(status_both.emitted(), 0);
}

#[test]
fn temp_directory_creation_failure_is_surfaced() {
    // A path whose parent is an existing file cannot be a directory.
    let file = std::env::temp_dir()
        .join(format!("rust-csv-merge-join-notdir-{}", std::process::id()));
    fs::write(&file, b"not a directory").unwrap();
    let blocked = file.join("child");
    let err = TempFiles::new(&blocked).unwrap_err();
    assert!(
        err.kind() == io::ErrorKind::AlreadyExists
            || err.kind() == io::ErrorKind::Other
            || err.raw_os_error().is_some()
    );
    fs::remove_file(&file).unwrap();
}

#[test]
fn input_read_error_is_reported_with_side() {
    fn io_err() -> csv::Error {
        csv::Error::from(io::Error::new(io::ErrorKind::UnexpectedEof, "rdr"))
    }
    let dir = TestDir::new("read-fail");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let left =
        vec![Ok(ByteRecord::from(vec!["a"])), Err(io_err())].into_iter();
    let right = vec![Ok(ByteRecord::from(vec!["a"]))].into_iter();
    let err = MergeJoinBuilder::new()
        .run(left, right, &mut temps, &mut |_| Ok::<_, csv::Error>(()))
        .unwrap_err();
    assert_eq!(err.side(), Some(JoinSide::Left));
    assert!(matches!(err.kind(), MergeJoinErrorKind::Read { .. }));
    assert!(err.cleanup_report().unwrap().is_clean());
}

#[cfg(unix)]
#[test]
fn spill_write_error_is_reported_as_spill_error() {
    use std::os::unix::fs::PermissionsExt;

    // A read-only directory prevents opening the spill file for writing.
    let dir = TestDir::new("spill-write-fail");
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o500))
        .unwrap();
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let left = (0..3)
        .map(|_| Ok(ByteRecord::from(vec!["k"])))
        .collect::<Vec<_>>()
        .into_iter();
    let right = vec![Ok(ByteRecord::from(vec!["k"]))].into_iter();
    let err = MergeJoinBuilder::new()
        .memory_budget(0)
        .run(left, right, &mut temps, &mut |_| Ok::<_, csv::Error>(()))
        .unwrap_err();
    assert!(matches!(err.kind(), MergeJoinErrorKind::Spill { .. }));
    assert_eq!(err.side(), Some(JoinSide::Left));
    // Restore writability so the TestDir destructor can remove everything.
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))
        .unwrap();
    assert!(err.cleanup_report().is_some());
}

#[test]
fn cleanup_interface_is_observable_and_idempotent() {
    let dir = TestDir::new("cleanup-iface");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    assert_eq!(temps.created(), 0);
    assert_eq!(temps.removed(), 0);
    assert_eq!(temps.removal_failures(), 0);
    assert_eq!(temps.live(), 0);

    let path = temps.create(JoinSide::Left);
    fs::write(&path, b"x").unwrap();
    assert_eq!(temps.created(), 1);
    assert_eq!(temps.live(), 1);
    assert!(temps.remove(&path));
    assert_eq!(temps.removed(), 1);
    assert_eq!(temps.live(), 0);
    assert!(temps.cleanup_all().is_clean());
    assert!(dir.files().is_empty());
}

#[test]
fn spill_reader_path_is_inside_caller_directory() {
    let dir = TestDir::new("caller-dir");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let path = temps.create(JoinSide::Right);
    assert!(path.starts_with(dir.path()));
    fs::write(&path, b"a,b\n").unwrap();
    assert!(temps.cleanup_all().is_clean());
}

#[test]
fn only_one_side_spills() {
    // A budget big enough for the small right group but too small for the
    // larger left group: only the left side opens a spill file.
    let left = (0..10)
        .map(|i| format!("k,L{:02}-aaaaaaaaaaaaaaaaaaaaaaaa\n", i))
        .collect::<String>();
    let right = (0..3).map(|i| format!("k,R{:02}\n", i)).collect::<String>();

    let dir = TestDir::new("one-side-spill");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let mut left_rdr = reader(left.as_bytes());
    let mut right_rdr = reader(right.as_bytes());
    let mut lines = Vec::new();
    let status = MergeJoinBuilder::new()
        .memory_budget(512)
        .run(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut |row| {
                let l = String::from_utf8_lossy(row.left().get(1).unwrap())
                    .trim_end_matches(['-', 'a'])
                    .to_string();
                let r = String::from_utf8_lossy(
                    row.right().unwrap().get(1).unwrap(),
                )
                .to_string();
                lines.push(format!("{},{}", l, r));
                Ok::<_, csv::Error>(())
            },
        )
        .unwrap();
    assert_eq!(status.emitted(), 30);
    let mut expected = Vec::new();
    for l in 0..10 {
        for r in 0..3 {
            expected.push(format!("L{:02},R{:02}", l, r));
        }
    }
    assert_eq!(lines, expected);
    // Left spilled; the three-record right group fit in 512 bytes.
    assert_eq!(temps.created(), 1);
    assert_eq!(temps.removed(), 1);
    assert!(status.report().cleanup().is_clean());
}

#[test]
fn single_record_groups_never_spill_even_at_zero_budget() {
    let dir = TestDir::new("zero-budget-singles");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let mut left_rdr = reader(b"a\nb\nc\n");
    let mut right_rdr = reader(b"a\nc\n");
    let mut wtr = csv::WriterBuilder::new().flexible(true).from_writer(vec![]);
    let status = MergeJoinBuilder::new()
        .join_kind(JoinKind::Left)
        .memory_budget(0)
        .run_to_writer(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut wtr,
        )
        .unwrap();
    let data = String::from_utf8(wtr.into_inner().unwrap()).unwrap();
    assert_eq!(data, "a,a\nb,\nc,c\n");
    assert_eq!(status.emitted(), 3);
    assert_eq!(temps.created(), 0, "single-record groups stay in memory");
    assert!(status.report().cleanup().is_clean());
}

#[test]
fn oversized_first_record_stays_in_memory() {
    // The first record of a group always stays buffered, even when it alone
    // exceeds the budget; subsequent records spill.
    let dir = TestDir::new("oversized-first");
    let mut temps = TempFiles::new(dir.path()).unwrap();
    let big = "x".repeat(512);
    let left = format!("k,{}\nk,s\n", big);
    let right = "k,r0\nk,r1\n";

    let mut left_rdr = reader(left.as_bytes());
    let mut right_rdr = reader(right.as_bytes());
    let mut rows: Vec<String> = Vec::new();
    let status = MergeJoinBuilder::new()
        .memory_budget(8)
        .run(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut |row| {
                let l = String::from_utf8_lossy(row.left().get(1).unwrap())
                    .to_string();
                let r = String::from_utf8_lossy(
                    row.right().unwrap().get(1).unwrap(),
                )
                .to_string();
                rows.push(format!("{},{}", l, r));
                Ok::<_, csv::Error>(())
            },
        )
        .unwrap();
    assert_eq!(status.emitted(), 4);
    assert_eq!(
        rows,
        vec![
            format!("{},r0", big),
            format!("{},r1", big),
            "s,r0".to_string(),
            "s,r1".to_string(),
        ]
    );
    // At least one side spilled (both sides exceed the budget past their
    // first record).
    assert!(temps.created() >= 1);
    assert!(status.report().cleanup().is_clean());
    assert!(dir.files().is_empty());
}

#[test]
fn join_row_api_views_and_consumes_records() {
    let left = ByteRecord::from(vec!["k", "lv"]);
    let right = ByteRecord::from(vec!["k", "rv"]);
    let row = csv::merge_join::JoinRow::both(&left, &right);
    assert_eq!(row.left(), &left);
    assert_eq!(row.right(), Some(&right));
    assert_eq!(
        row.to_byte_record(),
        ByteRecord::from(vec!["k", "lv", "k", "rv"])
    );

    let only = csv::merge_join::JoinRow::left_only(left.clone());
    assert!(only.right().is_none());
    assert_eq!(only.to_byte_record(), ByteRecord::from(vec!["k", "lv", ""]));
    assert_eq!(only.into_right(), None);
}
