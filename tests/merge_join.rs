use std::{
    cell::RefCell,
    path::PathBuf,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
};

use csv::{
    ByteRecord, JoinError, JoinKind, JoinSide, MergeJoinBuilder,
    ReaderBuilder, StopReason,
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique temporary directory removed on drop, even on test failure.
struct TestDir(PathBuf);

impl TestDir {
    fn new(test: &str) -> TestDir {
        let n = COUNTER.fetch_add(1, AtomicOrdering::SeqCst);
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "rust-csv-merge-join-{}-{}-{}",
            std::process::id(),
            test,
            n
        ));
        std::fs::create_dir_all(&dir).unwrap();
        TestDir(dir)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn rec(fields: &[&[u8]]) -> ByteRecord {
    let mut r = ByteRecord::new();
    for f in fields {
        r.push_field(f);
    }
    r
}

fn ok_iter(
    v: Vec<ByteRecord>,
) -> impl Iterator<Item = Result<ByteRecord, csv::Error>> {
    v.into_iter().map(Ok)
}

fn record_to_string(r: &ByteRecord) -> String {
    let mut out = Vec::new();
    for (i, f) in r.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(f);
    }
    String::from_utf8(out).unwrap()
}

fn rows_to_strings(
    rows: Vec<(ByteRecord, Option<ByteRecord>)>,
) -> Vec<(String, Option<String>)> {
    rows.into_iter()
        .map(|(l, r)| (record_to_string(&l), r.as_ref().map(record_to_string)))
        .collect()
}

fn run(
    kind: JoinKind,
    left: Vec<ByteRecord>,
    right: Vec<ByteRecord>,
    budget: usize,
) -> (
    Vec<(String, Option<String>)>,
    csv::JoinStatus,
    Rc<RefCell<Vec<PathBuf>>>,
    TestDir,
) {
    let dir = TestDir::new("run");
    let cleaned = Rc::new(RefCell::new(Vec::new()));
    let obs = Rc::clone(&cleaned);
    let mut join = MergeJoinBuilder::new()
        .kind(kind)
        .spill_dir(dir.path())
        .memory_budget(budget)
        .with_cleanup_observer(move |path| {
            obs.borrow_mut().push(path.to_path_buf())
        })
        .build(ok_iter(left), ok_iter(right));
    let rows: Vec<(ByteRecord, Option<ByteRecord>)> =
        join.by_ref().map(|r| r.unwrap()).collect();
    let status = join.finish().unwrap();
    (rows_to_strings(rows), status, cleaned, dir)
}

#[test]
fn inner_join_aa_b_vs_aa_c_in_memory() {
    let left =
        vec![rec(&[b"a", b"l1"]), rec(&[b"a", b"l2"]), rec(&[b"b", b"l3"])];
    let right =
        vec![rec(&[b"a", b"r1"]), rec(&[b"a", b"r2"]), rec(&[b"c", b"r3"])];
    let (rows, status, cleaned, _dir) =
        run(JoinKind::Inner, left, right, 1 << 20);

    assert_eq!(status.rows_emitted(), 4);
    assert_eq!(status.stop_reason(), StopReason::Completed);
    assert_eq!(
        rows,
        vec![
            ("a,l1".to_string(), Some("a,r1".to_string())),
            ("a,l1".to_string(), Some("a,r2".to_string())),
            ("a,l2".to_string(), Some("a,r1".to_string())),
            ("a,l2".to_string(), Some("a,r2".to_string())),
        ]
    );
    // No duplicates => no spill files at all.
    assert!(cleaned.borrow().is_empty());
    assert_eq!(status.cleanup().removed(), 0);
}

#[test]
fn left_join_adds_unmatched_b() {
    let left =
        vec![rec(&[b"a", b"l1"]), rec(&[b"a", b"l2"]), rec(&[b"b", b"l3"])];
    let right =
        vec![rec(&[b"a", b"r1"]), rec(&[b"a", b"r2"]), rec(&[b"c", b"r3"])];
    let (rows, status, _cleaned, _dir) =
        run(JoinKind::Left, left, right, 1 << 20);

    assert_eq!(status.rows_emitted(), 5);
    assert_eq!(
        rows,
        vec![
            ("a,l1".to_string(), Some("a,r1".to_string())),
            ("a,l1".to_string(), Some("a,r2".to_string())),
            ("a,l2".to_string(), Some("a,r1".to_string())),
            ("a,l2".to_string(), Some("a,r2".to_string())),
            ("b,l3".to_string(), None),
        ]
    );
}

#[test]
fn spill_budget_zero_matches_in_memory_results_and_order() {
    let left =
        vec![rec(&[b"a", b"l1"]), rec(&[b"a", b"l2"]), rec(&[b"b", b"l3"])];
    let right =
        vec![rec(&[b"a", b"r1"]), rec(&[b"a", b"r2"]), rec(&[b"c", b"r3"])];
    for kind in [JoinKind::Inner, JoinKind::Left] {
        let (spilled_rows, status, cleaned, dir) =
            run(kind, left.clone(), right.clone(), 0);
        let created = cleaned.borrow().len();
        let (memory_rows, _, _, _dir2) =
            run(kind, left.clone(), right.clone(), 1 << 20);

        assert_eq!(spilled_rows, memory_rows, "kind {:?}", kind);
        // Both "a" groups (2 records each) exceed a zero budget.
        assert!(created >= 2, "expected forced spills, got {}", created);
        assert_eq!(status.cleanup().removed(), created);
        assert!(status.cleanup().failures().is_empty());
        // Nothing left behind.
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }
}

#[test]
fn repeated_large_groups_full_cartesian_product() {
    // L=3, R=4 for key k: expect exactly 12 rows in stable order.
    let mut left = Vec::new();
    for i in 0..3 {
        left.push(rec(&[b"k", format!("l{}", i).as_bytes()]));
    }
    let mut right = Vec::new();
    for i in 0..4 {
        right.push(rec(&[b"k", format!("r{}", i).as_bytes()]));
    }
    let (rows, status, _cleaned, _dir) = run(JoinKind::Inner, left, right, 0);
    assert_eq!(status.rows_emitted(), 12);
    let expected: Vec<(String, Option<String>)> = (0..3)
        .flat_map(|li| {
            (0..4).map(move |ri| {
                (format!("k,l{}", li), Some(format!("k,r{}", ri)))
            })
        })
        .collect();
    assert_eq!(rows, expected);
}

#[test]
fn many_group_sizes_match_naive_join() {
    // Cross-check many group-size combinations and all budget levels
    // against a trivial nested-loop join.
    let keys = ["a", "b", "c", "d"];
    let l_counts = [1usize, 2, 1, 4];
    let r_counts = [3usize, 1, 2, 4];
    let mut left = Vec::new();
    for (k, n) in keys.iter().zip(&l_counts) {
        for i in 0..*n {
            left.push(rec(&[k.as_bytes(), format!("L{}", i).as_bytes()]));
        }
    }
    let mut right = Vec::new();
    for (k, n) in keys.iter().zip(&r_counts) {
        for i in 0..*n {
            right.push(rec(&[k.as_bytes(), format!("R{}", i).as_bytes()]));
        }
    }

    let mut naive = Vec::new();
    for l in &left {
        let mut matched = false;
        for r in &right {
            if l.get(0) == r.get(0) {
                naive.push((l, Some(r.clone())));
                matched = true;
            }
        }
        if !matched {
            naive.push((l, None));
        }
    }

    for budget in [0usize, 1, 4, 64, 1 << 20] {
        let dir = TestDir::new("sizes");
        let mut join = MergeJoinBuilder::new()
            .kind(JoinKind::Left)
            .spill_dir(dir.path())
            .memory_budget(budget)
            .build(
                left.clone().into_iter().map(Ok),
                right.clone().into_iter().map(Ok),
            );
        let got: Vec<(ByteRecord, Option<ByteRecord>)> =
            join.by_ref().map(|r| r.unwrap()).collect();
        join.finish().unwrap();
        assert_eq!(got.len(), naive.len(), "budget {}", budget);
        for (g, n) in got.iter().zip(&naive) {
            assert_eq!(&g.0, n.0);
            assert_eq!(g.1.as_ref(), n.1.as_ref());
        }
    }
}

#[test]
fn non_utf8_keys_and_fields_round_trip_through_spill() {
    let bad: &[u8] = &[0xFF, 0xFE, b'x'];
    let low: &[u8] = &[0x00, 0x80];
    let left =
        vec![rec(&[low, b"l3"]), rec(&[bad, b"l1"]), rec(&[bad, b"l2"])];
    let right = vec![rec(&[b"z", b"r"]), rec(&[bad, &[0xC3, 0xA9]])];
    let dir = TestDir::new("utf8");
    let mut join = MergeJoinBuilder::new()
        .kind(JoinKind::Left)
        .spill_dir(dir.path())
        .memory_budget(0)
        .build(ok_iter(left), ok_iter(right));
    let rows: Vec<(ByteRecord, Option<ByteRecord>)> =
        join.by_ref().map(|r| r.unwrap()).collect();
    let status = join.finish().unwrap();

    assert_eq!(status.rows_emitted(), 3);
    // The low, unmatched key sorts first in byte order.
    assert!(rows[0].1.is_none());
    assert_eq!(rows[0].0.get(0), Some(low));
    assert_eq!(rows[0].0.get(1), Some(&b"l3"[..]));
    // Then the invalid-UTF-8 duplicate group with its cross product.
    assert_eq!(rows[1].0.get(0), Some(bad));
    assert_eq!(rows[1].0.get(1), Some(&b"l1"[..]));
    assert_eq!(rows[1].1.as_ref().unwrap().get(0), Some(bad));
    assert_eq!(rows[1].1.as_ref().unwrap().get(1), Some(&[0xC3, 0xA9][..]));
    assert_eq!(rows[2].0.get(1), Some(&b"l2"[..]));
    assert_eq!(rows[2].1.as_ref().unwrap().get(1), Some(&[0xC3, 0xA9][..]));
}

#[test]
fn out_of_order_left_reports_side_and_position() {
    let left = vec![
        rec(&[b"a", b"l1"]),
        rec(&[b"a", b"lDup"]), // duplicate: spills under budget 0
        rec(&[b"c", b"l2"]),
        rec(&[b"b", b"l3"]), // out of order
    ];
    let right = vec![rec(&[b"a", b"r1"]), rec(&[b"z", b"r2"])];
    let dir = TestDir::new("ooo-l");
    let cleaned = Rc::new(RefCell::new(0usize));
    let obs = Rc::clone(&cleaned);
    let mut join = MergeJoinBuilder::new()
        .kind(JoinKind::Left)
        .spill_dir(dir.path())
        .memory_budget(0)
        .with_cleanup_observer(move |_p| *obs.borrow_mut() += 1)
        .build(ok_iter(left), ok_iter(right));
    let mut rows = 0;
    let err = loop {
        match join.next_row() {
            Ok(Some(_)) => rows += 1,
            Ok(None) => panic!("expected out-of-order error"),
            Err(err) => break err,
        }
    };
    match err {
        JoinError::OutOfOrder { side, record_index, position: _ } => {
            assert_eq!(side, JoinSide::Left);
            assert_eq!(record_index, 3);
        }
        other => panic!("expected OutOfOrder, got {:?}", other),
    }
    // Further calls report that the join is consumed...
    assert!(matches!(join.next_row(), Err(JoinError::Consumed)));
    // ...spill files were reclaimed...
    assert!(*cleaned.borrow() >= 1);
    // ...and finish surfaces the failure.
    assert!(matches!(join.finish(), Err(JoinError::Consumed)));
    let _ = rows;
}

#[test]
fn out_of_order_right_with_reader_positions() {
    let ldata = "a,l\nz,l\n";
    let rdata = "a,r1\nm,r2\nb,r3\n"; // m then b: out of order
    let mut lrdr =
        ReaderBuilder::new().has_headers(false).from_reader(ldata.as_bytes());
    let mut rrdr =
        ReaderBuilder::new().has_headers(false).from_reader(rdata.as_bytes());
    let dir = TestDir::new("ooo-r");
    let mut join = MergeJoinBuilder::new()
        .kind(JoinKind::Inner)
        .spill_dir(dir.path())
        .memory_budget(0)
        .build(lrdr.byte_records(), rrdr.byte_records());
    let err = join.by_ref().find_map(|r| r.err()).expect("an error");
    match err {
        JoinError::OutOfOrder { side, record_index, position } => {
            assert_eq!(side, JoinSide::Right);
            assert_eq!(record_index, 2);
            let pos = position.expect("reader position");
            assert_eq!(pos.record(), 2);
        }
        other => panic!("expected OutOfOrder, got {:?}", other),
    }
    assert!(matches!(join.next_row(), Err(JoinError::Consumed)));
}

#[test]
fn out_of_order_on_right_does_not_pretend_results_complete() {
    // Left has keys a..e; right is sorted until a late inversion. Rows
    // produced before the error must only cover keys strictly below the
    // inversion point.
    let left: Vec<ByteRecord> = ["a", "b", "c", "d", "e"]
        .iter()
        .map(|k| rec(&[k.as_bytes(), b"L"]))
        .collect();
    let right: Vec<ByteRecord> = ["a", "b", "c", "a"]
        .iter()
        .map(|k| rec(&[k.as_bytes(), b"R"]))
        .collect();
    let dir = TestDir::new("ooo-partial");
    let mut join = MergeJoinBuilder::new()
        .kind(JoinKind::Inner)
        .spill_dir(dir.path())
        .memory_budget(0)
        .build(ok_iter(left), ok_iter(right));
    let mut seen = Vec::new();
    let err = loop {
        match join.next_row() {
            Ok(Some((l, _))) => seen.push(l.get(0).unwrap().to_vec()),
            Ok(None) => panic!("expected error"),
            Err(e) => break e,
        }
    };
    assert!(matches!(err, JoinError::OutOfOrder { .. }));
    // Only a, b and c rows could have been emitted before detecting the
    // inverted trailing "a"; e is never emitted as a match.
    assert!(seen.iter().all(|k| k == b"a" || k == b"b" || k == b"c"));
    assert!(!seen.iter().any(|k| k == b"e"));
}

#[test]
fn missing_key_column_is_an_error() {
    let left = vec![rec(&[b"a"]), rec(&[b"b"])];
    let right = vec![rec(&[b"a", b"r"])];
    let dir = TestDir::new("missing-key");
    let mut join = MergeJoinBuilder::new()
        .key(1)
        .spill_dir(dir.path())
        .build(ok_iter(left), ok_iter(right));
    let err = join.by_ref().find_map(|r| r.err()).expect("an error");
    match err {
        JoinError::MissingKey { side, record_index, .. } => {
            assert_eq!(side, JoinSide::Left);
            assert_eq!(record_index, 0);
        }
        other => panic!("expected MissingKey, got {:?}", other),
    }
}

#[test]
fn max_outputs_stops_early_with_clear_status_and_cleanup() {
    // 4x4 duplicate group => 16 rows; stop at 5.
    let left: Vec<ByteRecord> =
        (0..4).map(|i| rec(&[b"k", format!("l{}", i).as_bytes()])).collect();
    let right: Vec<ByteRecord> =
        (0..4).map(|i| rec(&[b"k", format!("r{}", i).as_bytes()])).collect();
    let dir = TestDir::new("limit");
    let cleaned = Rc::new(RefCell::new(Vec::new()));
    let obs = Rc::clone(&cleaned);
    let mut join = MergeJoinBuilder::new()
        .kind(JoinKind::Inner)
        .spill_dir(dir.path())
        .memory_budget(0)
        .max_outputs(5)
        .with_cleanup_observer(move |p| obs.borrow_mut().push(p.to_path_buf()))
        .build(ok_iter(left), ok_iter(right));
    let rows: Vec<_> = join.by_ref().map(|r| r.unwrap()).collect();
    assert_eq!(rows.len(), 5);
    // Output is the stable prefix: first two left records.
    assert_eq!(rows[0].0.get(1), Some(&b"l0"[..]));
    assert_eq!(rows[4].0.get(1), Some(&b"l1"[..]));
    // Iterator now ends without error.
    assert!(join.next_row().unwrap().is_none());
    let status = join.finish().unwrap();
    assert_eq!(status.rows_emitted(), 5);
    assert_eq!(status.stop_reason(), StopReason::LimitReached);
    assert!(status.stopped_at_limit());
    // Every spill file created was observed and removed.
    assert!(!cleaned.borrow().is_empty());
    assert_eq!(status.cleanup().removed(), cleaned.borrow().len());
    for p in cleaned.borrow().iter() {
        assert!(!p.exists(), "spill file left behind: {:?}", p);
    }
}

#[test]
fn limit_zero_emits_nothing() {
    let left = vec![rec(&[b"a", b"l"])];
    let right = vec![rec(&[b"a", b"r"])];
    let dir = TestDir::new("limit0");
    let mut join = MergeJoinBuilder::new()
        .max_outputs(0)
        .spill_dir(dir.path())
        .build(ok_iter(left), ok_iter(right));
    assert!(join.next_row().unwrap().is_none());
    let status = join.finish().unwrap();
    assert_eq!(status.rows_emitted(), 0);
    assert_eq!(status.stop_reason(), StopReason::LimitReached);
}

#[test]
fn write_failure_when_spill_dir_is_unwritable_is_reported_and_cleaned() {
    // A path that exists as a file cannot be used as a directory; a
    // duplicate key then fails when opening the spill file.
    let base = TestDir::new("writefail");
    let file_path = base.path().join("not-a-dir");
    std::fs::write(&file_path, b"x").unwrap();
    let left = vec![rec(&[b"a", b"l1"]), rec(&[b"a", b"l2"])];
    let right = vec![rec(&[b"a", b"r1"]), rec(&[b"a", b"r2"])];
    let observed = Rc::new(RefCell::new(0usize));
    let obs = Rc::clone(&observed);
    let mut join = MergeJoinBuilder::new()
        .kind(JoinKind::Inner)
        .spill_dir(&file_path)
        .memory_budget(0)
        .with_cleanup_observer(move |_| *obs.borrow_mut() += 1)
        .build(ok_iter(left), ok_iter(right));
    let err = join.by_ref().find_map(|r| r.err()).expect("write failure");
    assert!(matches!(err, JoinError::Io(_)), "got {:?}", err);
    // No spill entries can have been created, and no rows.
    assert_eq!(*observed.borrow(), 0);
    assert!(matches!(join.finish(), Err(JoinError::Consumed)));
}

#[test]
fn input_read_error_is_reported_as_input_error() {
    // A failing reader iterator mapped to csv::Error.
    struct FailReader(bool);
    impl Iterator for FailReader {
        type Item = Result<ByteRecord, csv::Error>;
        fn next(&mut self) -> Option<Self::Item> {
            if !self.0 {
                self.0 = true;
                Some(Ok(rec(&[b"a", b"l1"])))
            } else {
                Some(Err(csv::Error::from(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "boom",
                ))))
            }
        }
    }
    let right = vec![rec(&[b"a", b"r1"])];
    let dir = TestDir::new("inputerr");
    let mut join = MergeJoinBuilder::new()
        .spill_dir(dir.path())
        .memory_budget(0)
        .build(FailReader(false), right.into_iter().map(Ok));
    let err = join.by_ref().find_map(|r| r.err()).expect("an error");
    assert!(matches!(err, JoinError::Input(_)), "got {:?}", err);
    assert!(format!("{}", err).contains("boom"));
}

#[test]
fn cleanup_on_drop_without_finish_reclaims_files() {
    let left: Vec<ByteRecord> =
        (0..3).map(|i| rec(&[b"k", format!("l{}", i).as_bytes()])).collect();
    let right: Vec<ByteRecord> =
        (0..3).map(|i| rec(&[b"k", format!("r{}", i).as_bytes()])).collect();
    let dir = TestDir::new("drop");
    let created = Rc::new(RefCell::new(Vec::new()));
    let obs = Rc::clone(&created);
    {
        let mut join = MergeJoinBuilder::new()
            .spill_dir(dir.path())
            .memory_budget(0)
            .with_cleanup_observer(move |p| {
                obs.borrow_mut().push(p.to_path_buf())
            })
            .build(ok_iter(left), ok_iter(right));
        // Pull only one row, then drop without finish().
        let _ = join.next_row().unwrap();
        let pending = join.spill_files();
        assert!(!pending.is_empty());
        for p in &pending {
            assert!(p.exists(), "spill file should exist mid-join: {:?}", p);
        }
        drop(join);
        for p in &pending {
            assert!(!p.exists(), "drop leaked {:?}", p);
        }
        assert_eq!(created.borrow().len(), pending.len());
    }
    for p in created.borrow().iter() {
        assert!(!p.exists(), "drop leaked {:?}", p);
    }
}

#[test]
fn different_key_columns_join_correctly() {
    // Left key in column 1, right key in column 0.
    let left =
        vec![rec(&[b"x", b"a"]), rec(&[b"y", b"a"]), rec(&[b"z", b"b"])];
    let right = vec![rec(&[b"a", b"r"])];
    let dir = TestDir::new("cols");
    let mut join = MergeJoinBuilder::new()
        .left_key(1)
        .right_key(0)
        .kind(JoinKind::Inner)
        .spill_dir(dir.path())
        .build(ok_iter(left), ok_iter(right));
    let rows: Vec<_> = join.by_ref().map(|r| r.unwrap()).collect();
    join.finish().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, vec![b"x".to_vec(), b"a".to_vec()]);
    assert_eq!(rows[1].0, vec![b"y".to_vec(), b"a".to_vec()]);
}

#[test]
fn finish_without_iterating_is_ok_and_cleans_up() {
    // Building and immediately finishing must not create or leak files.
    let dir = TestDir::new("noiter");
    let left = vec![rec(&[b"a", b"l"])];
    let right = vec![rec(&[b"a", b"r"])];
    let join = MergeJoinBuilder::new()
        .spill_dir(dir.path())
        .memory_budget(0)
        .build(ok_iter(left), ok_iter(right));
    let status = join.finish().unwrap();
    assert_eq!(status.rows_emitted(), 0);
    assert_eq!(status.stop_reason(), StopReason::Completed);
    assert_eq!(status.cleanup().removed(), 0);
    assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
}

#[test]
fn duplicate_budget_boundary_emits_in_stable_order() {
    // Exactly two records: budget sized so the second record triggers a
    // spill (reservation overhead pushes the pair over the budget).
    let left = vec![rec(&[b"k", b"l1"]), rec(&[b"k", b"l2"])];
    let right = vec![rec(&[b"k", b"r1"]), rec(&[b"k", b"r2"])];
    let dir = TestDir::new("boundary");
    let cleaned = Rc::new(RefCell::new(0usize));
    let obs = Rc::clone(&cleaned);
    let mut join = MergeJoinBuilder::new()
        .spill_dir(dir.path())
        .memory_budget(1)
        .with_cleanup_observer(move |_| *obs.borrow_mut() += 1)
        .build(ok_iter(left), ok_iter(right));
    let rows: Vec<_> = join.by_ref().map(|r| r.unwrap()).collect();
    let status = join.finish().unwrap();
    assert_eq!(
        rows.iter()
            .map(|(l, r)| (
                record_to_string(l),
                r.as_ref().map(record_to_string)
            ))
            .collect::<Vec<_>>(),
        vec![
            ("k,l1".to_string(), Some("k,r1".to_string())),
            ("k,l1".to_string(), Some("k,r2".to_string())),
            ("k,l2".to_string(), Some("k,r1".to_string())),
            ("k,l2".to_string(), Some("k,r2".to_string())),
        ]
    );
    assert_eq!(status.cleanup().removed(), *cleaned.borrow());
}
