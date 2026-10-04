//! Integration tests for the exact fixed-precision `csv::amount` module.

use csv::amount::{
    AggregateErrorKind, AmountAggregator, AmountConfig, KeyNormalization,
    MergeErrorKind,
};
use csv::{ReaderBuilder, StringRecord};

fn record(fields: &[&str]) -> StringRecord {
    StringRecord::from(fields)
}

fn feed(agg: &mut AmountAggregator, rows: &[&[&str]]) {
    for row in rows {
        agg.add_record(&record(row)).unwrap();
    }
}

fn names(agg: &AmountAggregator) -> Vec<String> {
    agg.entries().map(|e| e.key().to_string()).collect()
}

// The headline acceptance scenario: two chunks
// a,0.10/a,0.20 and a,0.30/b,1.00 merge to an exact total of 0.60 and
// count 3 for `a`.
#[test]
fn acceptance_two_chunks_merge_exactly() {
    let mut left = AmountAggregator::new(AmountConfig::new(0, 1, 2)).unwrap();
    feed(&mut left, &[&["a", "0.10"], &["a", "0.20"]]);

    let mut right = AmountAggregator::new(AmountConfig::new(0, 1, 2)).unwrap();
    feed(&mut right, &[&["a", "0.30"], &["b", "1.00"]]);

    left.merge(&right).unwrap();

    let a = left.get("a").unwrap();
    assert_eq!(a.count(), 3);
    assert_eq!(a.sum(), 60);
    assert_eq!(AmountAggregator::format_amount(a.sum(), 2), "0.60");

    let b = left.get("b").unwrap();
    assert_eq!(b.count(), 1);
    assert_eq!(b.sum(), 100);

    // Stable first-valid-appearance ordering across the merge.
    assert_eq!(names(&left), vec!["a".to_string(), "b".to_string()]);
}

// a,0.001 must be rejected at precision 2 and the previous total survives.
#[test]
fn acceptance_excess_precision_is_rejected_without_state_change() {
    let mut agg = AmountAggregator::new(AmountConfig::new(0, 1, 2)).unwrap();
    agg.add_record(&record(&["a", "0.10"])).unwrap();

    let err = agg.add_record(&record(&["a", "0.001"])).unwrap_err();
    assert_eq!(err.record(), 2);
    assert!(matches!(
        err.kind(),
        AggregateErrorKind::ExcessPrecision { precision, .. } if *precision == 2
    ));

    let a = agg.get("a").unwrap();
    assert_eq!(a.count(), 1);
    assert_eq!(a.sum(), 10);
    assert_eq!(names(&agg), vec!["a".to_string()]);
}

// The rejection also works when the fine-grained value arrives through a
// real csv::Reader, without changing Reader semantics.
#[test]
fn acceptance_excess_precision_through_reader() {
    let data = "a,0.10\na,0.001\n";
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(data.as_bytes());
    let mut agg = AmountAggregator::new(AmountConfig::new(0, 1, 2)).unwrap();
    let mut rejected = None;
    for (i, rec) in rdr.records().enumerate() {
        let rec = rec.unwrap();
        if let Err(err) = agg.add_record(&rec) {
            rejected = Some((i + 1, err));
        }
    }
    let (line, err) = rejected.expect("the 0.001 row must be rejected");
    assert_eq!(line, 2);
    assert!(matches!(err.kind(), AggregateErrorKind::ExcessPrecision { .. }));
    assert_eq!(agg.get("a").unwrap().sum(), 10);
    assert_eq!(agg.get("a").unwrap().count(), 1);
}

// Any single invalid field rejects the whole row: count and sum stay put and
// the record counter still advances.
#[test]
fn failed_rows_never_change_state_and_record_numbers_advance() {
    let mut agg = AmountAggregator::new(AmountConfig::new(0, 1, 2)).unwrap();
    agg.add_record(&record(&["a", "0.10"])).unwrap();

    let empty_key = agg.add_record(&record(&["", "0.10"])).unwrap_err();
    assert_eq!(empty_key.record(), 2);
    assert!(matches!(empty_key.kind(), AggregateErrorKind::EmptyKey));

    let bad_amount = agg.add_record(&record(&["a", "x"])).unwrap_err();
    assert_eq!(bad_amount.record(), 3);
    assert!(matches!(
        bad_amount.kind(),
        AggregateErrorKind::InvalidAmount(raw) if raw == "x"
    ));

    let missing = agg.add_record(&record(&["a"])).unwrap_err();
    assert_eq!(missing.record(), 4);
    assert!(matches!(
        missing.kind(),
        AggregateErrorKind::MissingColumn { column, fields }
            if *column == 1 && *fields == 1
    ));

    // Next valid row is record 5 and folds normally.
    agg.add_record(&record(&["a", "0.05"])).unwrap();
    assert_eq!(agg.record_number(), 6);
    let a = agg.get("a").unwrap();
    assert_eq!(a.count(), 2);
    assert_eq!(a.sum(), 15);
    assert_eq!(names(&agg), vec!["a".to_string()]);
}

#[test]
fn negative_amounts_are_exact_and_can_cancel() {
    let mut agg = AmountAggregator::new(AmountConfig::new(0, 1, 2)).unwrap();
    feed(&mut agg, &[&["a", "1.00"], &["a", "-0.30"], &["a", "-0.70"]]);
    let a = agg.get("a").unwrap();
    assert_eq!(a.count(), 3);
    assert_eq!(a.sum(), 0);
    assert_eq!(AmountAggregator::format_amount(a.sum(), 2), "0.00");

    let mut neg = AmountAggregator::new(AmountConfig::new(0, 1, 4)).unwrap();
    neg.add_record(&record(&["k", "-0.0001"])).unwrap();
    assert_eq!(neg.get("k").unwrap().sum(), -1);
}

#[test]
fn key_capacity_boundary_is_enforced_precisely() {
    let mut agg =
        AmountAggregator::new(AmountConfig::new(0, 1, 2).max_keys(2)).unwrap();
    agg.add_record(&record(&["a", "1.00"])).unwrap();
    agg.add_record(&record(&["b", "1.00"])).unwrap();

    // Capacity reached: a brand new key is rejected...
    let err = agg.add_record(&record(&["c", "1.00"])).unwrap_err();
    assert_eq!(err.record(), 3);
    assert!(matches!(
        err.kind(),
        AggregateErrorKind::KeyCapacityExceeded(k) if k == "c"
    ));
    assert_eq!(agg.len(), 2);

    // ...but rows for existing keys still fold in.
    agg.add_record(&record(&["a", "0.50"])).unwrap();
    assert_eq!(agg.get("a").unwrap().count(), 2);
    assert_eq!(agg.get("a").unwrap().sum(), 150);
}

#[test]
fn key_order_tracks_first_valid_appearance_only() {
    let mut agg = AmountAggregator::new(AmountConfig::new(0, 1, 2)).unwrap();
    // Rejected rows never reserve a slot or a position.
    let _ = agg.add_record(&record(&["z", "nope"]));
    agg.add_record(&record(&["b", "1.00"])).unwrap();
    agg.add_record(&record(&["a", "1.00"])).unwrap();
    let _ = agg.add_record(&record(&["c", "0.005"]));
    agg.add_record(&record(&["b", "1.00"])).unwrap();
    assert_eq!(names(&agg), vec!["b".to_string(), "a".to_string()]);
}

#[test]
fn ascii_lowercase_key_strategy_folds_keys() {
    let config = AmountConfig::new(0, 1, 2)
        .key_normalization(KeyNormalization::AsciiLowercase);
    let mut agg = AmountAggregator::new(config).unwrap();
    agg.add_record(&record(&["A", "1.00"])).unwrap();
    agg.add_record(&record(&["a", "1.00"])).unwrap();
    assert_eq!(agg.len(), 1);
    assert_eq!(agg.get("A").unwrap().sum(), 200);
}

#[test]
fn per_record_sum_overflow_leaves_state_untouched() {
    let mut agg = AmountAggregator::new(AmountConfig::new(0, 1, 2)).unwrap();
    agg.add_record(&record(&["a", "92233720368547758.07"])).unwrap();
    let before = agg.get("a").unwrap().sum();
    assert_eq!(before, i64::MAX);

    let err = agg.add_record(&record(&["a", "0.01"])).unwrap_err();
    assert_eq!(err.record(), 2);
    assert!(matches!(err.kind(), AggregateErrorKind::Overflow(_)));

    let after = agg.get("a").unwrap();
    assert_eq!(after.sum(), i64::MAX);
    assert_eq!(after.count(), 1);
}

#[test]
fn oversized_amount_text_is_an_overflow_error() {
    let mut agg = AmountAggregator::new(AmountConfig::new(0, 1, 2)).unwrap();
    let err = agg
        .add_record(&record(&["a", "99999999999999999999.00"]))
        .unwrap_err();
    assert!(matches!(err.kind(), AggregateErrorKind::Overflow(_)));
    assert!(agg.is_empty());
}

fn chunk(rows: &[&[&str]], precision: u8) -> AmountAggregator {
    let mut agg =
        AmountAggregator::new(AmountConfig::new(0, 1, precision)).unwrap();
    feed(&mut agg, rows);
    agg
}

#[test]
fn merge_requires_matching_precision_and_key_strategy() {
    let p2 = chunk(&[&["a", "1.00"]], 2);
    let p3 = chunk(&[&["a", "1.000"]], 3);
    let mut target = p2.clone();

    let err = target.merge(&p3).unwrap_err();
    assert!(matches!(
        err.kind(),
        MergeErrorKind::PrecisionMismatch { target: 2, other: 3 }
    ));
    // Target unchanged on conflict.
    assert_eq!(target.get("a").unwrap().sum(), 100);

    let lower = AmountAggregator::new(
        AmountConfig::new(0, 1, 2)
            .key_normalization(KeyNormalization::AsciiLowercase),
    )
    .unwrap();
    let err = target.merge(&lower).unwrap_err();
    assert!(matches!(err.kind(), MergeErrorKind::KeyNormalizationMismatch));
    assert_eq!(target.get("a").unwrap().sum(), 100);
}

#[test]
fn merge_overflow_is_atomic() {
    let mut target =
        chunk(&[&["a", "92233720368547758.07"], &["c", "1.00"]], 2);
    let snapshot = (
        target.get("a").unwrap().sum(),
        target.get("c").unwrap().sum(),
        names(&target),
    );
    // `b` merges cleanly but `a` overflows: the whole merge must roll back.
    let other = chunk(&[&["b", "1.00"], &["a", "0.01"]], 2);
    let err = target.merge(&other).unwrap_err();
    assert!(matches!(err.kind(), MergeErrorKind::SumOverflow(k) if k == "a"));

    assert_eq!(target.get("a").unwrap().sum(), snapshot.0);
    assert_eq!(target.get("a").unwrap().count(), 1);
    assert_eq!(target.get("c").unwrap().sum(), snapshot.1);
    assert!(target.get("b").is_none());
    assert_eq!(names(&target), snapshot.2);
}

#[test]
fn merge_respects_target_key_capacity_atomically() {
    let mut target =
        AmountAggregator::new(AmountConfig::new(0, 1, 2).max_keys(2)).unwrap();
    feed(&mut target, &[&["a", "1.00"], &["b", "1.00"]]);

    // d is new beyond the cap; c appearing earlier must not be committed.
    let other = chunk(&[&["c", "1.00"], &["d", "1.00"]], 2);
    let err = target.merge(&other).unwrap_err();
    assert!(matches!(
        err.kind(),
        MergeErrorKind::KeyCapacityExceeded(k) if k == "c"
    ));
    assert_eq!(names(&target), vec!["a".to_string(), "b".to_string()]);

    // A merge that only updates existing keys is still allowed at capacity.
    let fits = chunk(&[&["b", "0.50"]], 2);
    target.merge(&fits).unwrap();
    assert_eq!(target.get("b").unwrap().sum(), 150);
    assert_eq!(target.len(), 2);
}

#[test]
fn merge_appends_new_keys_in_other_first_seen_order() {
    let mut target = chunk(&[&["a", "1.00"]], 2);
    let other = chunk(&[&["c", "1.00"], &["b", "1.00"]], 2);
    target.merge(&other).unwrap();
    assert_eq!(
        names(&target),
        vec!["a".to_string(), "c".to_string(), "b".to_string()]
    );
}

// End-to-end: two CSV chunks read with an unmodified csv::Reader, headers
// skipped, are summarized independently and then merged.
#[test]
fn end_to_end_reader_chunks_then_merge() {
    let csv_one = "key,amount\na,0.10\na,0.20\n";
    let csv_two = "key,amount\na,0.30\nb,1.00\n";

    let summarize = |data: &str| {
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(data.as_bytes());
        let mut agg =
            AmountAggregator::new(AmountConfig::new(0, 1, 2)).unwrap();
        for rec in rdr.records() {
            agg.add_record(&rec.unwrap()).unwrap();
        }
        agg
    };

    let mut total = summarize(csv_one);
    total.merge(&summarize(csv_two)).unwrap();

    assert_eq!(total.get("a").unwrap().count(), 3);
    assert_eq!(total.get("a").unwrap().sum(), 60);
    assert_eq!(
        AmountAggregator::format_amount(total.get("a").unwrap().sum(), 2),
        "0.60"
    );
    assert_eq!(total.get("b").unwrap().count(), 1);
    assert_eq!(
        AmountAggregator::format_amount(total.get("b").unwrap().sum(), 2),
        "1.00"
    );
}
