//! Small demo for the exact fixed-precision `csv::amount` aggregator.
//!
//! Run it from the crate root with (all caches/target dirs can be pointed at
//! any scratch location):
//!
//! ```ignore
//! cargo run --example amount-aggregation
//! ```
//!
//! It reads two independent CSV chunks, summarizes each one without any
//! floating point, merges the two summaries atomically, and finally shows
//! that an over-precise amount (`0.001` at 2-digit precision) is rejected
//! without changing the accumulated totals.

use csv::amount::{AggregateErrorKind, AmountAggregator, AmountConfig};
use csv::ReaderBuilder;

const PRECISION: u8 = 2;

fn summarize(path: &str) -> AmountAggregator {
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .expect("open chunk csv");
    let mut agg =
        AmountAggregator::new(AmountConfig::new(0, 1, PRECISION)).unwrap();
    for result in rdr.records() {
        let record = result.expect("parse csv record");
        if let Err(err) = agg.add_record(&record) {
            println!("  row {} rejected: {}", err.record(), err);
        }
    }
    agg
}

fn print_state(title: &str, agg: &AmountAggregator) {
    println!("{title}");
    println!("  state entries in first-valid-appearance order:");
    for entry in agg.entries() {
        println!(
            "    key={:<4} count={:<2} sum={} minor units (formatted: {})",
            entry.key(),
            entry.count(),
            entry.sum(),
            AmountAggregator::format_amount(entry.sum(), PRECISION),
        );
    }
}

fn main() {
    println!("== summarizing two chunks independently ==");
    let mut total = summarize("examples/data/amount_chunk1.csv");
    print_state("chunk 1 (a,0.10 / a,0.20):", &total);
    let chunk2 = summarize("examples/data/amount_chunk2.csv");
    print_state("chunk 2 (a,0.30 / b,1.00):", &chunk2);

    println!("== merging chunk 2 into chunk 1 ==");
    total.merge(&chunk2).expect("same precision and key policy");
    print_state("merged state:", &total);
    let a = total.get("a").unwrap();
    println!(
        "a => count = {}, sum = {} (exact decimal {})",
        a.count(),
        a.sum(),
        AmountAggregator::format_amount(a.sum(), PRECISION),
    );

    println!("== over-precise row is rejected and the state survives ==");
    let before = (a.count(), a.sum());
    let bad = csv::StringRecord::from(vec!["a", "0.001"]);
    match total.add_record(&bad) {
        Ok(()) => panic!("0.001 must be rejected at precision 2"),
        Err(err) => {
            assert!(matches!(
                err.kind(),
                AggregateErrorKind::ExcessPrecision { .. }
            ));
            println!(
                "  row {} rejected ({}): {}",
                err.record(),
                match err.kind() {
                    AggregateErrorKind::ExcessPrecision { .. } =>
                        "excess precision",
                    _ => "other",
                },
                err,
            );
        }
    }
    let after = total.get("a").unwrap();
    assert_eq!((after.count(), after.sum()), before);
    println!(
        "a is unchanged: count = {}, sum = {} ({})",
        after.count(),
        after.sum(),
        AmountAggregator::format_amount(after.sum(), PRECISION),
    );
}
