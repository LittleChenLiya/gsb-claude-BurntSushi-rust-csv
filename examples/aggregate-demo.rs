//! Small CSV demo for the `aggregate` module.
//!
//! Reads two chunks of CSV data (no headers, `key,amount` rows), aggregates
//! amounts by business key with 2-decimal precision, merges the two chunks
//! and prints the resulting state.
//!
//! Run with:
//!
//! ```text
//! cargo run --example aggregate-demo
//! ```

use std::error::Error;

use csv::aggregate::{Aggregator, AggregateConfig, KeyStrategy};
use csv::ReaderBuilder;

fn main() -> Result<(), Box<dyn Error>> {
    // Two chunks of CSV data. Each row is `key,amount`. Amounts are converted
    // to integer units with 2-decimal precision (no floating point).
    let chunk_a = "a,0.10\na,0.20\n";
    let chunk_b = "a,0.30\nb,1.00\n";

    let config = AggregateConfig {
        precision: 2,
        capacity: 16,
        key_strategy: KeyStrategy::Exact,
        ..AggregateConfig::default()
    };

    // Aggregate each chunk independently.
    let mut rdr_a = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(chunk_a.as_bytes());
    let mut agg_a = Aggregator::new(config.clone());
    agg_a.extend(&mut rdr_a)?;

    let mut rdr_b = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(chunk_b.as_bytes());
    let mut agg_b = Aggregator::new(config.clone());
    agg_b.extend(&mut rdr_b)?;

    println!("== chunk a ==");
    println!("{}", agg_a);
    println!("== chunk b ==");
    println!("{}", agg_b);

    // Merge chunk b into chunk a. Both have the same precision and key
    // strategy, so the merge succeeds atomically.
    agg_a.merge(&agg_b)?;

    println!("== merged (a + b) ==");
    println!("{}", agg_a);

    // Write the merged state as CSV (key,sum,count). The sum column is in
    // integer units (e.g. 60 means 0.60 with precision 2).
    let mut wtr = csv::Writer::from_writer(Vec::new());
    agg_a.write_csv(&mut wtr)?;
    let csv_bytes = wtr.into_inner()?;
    println!("== merged CSV ==");
    println!("{}", String::from_utf8(csv_bytes)?);

    // Demonstrate that an over-precision amount is rejected and the state is
    // unchanged.
    let before = (agg_a.total_sum(), agg_a.total_count());
    let mut rdr_bad = ReaderBuilder::new()
        .has_headers(false)
        .from_reader("a,0.001\n".as_bytes());
    let mut agg_bad = Aggregator::new(config);
    // The bad row is rejected during extend (over-precision at precision 2).
    let extend_err = agg_bad.extend(&mut rdr_bad).unwrap_err();
    println!("== over-precision extend error ==");
    println!("{}", extend_err);
    // Merge the (empty) bad chunk: target state is unchanged.
    agg_a.merge(&agg_bad)?;
    assert_eq!((agg_a.total_sum(), agg_a.total_count()), before);
    println!(
        "state unchanged: total_sum={}, total_count={}",
        agg_a.total_sum(),
        agg_a.total_count()
    );

    Ok(())
}
