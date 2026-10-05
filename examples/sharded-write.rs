//! Write CSV records into record-count-sharded files.
//!
//! Each shard is a standalone CSV file with a header row and at most two
//! data records. Records are counted as whole records, so a record with an
//! embedded newline in a quoted field is never split across shards.
//!
//! Shards are written to temporary paths and only published (renamed to
//! their final path) once they are fully written and flushed. The manifest
//! lists only published shards and maps each original record to its shard
//! and in-shard record number.
//!
//! Usage:
//!
//! ```text
//! cargo run --example sharded-write [output-dir]
//! ```

use csv::ShardedWriter;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "shards-out".to_string());

    let records: Vec<Vec<&str>> = vec![
        vec!["id", "description"],
        vec!["1", "first record"],
        vec!["2", "second record"],
        vec!["3", "record with\nembedded newline"],
        vec!["4", "fourth record"],
        vec!["5", "fifth record"],
    ];

    let mut wtr = ShardedWriter::builder(&dir, "shard")
        .capacity(2)
        .headers(&records[0])
        .build()?;

    for record in &records[1..] {
        wtr.write_record(record)?;
    }

    let manifest = wtr.finish()?;

    println!("wrote {} records into {} shard(s):", manifest.mapping.len(), manifest.shards.len());
    for shard in &manifest.shards {
        println!(
            "  shard {}: {} record(s) -> {}",
            shard.index,
            shard.records,
            shard.path.display()
        );
    }
    println!("record locations:");
    for (i, loc) in manifest.mapping.iter().enumerate() {
        println!(
            "  record {} -> shard {}, record {}",
            i, loc.shard, loc.record
        );
    }

    Ok(())
}
