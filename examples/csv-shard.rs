//! 分片写出示例：容量 2 的五条记录切成三片，每片独立带表头，
//! 并打印“原记录号 -> （片号， 片内记录号）”的归属与最终清单。
//!
//! 运行：
//!   cargo run --example csv-shard -- target/csv-shard-example

use std::error::Error;
use std::fs;
use std::path::PathBuf;

use csv::shard::Sharder;
use csv::ByteRecord;

fn main() -> Result<(), Box<dyn Error>> {
    let dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/csv-shard-example"));
    let _ = fs::remove_dir_all(&dir);

    let header = ByteRecord::from(vec!["id", "name", "note"]);
    let records = vec![
        ByteRecord::from(vec!["1", "alice", "plain"]),
        ByteRecord::from(vec!["2", "bob", "quoted,\"comma\""]),
        ByteRecord::from(vec!["3", "carol", "line one\nline two\nline three"]),
        ByteRecord::from(vec!["4", "dave", "tail"]),
        ByteRecord::from(vec!["5", "erin", "last"]),
    ];

    let mut sharder = Sharder::in_dir(&dir, &header, 2)?;
    for rec in &records {
        let a = sharder.write_record(rec)?;
        println!("record {} -> shard {} row {}", a.record, a.shard, a.row);
    }
    let manifest = sharder.finish()?;

    println!("\nmanifest ({} shards):", manifest.len());
    for entry in &manifest {
        println!(
            "  shard {} -> {} ({} records)",
            entry.index, entry.file, entry.records
        );
    }
    println!("\nshard contents under {}:", dir.display());
    for entry in &manifest {
        println!("--- {} ---", entry.file);
        print!("{}", fs::read_to_string(dir.join(&entry.file))?);
    }
    Ok(())
}
