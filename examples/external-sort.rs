//! A thin CLI/demo wrapper around `csv::external_sort::ExternalSorter`.
//!
//! All sorting logic lives in the library (`src/external_sort.rs`); this
//! example only parses arguments, prints records and reports statistics.
//!
//! Usage:
//!
//!   external-sort                          # self-contained demo
//!   external-sort INPUT OUTPUT BUDGET COLS [MAX_OPEN_RUNS]
//!
//! COLS is a comma separated list of 0-based sort column indices, e.g.
//! "0,2". BUDGET is the memory budget in bytes (see the `external_sort`
//! module docs for how memory use is estimated).
//!
//! With no arguments, a demo CSV containing duplicate keys, quoted
//! newlines, empty fields and non-UTF-8 bytes is generated in a temporary
//! directory, sorted with a tiny budget (forcing multiple runs and merge
//! rounds), and the records before/after sorting plus the run statistics
//! are printed. The demo is deterministic and finishes in well under a
//! second.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;

use csv::external_sort::{record_memory_estimate, ExternalSorter, SortEvent};
use csv::{ByteRecord, ReaderBuilder, WriterBuilder};

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {}", err);
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() {
        return demo();
    }
    if args.len() < 4 {
        return Err(concat!(
            "usage: external-sort INPUT OUTPUT BUDGET COLS [MAX_OPEN_RUNS]",
        )
        .into());
    }
    let input = PathBuf::from(&args[0]);
    let output = PathBuf::from(&args[1]);
    let budget: usize = args[2].parse()?;
    let columns: Vec<usize> = args[3]
        .split(',')
        .map(|c| c.trim().parse().map_err(Into::into))
        .collect::<Result<_, Box<dyn std::error::Error>>>()?;
    let max_open_runs: usize =
        args.get(4).map(|s| s.parse()).transpose()?.unwrap_or(16);

    println!("== input records ({}) ==", input.display());
    print_records(&input)?;

    let stats = ExternalSorter::new()
        .columns(&columns)
        .memory_budget(budget)
        .max_open_runs(max_open_runs)
        .compare_by(|a: &[u8], b: &[u8]| a.cmp(b))
        .on_event(|event| {
            println!("event: {:?}", event);
            Ok(())
        })
        .sort_path(
            &input,
            &output,
            &ReaderBuilder::new(),
            &WriterBuilder::new(),
        )?;

    println!("== sorted records ({}) ==", output.display());
    print_records(&output)?;
    println!("== run statistics ==\n{}", format_stats(&stats));
    Ok(())
}

/// Runs the self-contained demo: generates a tricky CSV, sorts it with a
/// tiny budget and shows records before/after plus run statistics.
fn demo() -> Result<(), Box<dyn std::error::Error>> {
    let dir = env::temp_dir()
        .join(format!("csv-external-sort-demo-{}", process::id()));
    fs::create_dir_all(&dir)?;
    let input = dir.join("demo-input.csv");
    let output = dir.join("demo-sorted.csv");

    // Duplicate keys on column 0, a quoted newline, an empty field and
    // non-UTF-8 bytes. The tag column records the original input order so
    // stability is visible. Values are padded so that every record has the
    // same estimated size, which makes the run layout deterministic.
    let keys: &[&[u8]] = &[
        b"pear", b"apple", b"kiwi", b"apple", b"pear", b"apple", b"kiwi",
        b"banana", b"apple", b"banana", b"kiwi", b"apple",
    ];
    let mut records: Vec<ByteRecord> = vec![];
    for (i, key) in keys.iter().enumerate() {
        let mut rec = ByteRecord::new();
        rec.push_field(key);
        let val: &[u8] = match i {
            // A quoted field containing a newline (must stay one record).
            1 => b"line1\nline2",
            // An empty field.
            4 => b"",
            // Non-UTF-8 bytes, preserved verbatim via ByteRecord.
            7 => b"\xff\xfe-invalid-utf8",
            _ => b"normal",
        };
        let mut padded = val.to_vec();
        padded.resize(16, b'#');
        rec.push_field(&padded);
        rec.push_field(format!("{:04}", i).as_bytes());
        records.push(rec);
    }

    let mut wtr = WriterBuilder::new().from_path(&input)?;
    wtr.write_record(["key", "value", "tag"])?;
    for rec in &records {
        wtr.write_byte_record(rec)?;
    }
    wtr.flush()?;
    drop(wtr);

    // Budget: exactly two records per run -> 12 records = 6 runs, and with
    // a fan-in of 3 the merge needs two rounds (6 -> 2 -> output).
    let budget = 2 * records.iter().map(record_memory_estimate).max().unwrap();
    println!("demo directory: {}", dir.display());
    println!("memory budget: {} bytes (2 records per run)", budget);
    println!("== input records ==");
    print_records(&input)?;

    let stats = ExternalSorter::new()
        .columns(&[0])
        .memory_budget(budget)
        .max_open_runs(3)
        .compare_by(|a: &[u8], b: &[u8]| a.cmp(b))
        .on_event(|event| {
            match &event {
                SortEvent::RunCreated { records, bytes, .. } => {
                    println!(
                        "run written: {} records, {} bytes",
                        records, bytes
                    );
                }
                SortEvent::MergeRoundStart { round, inputs, .. } => {
                    println!(
                        "merge round {}: merging {} runs",
                        round,
                        inputs.len()
                    );
                }
                SortEvent::OutputReady { .. } => {
                    println!("output ready, publishing");
                }
            }
            Ok(())
        })
        .sort_path(
            &input,
            &output,
            &ReaderBuilder::new(),
            &WriterBuilder::new(),
        )?;

    println!("== sorted records ==");
    print_records(&output)?;
    println!("== run statistics ==\n{}", format_stats(&stats));
    Ok(())
}

fn print_records(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut rdr = ReaderBuilder::new().from_path(path)?;
    let headers = rdr.byte_headers()?.clone();
    println!("  header: {}", show_record(&headers));
    for (i, result) in rdr.byte_records().enumerate() {
        println!("  {:>3}: {}", i, show_record(&result?));
    }
    Ok(())
}

fn show_record(rec: &ByteRecord) -> String {
    let fields: Vec<String> = rec.iter().map(show_field).collect();
    fields.join(" | ")
}

fn show_field(field: &[u8]) -> String {
    let mut out = String::from("\"");
    for &b in field {
        match b {
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'"' => out.push_str("\\\""),
            0x20..=0x7e => out.push(b as char),
            _ => out.push_str(&format!("\\x{:02x}", b)),
        }
    }
    out.push('"');
    out
}

fn format_stats(stats: &csv::external_sort::SortStats) -> String {
    format!(
        "  records:      {}\n  runs:         {}\n  merge rounds: {}\n  run bytes:    {}\n  had header:   {}",
        stats.records, stats.runs, stats.merge_rounds, stats.run_bytes,
        stats.has_headers,
    )
}
