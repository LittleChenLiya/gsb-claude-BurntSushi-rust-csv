//! A thin command line front-end over the reusable `csv::extsort` module.
//!
//! All sorting logic lives in the library (`src/extsort.rs`); this example
//! only parses arguments and prints results.
//!
//! Usage:
//!
//! ```text
//! cargo run --example extsort -- <input.csv> <output.csv> [options]
//! cargo run --example extsort -- --demo
//! ```
//!
//! Options:
//!
//! ```text
//! -c, --columns <i,j,...>     column indices of the sort key (default: 0)
//! -b, --budget <bytes>        memory budget for in-memory records
//!                             (default: 16 MiB)
//! -m, --max-open-runs <n>     max run files merged at once (default: 8)
//! -d, --delimiter <char>      field delimiter (default: ,)
//! -q, --quote <char>          quote character (default: ")
//!     --no-headers            treat the first row as data, not a header
//!     --demo                  run a self-contained demo on tricky data and
//!                             verify it against an in-memory stable sort
//! ```
//!
//! Memory budget accounting (also documented in `csv::extsort`):
//!
//! ```text
//! estimate(record) = size_of::<ByteRecord>()           // handle in the Vec
//!                  + 128 bytes struct overhead         // boxed inner struct
//!                  + record.as_slice().len()           // field data bytes
//!                  + record.len() * size_of::<usize>() // field bounds index
//! ```
//!
//! When the running total exceeds the budget, the buffered records are
//! stably sorted and spilled to a sorted run file; runs are then merged
//! k-way with at most `--max-open-runs` files open, in as many rounds as
//! needed. A single record larger than the budget is rejected.

use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use csv::extsort::{estimate_record_size, ExternalSorter, SortStats};
use csv::{ByteRecord, ReaderBuilder, WriterBuilder};

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.iter().any(|a| a == "--demo") {
        return match demo() {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("demo failed: {}", err);
                ExitCode::FAILURE
            }
        };
    }
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("error: {}", msg);
            eprintln!();
            eprintln!("{}", usage());
            ExitCode::FAILURE
        }
    }
}

fn usage() -> String {
    "usage: extsort <input.csv> <output.csv> \
     [-c cols] [-b budget] [-m max-open-runs] [-d delim] [-q quote] \
     [--no-headers]\n       extsort --demo"
        .to_string()
}

struct Cli {
    input: PathBuf,
    output: PathBuf,
    columns: Vec<usize>,
    budget: usize,
    max_open_runs: usize,
    delimiter: u8,
    quote: u8,
    has_headers: bool,
}

fn parse_args(args: &[String]) -> Result<Cli, String> {
    let mut positional = vec![];
    let mut columns = vec![0];
    let mut budget = 16 * (1 << 20);
    let mut max_open_runs = 8;
    let mut delimiter = b',';
    let mut quote = b'"';
    let mut has_headers = true;
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        let mut take = |name: &str| -> Result<String, String> {
            it.next()
                .cloned()
                .ok_or_else(|| format!("missing value for {}", name))
        };
        match arg.as_str() {
            "-c" | "--columns" => {
                let spec = take("--columns")?;
                columns = spec
                    .split(',')
                    .map(|s| {
                        s.trim()
                            .parse::<usize>()
                            .map_err(|e| format!("bad column {:?}: {}", s, e))
                    })
                    .collect::<Result<_, _>>()?;
            }
            "-b" | "--budget" => {
                budget = take("--budget")?
                    .parse()
                    .map_err(|e| format!("bad budget: {}", e))?;
            }
            "-m" | "--max-open-runs" => {
                max_open_runs = take("--max-open-runs")?
                    .parse()
                    .map_err(|e| format!("bad max-open-runs: {}", e))?;
            }
            "-d" | "--delimiter" => {
                let s = take("--delimiter")?;
                let b = s.as_bytes();
                if b.len() != 1 {
                    return Err("delimiter must be a single byte".to_string());
                }
                delimiter = b[0];
            }
            "-q" | "--quote" => {
                let s = take("--quote")?;
                let b = s.as_bytes();
                if b.len() != 1 {
                    return Err("quote must be a single byte".to_string());
                }
                quote = b[0];
            }
            "--no-headers" => has_headers = false,
            other if other.starts_with('-') => {
                return Err(format!("unknown option: {}", other));
            }
            other => positional.push(other.to_string()),
        }
    }
    if positional.len() != 2 {
        return Err(
            "expected exactly <input.csv> and <output.csv>".to_string()
        );
    }
    if columns.is_empty() {
        return Err("at least one sort column is required".to_string());
    }
    Ok(Cli {
        input: PathBuf::from(&positional[0]),
        output: PathBuf::from(&positional[1]),
        columns,
        budget,
        max_open_runs,
        delimiter,
        quote,
        has_headers,
    })
}

fn run(args: &[String]) -> Result<(), String> {
    let cli = parse_args(args)?;
    let mut rdr_builder = ReaderBuilder::new();
    rdr_builder
        .delimiter(cli.delimiter)
        .quote(cli.quote)
        .has_headers(cli.has_headers);
    let mut wtr_builder = WriterBuilder::new();
    wtr_builder
        .delimiter(cli.delimiter)
        .quote(cli.quote)
        .has_headers(cli.has_headers);
    let sorter =
        ExternalSorter::new(cli.columns.clone(), |a: &[u8], b: &[u8]| {
            a.cmp(b)
        })
        .memory_budget(cli.budget)
        .max_open_runs(cli.max_open_runs);
    let stats = sorter
        .sort_file(&rdr_builder, &cli.input, &wtr_builder, &cli.output)
        .map_err(|e| e.to_string())?;
    print_stats(&stats);
    Ok(())
}

fn print_stats(stats: &SortStats) {
    println!("records read:    {}", stats.records_read);
    println!("records written: {}", stats.records_written);
    println!("runs created:    {}", stats.runs_created);
    println!("merge rounds:    {}", stats.merge_rounds);
    println!("bytes spilled:   {}", stats.bytes_spilled);
    println!("peak buffer est: {}", stats.peak_buffer_estimate);
}

/// Runs a self-contained, repeatable demo: builds a CSV with duplicate keys,
/// quoted newlines, empty fields and non-UTF-8 bytes, sorts it with a tiny
/// memory budget (forcing multiple runs and merge rounds) and verifies the
/// result field-by-field against an in-memory stable sort.
fn demo() -> Result<(), Box<dyn std::error::Error>> {
    let dir = env::temp_dir()
        .join(format!("csv-extsort-demo-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let result = demo_in(&dir);
    let _ = fs::remove_dir_all(&dir);
    result
}

fn demo_in(dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let input = dir.join("input.csv");
    let output = dir.join("sorted.csv");

    // Columns: key, payload, seq (original order, for stability checks).
    let rows: Vec<Vec<&[u8]>> = vec![
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
    ];
    {
        let mut wtr = WriterBuilder::new().from_path(&input)?;
        wtr.write_byte_record(&ByteRecord::from(vec![
            "key", "payload", "seq",
        ]))?;
        for row in &rows {
            wtr.write_byte_record(&ByteRecord::from(row.clone()))?;
        }
        wtr.flush()?;
    }

    println!("== input records (before sort) ==");
    print_records(&input, true)?;

    // A tiny budget (400 bytes) forces several runs; max_open_runs=2 forces
    // multiple merge rounds.
    let sorter = ExternalSorter::new(vec![0], |a: &[u8], b: &[u8]| a.cmp(b))
        .memory_budget(400)
        .max_open_runs(2);
    let stats = sorter.sort_file(
        &ReaderBuilder::new(),
        &input,
        &WriterBuilder::new(),
        &output,
    )?;

    println!();
    println!("== run statistics ==");
    print_stats(&stats);

    println!();
    println!("== output records (after sort) ==");
    print_records(&output, true)?;

    // Verify against an in-memory stable sort, field by field.
    let mut expected = read_all(&input, true)?.1;
    expected.sort_by(|a, b| a[0].cmp(&b[0]));
    let (got_header, got) = read_all(&output, true)?;
    assert_eq!(
        got_header,
        Some(ByteRecord::from(vec!["key", "payload", "seq"])),
        "header must be preserved exactly once"
    );
    assert_eq!(got.len(), expected.len(), "record count mismatch");
    for (i, (g, e)) in got.iter().zip(expected.iter()).enumerate() {
        assert_eq!(g.len(), e.len(), "record {}: field count mismatch", i);
        for (j, (gf, ef)) in g.iter().zip(e.iter()).enumerate() {
            assert_eq!(
                gf, ef,
                "record {} field {} mismatch: got {:?}, want {:?}",
                i, j, gf, ef
            );
        }
    }
    assert!(stats.runs_created >= 3, "demo should create >= 3 runs");
    assert!(stats.merge_rounds >= 2, "demo should take >= 2 merge rounds");
    println!();
    println!(
        "OK: {} records match an in-memory stable sort field-by-field \
         ({} runs, {} merge rounds)",
        got.len(),
        stats.runs_created,
        stats.merge_rounds
    );
    Ok(())
}

fn read_all(
    path: &Path,
    has_headers: bool,
) -> io::Result<(Option<ByteRecord>, Vec<ByteRecord>)> {
    let mut rdr =
        ReaderBuilder::new().has_headers(has_headers).from_path(path)?;
    let headers =
        if has_headers { Some(rdr.byte_headers()?.clone()) } else { None };
    let mut records = vec![];
    let mut rec = ByteRecord::new();
    while rdr.read_byte_record(&mut rec)? {
        records.push(rec.clone());
    }
    Ok((headers, records))
}

fn print_records(path: &Path, has_headers: bool) -> io::Result<()> {
    let (headers, records) = read_all(path, has_headers)?;
    if let Some(h) = headers {
        println!("header: {}", show_record(&h));
    }
    for (i, rec) in records.iter().enumerate() {
        println!(
            "{:3} [est. {:4}B] {}",
            i,
            estimate_record_size(rec),
            show_record(rec)
        );
    }
    Ok(())
}

fn show_record(rec: &ByteRecord) -> String {
    let fields: Vec<String> = rec
        .iter()
        .map(|f| {
            f.iter()
                .map(|&b| {
                    if (0x20..0x7f).contains(&b) && b != 0x5c {
                        (b as char).to_string()
                    } else {
                        format!("\\x{:02x}", b)
                    }
                })
                .collect()
        })
        .collect();
    format!("[{}]", fields.join(" | "))
}
