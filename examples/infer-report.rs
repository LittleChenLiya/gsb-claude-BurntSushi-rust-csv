use std::env;
use std::fs::File;
use std::io;
use std::process;

use csv::{InferenceReport, Inferencer, ReaderBuilder};

fn usage() -> ! {
    eprintln!(
        "Usage: infer-report [FILE] [SAMPLE-LIMIT]

Infers the type of each CSV column (null, boolean, integer, decimal or
text) without modifying the input. Reads stdin when FILE is omitted or
'-'. SAMPLE-LIMIT optionally caps how many data records are scanned; the
report is then marked as a prefix-only (incomplete) conclusion."
    );
    process::exit(2);
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        usage();
    }
    let (path, limit) = match args.len() {
        0 => (None, None),
        1 => match args[0].parse::<u64>() {
            Ok(n) => (None, Some(n)),
            Err(_) => (Some(args[0].clone()), None),
        },
        2 => {
            let limit = args[1].parse::<u64>().unwrap_or_else(|_| usage());
            (Some(args[0].clone()), Some(limit))
        }
        _ => usage(),
    };
    let inferencer = match limit {
        None => Inferencer::new(),
        Some(n) => Inferencer::with_sample_limit(n),
    };
    let report = match run(&inferencer, path.as_deref()) {
        Ok(report) => report,
        Err(err) => {
            eprintln!("error: {}", err);
            process::exit(1);
        }
    };
    print_report(&report);
}

fn run(
    inferencer: &Inferencer,
    path: Option<&str>,
) -> csv::Result<InferenceReport> {
    match path {
        None | Some("-") => {
            let mut rdr = ReaderBuilder::new().from_reader(io::stdin());
            inferencer.infer(&mut rdr)
        }
        Some(path) => {
            let file = File::open(path)?;
            let mut rdr =
                ReaderBuilder::new().from_reader(io::BufReader::new(file));
            inferencer.infer(&mut rdr)
        }
    }
}

fn print_report(report: &InferenceReport) {
    println!("records scanned: {}", report.records_scanned());
    if report.complete() {
        println!("complete: yes (entire input was scanned)");
    } else {
        println!(
            "complete: no (sample limit reached; \
             this is a prefix-only conclusion)"
        );
    }
    let empty = Vec::new();
    let headers = report.headers().unwrap_or(&empty);
    for (i, col) in report.columns().iter().enumerate() {
        let name = headers.get(i).map(String::as_str).unwrap_or("?");
        println!(
            "column {} ({}): type={:?}, nulls={}",
            i,
            name,
            col.inferred_type(),
            col.null_count()
        );
        if let Some(p) = col.first_promotion() {
            println!(
                "  first promoted from {:?} to {:?} at record {} \
                 (line {}, byte {})",
                p.from(),
                p.to(),
                p.record(),
                p.line(),
                p.byte()
            );
        }
    }
}
