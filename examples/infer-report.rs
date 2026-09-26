use std::{env, error::Error, io, process};

fn run() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let path = args.next();
    let sample_limit: Option<u64> = match args.next() {
        Some(raw) => Some(raw.parse()?),
        None => None,
    };
    let mut inferencer = csv::infer::Inferencer::new();
    inferencer.sample_limit(sample_limit);
    let report = match path {
        None => inferencer.infer_reader(io::stdin())?,
        Some(path) => inferencer.infer_path(path)?,
    };

    println!(
        "{:<5} {:<12} {:<9} {:<6} first promotion",
        "col", "name", "type", "empty"
    );
    for (i, column) in report.columns().iter().enumerate() {
        let name = report
            .headers()
            .get(i)
            .map(|h| String::from_utf8_lossy(h).into_owned())
            .unwrap_or_default();
        let promotion = match column.first_promotion() {
            None => "-".to_string(),
            Some(p) => format!(
                "record {} (line {}): {} -> {}",
                p.record(),
                p.position().line(),
                p.from(),
                p.to()
            ),
        };
        println!(
            "{:<5} {:<12} {:<9} {:<6} {}",
            i,
            name,
            column.ty().to_string(),
            column.empty_count(),
            promotion
        );
    }
    let status = if report.is_complete() {
        "complete".to_string()
    } else {
        "partial: sample limit reached, only a prefix was scanned".to_string()
    };
    println!("records scanned: {} ({})", report.records_scanned(), status);
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {}", err);
        process::exit(1);
    }
}
