// A short (well under 90 seconds), self contained demonstration of the
// library level merge join:
//
//   1. An inner join over the a/a/b keys from the task.
//   2. A left join that additionally emits the unmatched left "b" row.
//   3. A tiny memory budget that forces same-key groups to spill to disk,
//      with the observable cleanup counters printed afterwards.
//   4. An out-of-order input and the resulting side/record diagnostic.
//   5. A note that fields never have to be valid UTF-8.
//
// Run it with:
//
//     cargo run --example merge-join-demo

use std::error::Error;
use std::path::PathBuf;

use csv::{
    merge_join::{JoinKind, MergeJoinBuilder, TempFiles},
    ReaderBuilder,
};

fn main() {
    let dir = std::env::temp_dir().join("csv-merge-join-demo");
    if let Err(err) = run(&dir) {
        eprintln!("{}", err);
        std::process::exit(1);
    }
}

fn run(dir: &PathBuf) -> Result<(), Box<dyn Error>> {
    let left = b"a,l0\na,l1\nb,l2\n";
    let right = b"a,r0\na,r1\nc,r2\n";

    println!("== merge join demo ==");
    println!("left input :\n{}", std::str::from_utf8(left)?);
    println!("right input:\n{}", std::str::from_utf8(right)?);

    // 1. Inner join.
    let mut temps = TempFiles::new(dir)?;
    println!("-- inner join (Cartesian product of the two 'a' groups) --");
    let mut left_rdr =
        ReaderBuilder::new().has_headers(false).from_reader(&left[..]);
    let mut right_rdr =
        ReaderBuilder::new().has_headers(false).from_reader(&right[..]);
    let mut out = csv::WriterBuilder::new().flexible(true).from_writer(vec![]);
    let status =
        MergeJoinBuilder::new().join_kind(JoinKind::Inner).run_to_writer(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut out,
        )?;
    print!("{}", String::from_utf8(out.into_inner()?)?);
    println!(
        "inner join: {} row(s), spill files created/removed: {}/{}",
        status.emitted(),
        temps.created(),
        temps.removed()
    );

    // 2 & 3. Left join with a zero byte budget, forcing every non-trivial
    // same-key group to spill into the caller provided directory.
    let mut temps = TempFiles::new(dir)?;
    println!();
    println!(
        "-- left join with memory_budget(0): the unmatched 'b' row and \
         forced spilling --"
    );
    let mut left_rdr =
        ReaderBuilder::new().has_headers(false).from_reader(&left[..]);
    let mut right_rdr =
        ReaderBuilder::new().has_headers(false).from_reader(&right[..]);
    let mut out = csv::WriterBuilder::new().flexible(true).from_writer(vec![]);
    let status = MergeJoinBuilder::new()
        .join_kind(JoinKind::Left)
        .memory_budget(0)
        .run_to_writer(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            &mut out,
        )?;
    print!("{}", String::from_utf8(out.into_inner()?)?);
    println!(
        "left join: {} row(s), spill files created: {}, removed: {}, \
         removal failures: {}, still live: {}",
        status.emitted(),
        temps.created(),
        temps.removed(),
        temps.removal_failures(),
        temps.live(),
    );
    let leftover: Vec<_> = std::fs::read_dir(dir)?
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    println!("files left in the managed directory: {:?}", leftover);

    // 4. Out-of-order input produces a precise diagnostic naming the side and
    //    the record position; the join does not pretend the result is whole.
    println!();
    println!("-- out-of-order diagnostic --");
    let broken_left = b"a,l0\nc,bogus\nb,l2\n";
    let mut temps = TempFiles::new(dir)?;
    let mut left_rdr =
        ReaderBuilder::new().has_headers(false).from_reader(&broken_left[..]);
    let mut right_rdr =
        ReaderBuilder::new().has_headers(false).from_reader(&right[..]);
    let err = MergeJoinBuilder::new()
        .join_kind(JoinKind::Left)
        .run_to_writer(
            left_rdr.byte_records(),
            right_rdr.byte_records(),
            &mut temps,
            // Discard output for this section.
            &mut csv::WriterBuilder::new()
                .flexible(true)
                .from_writer(std::io::sink()),
        )
        .expect_err("unsorted input must be reported");
    println!("{}", err);
    if let Some(pos) = err.position() {
        println!(
            "side: {:?}, record index: {}, line: {}, byte offset: {}",
            err.side(),
            pos.record(),
            pos.line(),
            pos.byte()
        );
    }
    println!(
        "cleanup after the failure: {:?}",
        err.cleanup_report().expect("cleanup runs even on failure")
    );

    // 5. Output limit stops early and reports an explicit status.
    println!();
    println!("-- early stop at max_outputs(3) --");
    let mut temps = TempFiles::new(dir)?;
    let mut left_rdr =
        ReaderBuilder::new().has_headers(false).from_reader(&left[..]);
    let mut right_rdr =
        ReaderBuilder::new().has_headers(false).from_reader(&right[..]);
    let mut seen = 0u64;
    let status = MergeJoinBuilder::new().max_outputs(3).run(
        left_rdr.byte_records(),
        right_rdr.byte_records(),
        &mut temps,
        &mut |_row| {
            seen += 1;
            Ok::<_, csv::Error>(())
        },
    )?;
    println!(
        "status is_limit: {}, rows handed to the sink: {}, clean: {}",
        status.is_limit(),
        seen,
        status.report().cleanup().is_clean()
    );

    println!();
    println!(
        "note: join keys and fields are compared as raw bytes, so \
              invalid UTF-8 (e.g. b\"k\\xFF\") works unchanged."
    );
    Ok(())
}
