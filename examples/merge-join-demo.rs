//! A short, dependency-free demo of the library-level merge join.
//!
//! Run it with:
//!
//! ```ignore
//! cargo run --example merge-join-demo
//! ```
//!
//! It demonstrates, in well under a minute:
//!
//! 1. an inner join of `a/a/b` against `a/a/c` (four cross-product rows),
//! 2. the extra unmatched `b` row produced by a left join,
//! 3. stable ordering while forced spilling with a tiny memory budget,
//! 4. the observable cleanup of temporary files, and
//! 5. the diagnostic reported when an input stream is out of order.

use std::{cell::Cell, path::PathBuf, rc::Rc};

use csv::{ByteRecord, JoinError, JoinKind, MergeJoinBuilder};

fn main() {
    let dir = std::env::temp_dir()
        .join(format!("csv-merge-join-demo-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let left =
        vec![rec(&[b"a", b"l1"]), rec(&[b"a", b"l2"]), rec(&[b"b", b"l3"])];
    let right =
        vec![rec(&[b"a", b"r1"]), rec(&[b"a", b"r2"]), rec(&[b"c", b"r3"])];

    println!("== inner join (budget forces every duplicate key to spill) ==");
    let removed = run(JoinKind::Inner, &dir, left.clone(), right.clone());
    println!("spill files observed and removed: {}", removed);

    println!();
    println!("== left join: the unmatched 'b' row has an empty right side ==");
    let _ = run(JoinKind::Left, &dir, left.clone(), right.clone());

    println!();
    println!("== out-of-order diagnostic ==");
    let bad_left = vec![
        rec(&[b"a", b"l1"]),
        rec(&[b"c", b"l2"]),
        rec(&[b"b", b"l3"]), // inversion: c then b
    ];
    let mut join = MergeJoinBuilder::new()
        .kind(JoinKind::Left)
        .spill_dir(&dir)
        .memory_budget(0)
        .build(bad_left.into_iter().map(ok), right.into_iter().map(ok));
    let mut emitted = 0;
    let error = loop {
        match join.next_row() {
            Ok(Some(_)) => emitted += 1,
            Ok(None) => panic!("expected an ordering error"),
            Err(err) => break err,
        }
    };
    println!(
        "rows emitted before the violation: {} (output is NOT complete)",
        emitted
    );
    println!("error: {}", error);
    if let JoinError::OutOfOrder { side, record_index, position } = &error {
        println!(
            "  side: {:?}, record index on that side: {}",
            side, record_index
        );
        if let Some(pos) = position {
            println!(
                "  reader position: record {}, line {}, byte {}",
                pos.record(),
                pos.line(),
                pos.byte()
            );
        }
    }
    // Consumed joins still clean up via finish (or Drop).
    let _ = join.finish();

    std::fs::remove_dir_all(&dir).unwrap();
    println!();
    println!("demo finished; temporary directory reclaimed");
}

fn run(
    kind: JoinKind,
    dir: &PathBuf,
    left: Vec<ByteRecord>,
    right: Vec<ByteRecord>,
) -> usize {
    let removed = Rc::new(Cell::new(0usize));
    let obs = Rc::clone(&removed);
    let mut join = MergeJoinBuilder::new()
        .kind(kind)
        .spill_dir(dir)
        .memory_budget(0)
        .with_cleanup_observer(move |_path| obs.set(obs.get() + 1))
        .build(left.into_iter().map(ok), right.into_iter().map(ok));
    let mut count = 0;
    while let Some((l, r)) = join.next_row().unwrap() {
        count += 1;
        let right = match r {
            Some(rec) => show(&rec),
            None => "<empty>".to_string(),
        };
        println!("{:>2}: {} | {}", count, show(&l), right);
    }
    let status = join.finish().unwrap();
    println!(
        "rows: {}, stop: {:?}, spill files removed: {}",
        status.rows_emitted(),
        status.stop_reason(),
        status.cleanup().removed()
    );
    assert_eq!(status.cleanup().removed(), removed.get());
    removed.get()
}

fn rec(fields: &[&[u8]]) -> ByteRecord {
    let mut r = ByteRecord::new();
    for f in fields {
        r.push_field(f);
    }
    r
}

fn ok(r: ByteRecord) -> Result<ByteRecord, csv::Error> {
    Ok(r)
}

fn show(r: &ByteRecord) -> String {
    let mut out = String::new();
    for (i, field) in r.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&String::from_utf8_lossy(field));
    }
    out
}
