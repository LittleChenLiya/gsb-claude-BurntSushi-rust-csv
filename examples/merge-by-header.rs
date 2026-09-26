//! Merge two CSV inputs by header name and print the result.
//!
//! Run this example with:
//!
//!     cargo run --example merge-by-header

use std::error::Error;
use std::io;

fn main() -> Result<(), Box<dyn Error>> {
    let mut merger = csv::HeaderMerger::new();
    // The first input's column order wins...
    merger.add_reader("id,name\n1,alice\n2,bob\n".as_bytes());
    // ...and columns first seen later (here: age) are appended.
    merger.add_reader("age,id\n30,3\n".as_bytes());
    merger.prepare()?;

    let mut wtr = csv::Writer::from_writer(io::stdout());
    merger.write_all(&mut wtr)?;
    wtr.flush()?;
    // Output:
    // id,name,age
    // 1,alice,
    // 2,bob,
    // 3,,30
    Ok(())
}
