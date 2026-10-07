csv
===
A fast and flexible CSV reader and writer for Rust, with support for Serde.

[![Build status](https://github.com/BurntSushi/rust-csv/workflows/ci/badge.svg)](https://github.com/BurntSushi/rust-csv/actions)
[![crates.io](https://img.shields.io/crates/v/csv.svg)](https://crates.io/crates/csv)

Dual-licensed under MIT or the [UNLICENSE](http://unlicense.org).


### Documentation

https://docs.rs/csv

If you're new to Rust, the
[tutorial](https://docs.rs/csv/1.*/csv/tutorial/index.html)
is a good place to start.


### Usage

To bring this crate into your repository, either add `csv` to your
`Cargo.toml`, or run `cargo add csv`.


### Example

This example shows how to read CSV data from stdin and print each record to
stdout.

There are more examples in the
[cookbook](https://docs.rs/csv/1.*/csv/cookbook/index.html).

```rust
use std::{error::Error, io, process};

fn example() -> Result<(), Box<dyn Error>> {
    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::Reader::from_reader(io::stdin());
    for result in rdr.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.
        let record = result?;
        println!("{:?}", record);
    }
    Ok(())
}

fn main() {
    if let Err(err) = example() {
        println!("error running example: {}", err);
        process::exit(1);
    }
}
```

The above example can be run like so:

```text
$ git clone git://github.com/BurntSushi/rust-csv
$ cd rust-csv
$ cargo run --example cookbook-read-basic < examples/data/smallpop.csv
```

### Example with Serde

This example shows how to read CSV data from stdin into your own custom struct.
By default, the member names of the struct are matched with the values in the
header record of your CSV data.

```rust
use std::{error::Error, io, process};

#[derive(Debug, serde::Deserialize)]
struct Record {
    city: String,
    region: String,
    country: String,
    population: Option<u64>,
}

fn example() -> Result<(), Box<dyn Error>> {
    let mut rdr = csv::Reader::from_reader(io::stdin());
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let record: Record = result?;
        println!("{:?}", record);
    }
    Ok(())
}

fn main() {
    if let Err(err) = example() {
        println!("error running example: {}", err);
        process::exit(1);
    }
}
```

The above example can be run like so:

```
$ git clone git://github.com/BurntSushi/rust-csv
$ cd rust-csv
$ cargo run --example cookbook-read-serde < examples/data/smallpop.csv
```

### Merge joins

The `csv::merge_join` module performs library level merge joins over two
`ByteRecord` streams that are already sorted by a key column in non-decreasing
byte order. Inner and left joins are supported, duplicate keys expand to a
Cartesian product, and large same-key groups spill to temporary files in a
caller provided directory while preserving the stable "left order, then right
order" output. A maximum number of output rows can be configured, unsorted
input is reported with the offending side and record position, and fields are
not required to be valid UTF-8.

```ignore
use csv::{
    merge_join::{JoinKind, MergeJoinBuilder, TempFiles},
    ReaderBuilder,
};

let left = b"a,l0\na,l1\nb,l2\n";
let right = b"a,r0\na,r1\nc,r2\n";
let mut lrdr = ReaderBuilder::new().has_headers(false).from_reader(&left[..]);
let mut rrdr = ReaderBuilder::new().has_headers(false).from_reader(&right[..]);

let mut temps = TempFiles::new(std::env::temp_dir().join("my-join-spills"))?;
let mut wtr = csv::WriterBuilder::new()
    .flexible(true)
    .from_writer(vec![]);
let status = MergeJoinBuilder::new()
    .join_kind(JoinKind::Left)
    .run_to_writer(lrdr.byte_records(), rrdr.byte_records(), &mut temps, &mut wtr)?;

assert_eq!(status.emitted(), 5); // four Cartesian product rows plus the unmatched "b"
