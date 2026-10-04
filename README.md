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

### Exact per-key money totals

The `csv::amount` module sums a money column by a business key using a fixed
decimal precision and plain integer arithmetic, so values never pass through
floating point. Two chunk summaries can be merged atomically (the merge
requires the same precision and key strategy, and any conflict leaves the
target state unchanged), and output follows each key's first valid
appearance.

A small demo with two CSV chunks lives in
[`examples/amount-aggregation.rs`](examples/amount-aggregation.rs):

```text
$ cargo run --example amount-aggregation
```

It reads `examples/data/amount_chunk1.csv` (`a,0.10` / `a,0.20`) and
`examples/data/amount_chunk2.csv` (`a,0.30` / `b,1.00`) separately, merges
them (`a` totals exactly `0.60` with count `3`), and shows that `a,0.001` is
rejected at 2-digit precision without changing any total. The in-memory
state consists of the config, the next 1-based record number, and one entry
per key `{ key, count: u64, sum: i64 minor units }` in first-seen order; see
the module documentation for details.
