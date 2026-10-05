//! Integration tests for the record-count-sharded CSV writer.

use csv::{ErrorKind, ReaderBuilder, ShardLocation, ShardedWriter};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Return a fresh test directory under the worktree's `target` directory so
/// that all test artifacts stay on the D drive.
fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::current_dir()
        .unwrap()
        .join("target")
        .join("shard-tests")
        .join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// A writer that fails on its first write attempt and delegates all
/// subsequent writes to the inner writer.
///
/// This simulates a transient I/O failure while writing a shard: the first
/// shard that uses this writer cannot be written, but a fresh writer for the
/// same temporary path succeeds on retry.
struct FirstWriteFails<W> {
    inner: W,
    failed: bool,
}

impl<W: Write> FirstWriteFails<W> {
    fn new(inner: W) -> FirstWriteFails<W> {
        FirstWriteFails {
            inner,
            failed: false,
        }
    }
}

impl<W: Write> Write for FirstWriteFails<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.failed {
            self.failed = true;
            Err(io::Error::new(
                io::ErrorKind::Other,
                "injected shard write fault",
            ))
        } else {
            self.inner.write(buf)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// A writer that fails on its first flush and delegates all subsequent
/// flushes (and all writes) to the inner writer.
///
/// This simulates a transient I/O failure while publishing a shard: the
/// shard's records are written but the flush that precedes publication
/// fails.
struct FirstFlushFails<W> {
    inner: W,
    failed: bool,
}

impl<W: Write> FirstFlushFails<W> {
    fn new(inner: W) -> FirstFlushFails<W> {
        FirstFlushFails {
            inner,
            failed: false,
        }
    }
}

impl<W: Write> Write for FirstFlushFails<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.failed {
            self.failed = true;
            Err(io::Error::new(
                io::ErrorKind::Other,
                "injected shard flush fault",
            ))
        } else {
            self.inner.flush()
        }
    }
}

/// Read a shard file back into its headers and records.
fn read_shard(path: &Path) -> (Vec<String>, Vec<Vec<String>>) {
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .unwrap();
    let headers: Vec<String> = rdr
        .headers()
        .unwrap()
        .iter()
        .map(|s| s.to_string())
        .collect();
    let records: Vec<Vec<String>> = rdr
        .records()
        .map(|r| {
            r.unwrap()
                .iter()
                .map(|s| s.to_string())
                .collect()
        })
        .collect();
    (headers, records)
}

fn sample_records() -> Vec<Vec<&'static str>> {
    vec![
        vec!["1", "alpha"],
        vec!["2", "beta"],
        vec!["3", "line1\nline2"],
        vec!["4", "gamma"],
        vec!["5", "delta"],
    ]
}

#[test]
fn shards_by_record_count_keep_embedded_newlines() {
    let dir = test_dir("shards-by-count");
    let records = sample_records();
    let mut wtr = ShardedWriter::builder(&dir, "data")
        .capacity(2)
        .headers(&["id", "text"])
        .build()
        .unwrap();
    for record in &records {
        wtr.write_record(record).unwrap();
    }
    let manifest = wtr.finish().unwrap();

    // Five records at capacity two produce exactly three shards.
    assert_eq!(manifest.shards.len(), 3);
    assert_eq!(manifest.mapping.len(), 5);
    assert_eq!(
        manifest
            .shards
            .iter()
            .map(|s| s.index)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        manifest.mapping,
        vec![
            ShardLocation { shard: 0, record: 0 },
            ShardLocation { shard: 0, record: 1 },
            ShardLocation { shard: 1, record: 0 },
            ShardLocation { shard: 1, record: 1 },
            ShardLocation { shard: 2, record: 0 },
        ]
    );

    // Shard 0: records 0 and 1.
    let (headers, records) = read_shard(&manifest.shards[0].path);
    assert_eq!(headers, vec!["id", "text"]);
    assert_eq!(
        records,
        vec![vec!["1", "alpha"], vec!["2", "beta"]]
    );

    // Shard 1: records 2 and 3. The record with an embedded newline stays
    // intact and is not split across shards.
    let (headers, records) = read_shard(&manifest.shards[1].path);
    assert_eq!(headers, vec!["id", "text"]);
    assert_eq!(
        records,
        vec![
            vec!["3", "line1\nline2"],
            vec!["4", "gamma"],
        ]
    );

    // Shard 2: record 4.
    let (headers, records) = read_shard(&manifest.shards[2].path);
    assert_eq!(headers, vec!["id", "text"]);
    assert_eq!(records, vec![vec!["5", "delta"]]);

    // No temporary files are left behind.
    let names = list_file_names(&dir);
    assert!(names.iter().all(|n| !n.starts_with('.')));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn failed_shard_is_not_registered_retry_rewrites_it() {
    let dir = test_dir("failed-shard-retry");
    let attempts = std::cell::Cell::new(0u32);
    let factory =
        move |index: usize, path: &Path| -> io::Result<Box<dyn Write>> {
            let file = File::create(path)?;
            // The first attempt to write shard 3 fails immediately.
            if index == 2 && attempts.get() == 0 {
                attempts.set(1);
                Ok(Box::new(FirstWriteFails::new(file)))
            } else {
                Ok(Box::new(file))
            }
        };
    let records = sample_records();
    let mut wtr = ShardedWriter::builder(&dir, "data")
        .capacity(2)
        .buffer_capacity(1)
        .headers(&["id", "text"])
        .factory(factory)
        .build()
        .unwrap();

    for record in &records[..4] {
        wtr.write_record(record).unwrap();
    }

    // The fifth record starts shard 3, whose first write (the header)
    // fails. The error is returned and the shard is not registered.
    let err = wtr.write_record(&records[4]).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Io(_)));

    // Only the first two shards were published; no third shard or temporary
    // file exists.
    let mut names = list_file_names(&dir);
    names.sort();
    assert_eq!(names, vec!["data-0000.csv", "data-0001.csv"]);

    // Retry the same record: the failed shard is rewritten from a fresh
    // temporary file and published exactly once. Old shards are not
    // re-registered.
    wtr.write_record(&records[4]).unwrap();
    let manifest = wtr.finish().unwrap();

    assert_eq!(manifest.shards.len(), 3);
    assert_eq!(
        manifest
            .shards
            .iter()
            .map(|s| s.index)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(manifest.mapping.len(), 5);
    assert_eq!(
        manifest.mapping,
        vec![
            ShardLocation { shard: 0, record: 0 },
            ShardLocation { shard: 0, record: 1 },
            ShardLocation { shard: 1, record: 0 },
            ShardLocation { shard: 1, record: 1 },
            ShardLocation { shard: 2, record: 0 },
        ]
    );

    // The first two shards are unchanged and consistent with the manifest.
    let (_, records) = read_shard(&manifest.shards[0].path);
    assert_eq!(
        records,
        vec![vec!["1", "alpha"], vec!["2", "beta"]]
    );
    let (_, records) = read_shard(&manifest.shards[1].path);
    assert_eq!(
        records,
        vec![
            vec!["3", "line1\nline2"],
            vec!["4", "gamma"],
        ]
    );
    let (_, records) = read_shard(&manifest.shards[2].path);
    assert_eq!(records, vec![vec!["5", "delta"]]);

    // No temporary files are left behind.
    let names = list_file_names(&dir);
    assert!(names.iter().all(|n| !n.starts_with('.')));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn failed_publish_is_retried_without_duplication() {
    let dir = test_dir("failed-publish-retry");
    let attempts = std::cell::Cell::new(0u32);
    let factory =
        move |index: usize, path: &Path| -> io::Result<Box<dyn Write>> {
            let file = File::create(path)?;
            // The first attempt to publish shard 3 fails at the flush.
            if index == 2 && attempts.get() == 0 {
                attempts.set(1);
                Ok(Box::new(FirstFlushFails::new(file)))
            } else {
                Ok(Box::new(file))
            }
        };
    // Six records: shard 0 (0,1), shard 1 (2,3), shard 2 (4,5).
    let records: Vec<Vec<&str>> = vec![
        vec!["1", "alpha"],
        vec!["2", "beta"],
        vec!["3", "gamma"],
        vec!["4", "delta"],
        vec!["5", "epsilon"],
        vec!["6", "zeta"],
    ];
    let mut wtr = ShardedWriter::builder(&dir, "data")
        .capacity(2)
        .buffer_capacity(1)
        .headers(&["id", "text"])
        .factory(factory)
        .build()
        .unwrap();

    for record in &records[..5] {
        wtr.write_record(record).unwrap();
    }

    // Record 5 fills shard 2; the flush fails and the shard is not
    // published.
    let err = wtr.write_record(&records[5]).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Io(_)));

    // Only the first two shards were published.
    let mut names = list_file_names(&dir);
    names.sort();
    assert_eq!(names, vec!["data-0000.csv", "data-0001.csv"]);

    // Retry record 5: the shard is rewritten from the buffered records and
    // published exactly once. The retried record is not written twice.
    wtr.write_record(&records[5]).unwrap();
    let manifest = wtr.finish().unwrap();

    assert_eq!(manifest.shards.len(), 3);
    assert_eq!(manifest.mapping.len(), 6);
    assert_eq!(
        manifest.mapping,
        vec![
            ShardLocation { shard: 0, record: 0 },
            ShardLocation { shard: 0, record: 1 },
            ShardLocation { shard: 1, record: 0 },
            ShardLocation { shard: 1, record: 1 },
            ShardLocation { shard: 2, record: 0 },
            ShardLocation { shard: 2, record: 1 },
        ]
    );

    // Shard 2 contains exactly records 4 and 5, with no duplication.
    let (_, records) = read_shard(&manifest.shards[2].path);
    assert_eq!(
        records,
        vec![vec!["5", "epsilon"], vec!["6", "zeta"]]
    );

    // No temporary files are left behind.
    let names = list_file_names(&dir);
    assert!(names.iter().all(|n| !n.starts_with('.')));
    let _ = fs::remove_dir_all(&dir);
}

fn list_file_names(dir: &Path) -> Vec<String> {
    fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect()
}
