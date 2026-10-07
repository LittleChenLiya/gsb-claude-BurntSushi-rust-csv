/*!
Integration tests for the sparse random access index.

These tests build CSV data containing CRLF terminators, quoted fields with
embedded newlines and empty fields, index it with a checkpoint every 2
records and verify that random access reads match sequential reads byte for
byte. They also verify that corrupt or stale indexes are rejected.
*/

use std::io;

use csv::ByteRecord;
use csv_index::{Dialect, SourceMeta, SparseIndex, SparseRandomAccess};

/// The length of the fixed size index header: 8 bytes of magic, a 4 byte
/// version, then interval, checkpoint count, record count, source length,
/// source digest and dialect fingerprint (8 bytes each) and a 16 byte
/// dialect block.
const HEADER_LEN: usize = 8 + 4 + 8 * 6 + 16;
/// The length of a single encoded checkpoint: record, byte, line.
const CHECKPOINT_LEN: usize = 24;

/// Write a big endian u64 into `buf` at `offset`.
fn put_u64(buf: &mut [u8], offset: usize, val: u64) {
    buf[offset..offset + 8].copy_from_slice(&val.to_be_bytes());
}

/// CSV data with CRLF record terminators, quoted fields containing embedded
/// LF and CRLF newlines, escaped quotes and empty fields. Records 0, 2 and
/// 4 (the checkpoints when the interval is 2) contain quoted newlines, so
/// scanning forward from a checkpoint must cross them correctly.
fn sample_data() -> Vec<u8> {
    let rows: Vec<&[u8]> = vec![
        b"h1,h2,h3",
        b"\"a\r\nb\",,\"x\"\"y\"",
        b"\"p\nq\r\nr\",,",
        b"plain,,",
        b"\"multi\r\nline\",z,",
        b",,\"end\"",
        b"last,\"q\r\nq\",9",
    ];
    // Note: no trailing terminator after the last record.
    rows.join(&b"\r\n"[..])
}

/// Build a sparse index over `data` with the given dialect and interval.
/// Returns the serialized index and the source metadata.
fn build_index(
    data: &[u8],
    dialect: &Dialect,
    interval: u64,
) -> (Vec<u8>, SourceMeta) {
    let meta = SourceMeta::from_reader(data).unwrap();
    let mut builder = dialect.reader_builder();
    builder.has_headers(false);
    let mut rdr = builder.from_reader(io::Cursor::new(data));
    let mut idxbuf = io::Cursor::new(vec![]);
    SparseIndex::create(&mut rdr, interval, &meta, dialect, &mut idxbuf)
        .unwrap();
    (idxbuf.into_inner(), meta)
}

/// Read all records sequentially, in order.
fn sequential_records(data: &[u8], dialect: &Dialect) -> Vec<ByteRecord> {
    let mut builder = dialect.reader_builder();
    builder.has_headers(false);
    let mut rdr = builder.from_reader(io::Cursor::new(data));
    let mut records = vec![];
    let mut record = ByteRecord::new();
    while rdr.read_byte_record(&mut record).unwrap() {
        records.push(record.clone());
    }
    records
}

#[test]
fn random_access_matches_sequential() {
    let data = sample_data();
    let dialect = Dialect::default();
    let (idxbytes, meta) = build_index(&data, &dialect, 2);

    let idx = SparseIndex::open(&idxbytes[..], &meta, &dialect).unwrap();
    assert_eq!(idx.interval(), 2);

    let expected = sequential_records(&data, &dialect);
    let n = expected.len() as u64;
    assert_eq!(n, 7);
    assert_eq!(idx.len(), n);

    // Every checkpoint must be at most `interval` records away from the
    // record it is used to reach.
    for k in 0..n {
        let pos = idx.checkpoint_for(k).unwrap();
        assert!(pos.record() <= k);
        assert!(k - pos.record() < 2);
        assert_eq!(pos.record() % 2, 0);
    }
    assert!(idx.checkpoint_for(n).is_none());

    let mut ra = SparseRandomAccess::from_reader(io::Cursor::new(&data), idx);

    // A deterministic shuffled order covering every record: 1,4,0,3,6,2,5.
    let order: Vec<u64> = (0..n).map(|i| (i * 3 + 1) % n).collect();
    {
        let mut sorted = order.clone();
        sorted.sort();
        assert_eq!(sorted, (0..n).collect::<Vec<u64>>());
    }
    for &k in &order {
        let got = ra.get(k).unwrap().unwrap();
        assert_eq!(&got, &expected[k as usize], "record {} mismatch", k);
        assert_eq!(
            got.as_slice(),
            expected[k as usize].as_slice(),
            "record {} raw bytes mismatch",
            k
        );
        // The position reported for the record must match the position
        // reported during a sequential scan.
        assert_eq!(
            got.position().unwrap().byte(),
            expected[k as usize].position().unwrap().byte()
        );
        assert_eq!(
            got.position().unwrap().line(),
            expected[k as usize].position().unwrap().line()
        );
    }

    // Reading every record in order must work too.
    for k in 0..n {
        let got = ra.get(k).unwrap().unwrap();
        assert_eq!(&got, &expected[k as usize]);
    }

    // Out of range reads return None.
    assert!(ra.get(n).unwrap().is_none());
    assert!(ra.get(n + 100).unwrap().is_none());
}

#[test]
fn random_access_with_headers() {
    // The default dialect interprets the first record as headers. The
    // index still covers the header record as record 0.
    let data = sample_data();
    let mut dialect = Dialect::default();
    dialect.has_headers = true;
    let meta = SourceMeta::from_reader(&data[..]).unwrap();
    let mut rdr = dialect.reader_builder().from_reader(io::Cursor::new(&data));
    let mut idxbuf = io::Cursor::new(vec![]);
    SparseIndex::create(&mut rdr, 2, &meta, &dialect, &mut idxbuf).unwrap();

    let idx =
        SparseIndex::open(&idxbuf.into_inner()[..], &meta, &dialect).unwrap();
    let expected = sequential_records(&data, &dialect);
    let mut ra = SparseRandomAccess::from_reader(io::Cursor::new(&data), idx);
    for k in (0..expected.len() as u64).rev() {
        let got = ra.get(k).unwrap().unwrap();
        assert_eq!(&got, &expected[k as usize], "record {} mismatch", k);
    }
    // Record 0 is the header row.
    let header = ra.get(0).unwrap().unwrap();
    assert_eq!(header, vec![b"h1".to_vec(), b"h2".to_vec(), b"h3".to_vec()]);
}

#[test]
fn custom_dialect_roundtrip() {
    let data = &b"x;\"a\r\nb\";1\r\ny;;2\r\nz;'q';3"[..];
    let mut dialect = Dialect::default();
    dialect.delimiter = b';';
    dialect.has_headers = false;
    let (idxbytes, meta) = build_index(data, &dialect, 2);

    let idx = SparseIndex::open(&idxbytes[..], &meta, &dialect).unwrap();
    let expected = sequential_records(data, &dialect);
    let mut ra = SparseRandomAccess::from_reader(io::Cursor::new(data), idx);
    for k in [2, 0, 1] {
        let got = ra.get(k).unwrap().unwrap();
        assert_eq!(&got, &expected[k as usize]);
    }

    // The same index opened with the default dialect (comma delimiter)
    // must be rejected as stale.
    assert!(
        SparseIndex::open(&idxbytes[..], &meta, &Dialect::default()).is_err()
    );
}

#[test]
fn empty_data() {
    let data = &b""[..];
    let dialect = Dialect::default();
    let (idxbytes, meta) = build_index(data, &dialect, 2);
    let idx = SparseIndex::open(&idxbytes[..], &meta, &dialect).unwrap();
    assert_eq!(idx.len(), 0);
    assert!(idx.is_empty());
    let mut ra = SparseRandomAccess::from_reader(io::Cursor::new(data), idx);
    assert!(ra.get(0).unwrap().is_none());
}

#[test]
fn truncated_index_is_rejected() {
    let data = sample_data();
    let dialect = Dialect::default();
    let (idxbytes, meta) = build_index(&data, &dialect, 2);
    let cuts = [
        0,
        4,
        8,
        20,
        HEADER_LEN - 1,
        HEADER_LEN,
        HEADER_LEN + 5,
        HEADER_LEN + CHECKPOINT_LEN,
        idxbytes.len() - 1,
    ];
    for &cut in &cuts {
        assert!(
            SparseIndex::open(&idxbytes[..cut], &meta, &dialect).is_err(),
            "truncated index of {} bytes must be rejected",
            cut
        );
    }
    // Sanity check: the full index opens fine.
    assert!(SparseIndex::open(&idxbytes[..], &meta, &dialect).is_ok());
}

#[test]
fn record_number_backwards_is_rejected() {
    let data = sample_data();
    let dialect = Dialect::default();
    let (idxbytes, meta) = build_index(&data, &dialect, 2);
    assert!(idxbytes.len() >= HEADER_LEN + 2 * CHECKPOINT_LEN);

    // Rewind the second checkpoint from record 2 back to record 0.
    let mut bad = idxbytes.clone();
    put_u64(&mut bad, HEADER_LEN + CHECKPOINT_LEN, 0);
    assert!(SparseIndex::open(&bad[..], &meta, &dialect).is_err());

    // A record number that is not a multiple of the interval is corrupt.
    let mut bad = idxbytes.clone();
    put_u64(&mut bad, HEADER_LEN + CHECKPOINT_LEN, 3);
    assert!(SparseIndex::open(&bad[..], &meta, &dialect).is_err());

    // A record number beyond the total record count is corrupt.
    let mut bad = idxbytes.clone();
    put_u64(&mut bad, HEADER_LEN + CHECKPOINT_LEN, 100);
    assert!(SparseIndex::open(&bad[..], &meta, &dialect).is_err());
}

#[test]
fn changed_file_same_size_is_rejected() {
    let data = sample_data();
    let dialect = Dialect::default();
    let (idxbytes, meta) = build_index(&data, &dialect, 2);

    // Modify the data in place without changing its length.
    let mut changed = data.clone();
    assert_eq!(changed[0], b'h');
    changed[0] = b'X';
    let meta2 = SourceMeta::from_reader(&changed[..]).unwrap();
    assert_eq!(meta.len(), meta2.len());
    assert!(SparseIndex::open(&idxbytes[..], &meta2, &dialect).is_err());

    // A different length with the same digest is also rejected.
    let meta3 = SourceMeta::new(meta.len() + 1, meta.digest());
    assert!(SparseIndex::open(&idxbytes[..], &meta3, &dialect).is_err());
    let meta4 = SourceMeta::new(meta.len() - 1, meta.digest());
    assert!(SparseIndex::open(&idxbytes[..], &meta4, &dialect).is_err());
}

#[test]
fn changed_dialect_is_rejected() {
    let data = sample_data();
    let dialect = Dialect::default();
    let (idxbytes, meta) = build_index(&data, &dialect, 2);

    let mut d = dialect;
    d.delimiter = b';';
    assert!(SparseIndex::open(&idxbytes[..], &meta, &d).is_err());

    let mut d = dialect;
    d.quote = 0x27;
    assert!(SparseIndex::open(&idxbytes[..], &meta, &d).is_err());

    let mut d = dialect;
    d.escape = Some(0x5C);
    assert!(SparseIndex::open(&idxbytes[..], &meta, &d).is_err());

    let mut d = dialect;
    d.double_quote = false;
    assert!(SparseIndex::open(&idxbytes[..], &meta, &d).is_err());

    let mut d = dialect;
    d.quoting = false;
    assert!(SparseIndex::open(&idxbytes[..], &meta, &d).is_err());

    let mut d = dialect;
    d.has_headers = false;
    assert!(SparseIndex::open(&idxbytes[..], &meta, &d).is_err());

    let mut d = dialect;
    d.flexible = true;
    assert!(SparseIndex::open(&idxbytes[..], &meta, &d).is_err());

    let mut d = dialect;
    d.comment = Some(b'#');
    assert!(SparseIndex::open(&idxbytes[..], &meta, &d).is_err());
}

#[test]
fn illegal_checkpoint_offset_is_rejected() {
    let data = sample_data();
    let dialect = Dialect::default();
    let (idxbytes, meta) = build_index(&data, &dialect, 2);

    // A checkpoint offset beyond the end of the source data.
    let mut bad = idxbytes.clone();
    put_u64(&mut bad, HEADER_LEN + CHECKPOINT_LEN + 8, meta.len() + 1000);
    assert!(SparseIndex::open(&bad[..], &meta, &dialect).is_err());

    // A checkpoint offset exactly at the end of the source data: no record
    // can start there.
    let mut bad = idxbytes.clone();
    put_u64(&mut bad, HEADER_LEN + CHECKPOINT_LEN + 8, meta.len());
    assert!(SparseIndex::open(&bad[..], &meta, &dialect).is_err());

    // A checkpoint offset that moves backwards relative to the previous
    // checkpoint.
    let mut bad = idxbytes.clone();
    put_u64(&mut bad, HEADER_LEN + 2 * CHECKPOINT_LEN + 8, 1);
    assert!(SparseIndex::open(&bad[..], &meta, &dialect).is_err());
}

#[test]
fn bad_magic_and_version_are_rejected() {
    let data = sample_data();
    let dialect = Dialect::default();
    let (idxbytes, meta) = build_index(&data, &dialect, 2);

    let mut bad = idxbytes.clone();
    bad[0] = b'X';
    assert!(SparseIndex::open(&bad[..], &meta, &dialect).is_err());

    let mut bad = idxbytes.clone();
    bad[11] = 0xFF; // version is the u32 at offset 8
    assert!(SparseIndex::open(&bad[..], &meta, &dialect).is_err());
}
