use std::io;

use csv::ReaderBuilder;
use csv_index::sparse::{
    ErrorKind, StaleError, CHECKPOINT_LEN, HEADER_LEN, OFF_CP_BYTE,
    OFF_CP_RECORD, OFF_DIALECT,
};
use csv_index::SparseIndex;

/// Read every record sequentially, collecting the field bytes and the
/// position reported for each record.
fn sequential(
    data: &[u8],
    builder: &ReaderBuilder,
) -> Vec<(Vec<Vec<u8>>, csv::Position)> {
    let mut rdr = builder.from_reader(io::Cursor::new(data));
    let mut out = vec![];
    let mut record = csv::ByteRecord::new();
    while rdr.read_byte_record(&mut record).unwrap() {
        let fields =
            record.iter().map(|f| f.to_vec()).collect::<Vec<Vec<u8>>>();
        let pos = record.position().unwrap().clone();
        out.push((fields, pos));
    }
    out
}

fn default_builder() -> ReaderBuilder {
    let mut b = ReaderBuilder::new();
    b.has_headers(false);
    // The test data deliberately mixes 2 and 3 field records.
    b.flexible(true);
    b
}

/// Build an index of `data` in memory, recording every `step`th record.
fn build_index(data: &[u8], step: u64) -> io::Cursor<Vec<u8>> {
    let mut idxbuf = io::Cursor::new(vec![]);
    let mut rdr = default_builder().from_reader(io::Cursor::new(data));
    SparseIndex::create(&mut rdr, step, &mut idxbuf).unwrap();
    idxbuf
}

/// Data exercising CRLF terminators, a quoted field spanning a CRLF line
/// break, a quoted field spanning an LF line break, and several empty
/// fields.
fn tricky_data() -> &'static [u8] {
    concat!(
        "a,b\r\n",                 // record 0
        "\"x\r\ny\",,\"z,z\"\r\n", // record 1 (embedded CRLF, empty field)
        ",\"q\nq\",\n",            // record 2 (empty fields + embedded LF)
        "c,d\r\n",                 // record 3
        "e,\n",                    // record 4 (trailing empty field)
    )
    .as_bytes()
}

#[test]
fn out_of_order_reads_match_sequential_byte_for_byte() {
    let data = tricky_data();
    let expected = sequential(data, &default_builder());
    assert_eq!(expected.len(), 5);

    let idxbuf = build_index(data, 2);
    let mut idx = SparseIndex::open(idxbuf).unwrap();
    assert_eq!(idx.len(), 5);
    assert_eq!(idx.step(), 2);
    // Records 0, 2 and 4 are checkpoints.
    assert_eq!(idx.checkpoint_len(), 3);
    assert_eq!(idx.source_len(), data.len() as u64);
    assert!(!idx.is_empty());

    // A deliberately out-of-order traversal. Every record is reached from
    // a *fresh* reader via its nearest checkpoint.
    for k in [4u64, 0, 3, 1, 2, 0, 4, 2, 1, 3] {
        let mut rdr = default_builder().from_reader(io::Cursor::new(data));
        idx.verify(&mut rdr).unwrap();

        let (pos, skip) = idx.get(k).unwrap();
        assert_eq!(skip, k % 2);
        assert_eq!(pos.record(), k - k % 2);

        idx.seek_record(&mut rdr, k).unwrap();
        let mut record = csv::ByteRecord::new();
        assert!(rdr.read_byte_record(&mut record).unwrap());

        let (fields, want_pos) = &expected[k as usize];
        let got_fields = record.iter().map(|f| f.to_vec()).collect::<Vec<_>>();
        // The recovered record must be byte-for-byte identical.
        assert_eq!(&got_fields, fields, "record {} fields differ", k);

        let got_pos = record.position().unwrap();
        assert_eq!(got_pos, want_pos, "record {} position differs", k);
    }
}

#[test]
fn recovery_crosses_quoted_line_breaks_and_continues() {
    let data = tricky_data();
    let expected = sequential(data, &default_builder());
    let idxbuf = build_index(data, 2);
    let mut idx = SparseIndex::open(idxbuf).unwrap();

    // Seek to the checkpoint at record 0, then read everything in order.
    // This forces the parser to cross the quoted CRLF and quoted LF while
    // starting at a checkpoint.
    let mut rdr = default_builder().from_reader(io::Cursor::new(data));
    idx.verify(&mut rdr).unwrap();
    idx.seek_record(&mut rdr, 0).unwrap();
    let mut record = csv::ByteRecord::new();
    for (k, (fields, pos)) in expected.iter().enumerate() {
        assert!(rdr.read_byte_record(&mut record).unwrap(), "record {}", k);
        let got = record.iter().map(|f| f.to_vec()).collect::<Vec<_>>();
        assert_eq!(&got, fields, "record {}", k);
        assert_eq!(record.position().unwrap(), pos, "record {}", k);
    }
    assert!(!rdr.read_byte_record(&mut record).unwrap());

    // Record 1 starts right after the `a,b\r\n` terminator at byte 4.
    // Logically it is still line 1 (the CRLF is inside the quoted field),
    // and record 2 starts on line 3. Recovery must reproduce exactly that.
    let mut rdr = default_builder().from_reader(io::Cursor::new(data));
    idx.verify(&mut rdr).unwrap();
    idx.seek_record(&mut rdr, 1).unwrap(); // skip 1 from checkpoint 0
    let mut record = csv::ByteRecord::new();
    assert!(rdr.read_byte_record(&mut record).unwrap());
    assert_eq!(
        record.iter().collect::<Vec<_>>(),
        vec![&b"x\r\ny"[..], &b""[..], &b"z,z"[..]]
    );
    assert_eq!(record.position().unwrap().byte(), 4);
    assert_eq!(record.position().unwrap().line(), 1);
    assert_eq!(record.position().unwrap().record(), 1);

    // Seeking to record 2 (itself a checkpoint) then continuing crosses the
    // embedded LF while preserving line bookkeeping.
    let mut rdr = default_builder().from_reader(io::Cursor::new(data));
    idx.verify(&mut rdr).unwrap();
    idx.seek_record(&mut rdr, 2).unwrap();
    let mut record = csv::ByteRecord::new();
    assert!(rdr.read_byte_record(&mut record).unwrap());
    assert_eq!(
        record.iter().collect::<Vec<_>>(),
        vec![&b""[..], &b"q\nq"[..], &b""[..]]
    );
    assert!(rdr.read_byte_record(&mut record).unwrap());
    assert_eq!(record.iter().collect::<Vec<_>>(), vec![&b"c"[..], &b"d"[..]]);
}

#[test]
fn headers_are_indexed_as_record_zero() {
    let data = tricky_data();
    let mut builder = ReaderBuilder::new();
    builder.has_headers(true).flexible(true);

    let mut idxbuf = io::Cursor::new(vec![]);
    {
        let mut rdr = builder.from_reader(io::Cursor::new(data));
        SparseIndex::create(&mut rdr, 2, &mut idxbuf).unwrap();
    }
    let mut idx = SparseIndex::open(idxbuf).unwrap();
    // All five physical rows are indexed, including the header row.
    assert_eq!(idx.len(), 5);

    // After a seek, header skipping is disabled, so record 0 *is* the
    // header row.
    let mut rdr = builder.from_reader(io::Cursor::new(data));
    idx.verify(&mut rdr).unwrap();
    idx.seek_record(&mut rdr, 0).unwrap();
    let mut record = csv::ByteRecord::new();
    assert!(rdr.read_byte_record(&mut record).unwrap());
    assert_eq!(record.iter().collect::<Vec<_>>(), vec![&b"a"[..], &b"b"[..]]);

    // Record 1 (first data row) is reachable by skipping one record from
    // checkpoint 0.
    let mut rdr = builder.from_reader(io::Cursor::new(data));
    idx.verify(&mut rdr).unwrap();
    idx.seek_record(&mut rdr, 1).unwrap();
    let mut record = csv::ByteRecord::new();
    assert!(rdr.read_byte_record(&mut record).unwrap());
    assert_eq!(
        record.iter().collect::<Vec<_>>(),
        vec![&b"x\r\ny"[..], &b""[..], &b"z,z"[..]]
    );
}

#[test]
fn empty_source_has_no_checkpoints() {
    let data = &b""[..];
    let idxbuf = build_index(data, 2);
    let mut idx = SparseIndex::open(idxbuf).unwrap();
    assert_eq!(idx.len(), 0);
    assert_eq!(idx.checkpoint_len(), 0);
    assert!(idx.is_empty());
    assert!(idx.get(0).is_err());
}

#[test]
fn step_larger_than_record_count() {
    let data = &b"a,b\nc,d\n"[..];
    let idxbuf = build_index(data, 100);
    let mut idx = SparseIndex::open(idxbuf).unwrap();
    assert_eq!(idx.len(), 2);
    assert_eq!(idx.checkpoint_len(), 1); // only record 0

    let mut rdr = default_builder().from_reader(io::Cursor::new(data));
    idx.verify(&mut rdr).unwrap();
    let (_, skip) = idx.get(1).unwrap();
    assert_eq!(skip, 1);
    idx.seek_record(&mut rdr, 1).unwrap();
    let mut record = csv::ByteRecord::new();
    assert!(rdr.read_byte_record(&mut record).unwrap());
    assert_eq!(record.iter().collect::<Vec<_>>(), vec![&b"c"[..], &b"d"[..]]);
}

#[test]
fn create_rejects_zero_step() {
    let data = &b"a\n"[..];
    let mut idxbuf = io::Cursor::new(vec![]);
    let mut rdr = default_builder().from_reader(io::Cursor::new(data));
    let err = SparseIndex::create(&mut rdr, 0, &mut idxbuf).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Corrupt(_)));
}

#[test]
fn verify_rejects_different_dialects() {
    let data = tricky_data();
    let idxbuf = build_index(data, 2);
    let mut idx = SparseIndex::open(idxbuf).unwrap();

    // A different delimiter invalidates the index.
    let mut b = ReaderBuilder::new();
    b.has_headers(false).flexible(true).delimiter(b';');
    let mut rdr = b.from_reader(io::Cursor::new(data));
    let err = idx.verify(&mut rdr).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Stale(StaleError::Dialect)));

    // A different quote character invalidates it too.
    let mut b = ReaderBuilder::new();
    b.has_headers(false).flexible(true).quote(b'\'');
    let mut rdr = b.from_reader(io::Cursor::new(data));
    let err = idx.verify(&mut rdr).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Stale(StaleError::Dialect)));

    // ...and the stored dialect fingerprint itself must record the quote.
    let mut idxbuf2 = io::Cursor::new(vec![]);
    {
        let mut b = ReaderBuilder::new();
        b.has_headers(false).flexible(true).quote(b'\'');
        let mut rdr = b.from_reader(io::Cursor::new(data));
        SparseIndex::create(&mut rdr, 2, &mut idxbuf2).unwrap();
    }
    let buf1 = idx.into_inner().into_inner();
    let buf2 = idxbuf2.into_inner();
    assert_ne!(
        &buf1[OFF_DIALECT as usize..OFF_DIALECT as usize + 32],
        &buf2[OFF_DIALECT as usize..OFF_DIALECT as usize + 32],
    );
}

#[test]
fn verify_rejects_source_size_change() {
    let data = tricky_data();
    let idxbuf = build_index(data, 2);
    let mut idx = SparseIndex::open(idxbuf).unwrap();

    // The file was truncated: size differs.
    let changed = &data[..data.len() - 3];
    let mut rdr = default_builder().from_reader(io::Cursor::new(changed));
    let err = idx.verify(&mut rdr).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Stale(StaleError::SourceSize)));
}

#[test]
fn verify_rejects_same_size_content_change() {
    let data = tricky_data();
    let idxbuf = build_index(data, 2);
    let mut idx = SparseIndex::open(idxbuf).unwrap();

    // Flip one byte without changing the length. The size check must not
    // mask this: the digest has to catch it.
    let mut changed = data.to_vec();
    changed[0] = if changed[0] == b'a' { b'B' } else { b'a' };
    assert_eq!(changed.len(), data.len());
    assert_ne!(changed.as_slice(), data);

    let mut rdr =
        default_builder().from_reader(io::Cursor::new(changed.as_slice()));
    let err = idx.verify(&mut rdr).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Stale(StaleError::SourceDigest)));
}

// ---- Corruption tests -------------------------------------------------

fn corrupt_index(patch: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let data = tricky_data();
    let idxbuf = build_index(data, 2);
    let mut buf = idxbuf.into_inner();
    patch(&mut buf);
    buf
}

#[test]
fn corrupt_truncated_index_is_rejected() {
    // Truncated to a few bytes: not even a full header.
    let mut buf = corrupt_index(|b| b.truncate(10));
    let err = SparseIndex::open(io::Cursor::new(&mut buf)).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Corrupt(_)));

    // Header intact but checkpoint table cut in the middle of an entry.
    let want_len = HEADER_LEN as usize + CHECKPOINT_LEN as usize + 7;
    let mut buf = corrupt_index(|b| b.truncate(want_len));
    let err = SparseIndex::open(io::Cursor::new(&mut buf)).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Corrupt(_)));

    // Header intact but the whole table missing despite the count field.
    let mut buf = corrupt_index(|b| b.truncate(HEADER_LEN as usize));
    let err = SparseIndex::open(io::Cursor::new(&mut buf)).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Corrupt(_)));
}

#[test]
fn corrupt_bad_magic_is_rejected() {
    let mut buf = corrupt_index(|b| b[0] = b'X');
    let err = SparseIndex::open(io::Cursor::new(&mut buf)).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Corrupt(_)));
}

#[test]
fn corrupt_record_number_regression_is_rejected() {
    // With step 2 the second checkpoint (index #1) must point at record 2.
    // Rewrite it to record 0, i.e. a regression that duplicates the first
    // checkpoint's record number.
    let mut buf = corrupt_index(|b| {
        let at = HEADER_LEN as usize
            + 1 * CHECKPOINT_LEN as usize
            + OFF_CP_RECORD as usize;
        b[at..at + 8].copy_from_slice(&0u64.to_be_bytes());
    });
    let err = SparseIndex::open(io::Cursor::new(&mut buf)).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Corrupt(_)));

    // A regression in the *last* checkpoint (record 4 -> record 1) is
    // caught just the same.
    let mut buf = corrupt_index(|b| {
        let at = HEADER_LEN as usize
            + 2 * CHECKPOINT_LEN as usize
            + OFF_CP_RECORD as usize;
        b[at..at + 8].copy_from_slice(&1u64.to_be_bytes());
    });
    let err = SparseIndex::open(io::Cursor::new(&mut buf)).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Corrupt(_)));

    // Checkpoint byte offsets must strictly advance too.
    let mut buf = corrupt_index(|b| {
        let at = HEADER_LEN as usize + 1 * CHECKPOINT_LEN as usize + 0;
        b[at + OFF_CP_BYTE as usize..at + OFF_CP_BYTE as usize + 8]
            .copy_from_slice(&0u64.to_be_bytes());
    });
    let err = SparseIndex::open(io::Cursor::new(&mut buf)).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Corrupt(_)));
}

#[test]
fn corrupt_illegal_checkpoint_offset_is_rejected() {
    // Point the first checkpoint past the end of the source file.
    let mut buf = corrupt_index(|b| {
        let at = HEADER_LEN as usize + OFF_CP_BYTE as usize;
        b[at..at + 8].copy_from_slice(&u64::MAX.to_be_bytes());
    });
    let err = SparseIndex::open(io::Cursor::new(&mut buf)).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Corrupt(_)));

    // An offset exactly equal to the source length is also out of range.
    let data = tricky_data();
    let mut buf = corrupt_index(|b| {
        let at = HEADER_LEN as usize + OFF_CP_BYTE as usize;
        b[at..at + 8].copy_from_slice(&(data.len() as u64).to_be_bytes());
    });
    let err = SparseIndex::open(io::Cursor::new(&mut buf)).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Corrupt(_)));
}

#[test]
fn corrupt_checkpoint_count_mismatch_is_rejected() {
    // Claim an extra checkpoint without adding its 24 bytes.
    let mut buf = corrupt_index(|b| {
        b[24..32].copy_from_slice(&99u64.to_be_bytes());
    });
    let err = SparseIndex::open(io::Cursor::new(&mut buf)).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Corrupt(_)));
}

#[test]
fn index_is_stable_on_disk() {
    // Rebuilding the same index for the same data/dialect must produce
    // byte-identical output.
    let data = tricky_data();
    let buf1 = build_index(data, 2).into_inner();
    let buf2 = build_index(data, 2).into_inner();
    assert_eq!(buf1, buf2);
    assert_eq!(buf1.len() as u64, HEADER_LEN + 3 * CHECKPOINT_LEN);
}

#[test]
fn last_record_without_trailing_terminator() {
    // The final record lacks a line terminator. Its checkpoint must be
    // accepted even though it points close to the end of the file, and
    // recovery must return its exact bytes.
    let data = &b"a,b\nc,d\ne,f"[..];
    let expected = sequential(data, &default_builder());
    let idxbuf = build_index(data, 2);
    let mut idx = SparseIndex::open(idxbuf).unwrap();
    assert_eq!(idx.len(), 3);
    assert_eq!(idx.checkpoint_len(), 2); // records 0 and 2

    for k in [0u64, 1, 2] {
        let mut rdr = default_builder().from_reader(io::Cursor::new(data));
        idx.verify(&mut rdr).unwrap();
        idx.seek_record(&mut rdr, k).unwrap();
        let mut record = csv::ByteRecord::new();
        assert!(rdr.read_byte_record(&mut record).unwrap());
        let got = record.iter().map(|f| f.to_vec()).collect::<Vec<_>>();
        assert_eq!(&got, &expected[k as usize].0);
        assert_eq!(record.position().unwrap(), &expected[k as usize].1);
    }
}

#[test]
fn non_default_dialect_roundtrip() {
    // Semicolon delimiter, single-quote quote, LF terminator and an
    // embedded LF inside a quoted field.
    let data = &b"a;b;c\n'x\ny';'';z\nq;;w\n"[..];
    let mut builder = ReaderBuilder::new();
    builder
        .has_headers(false)
        .flexible(true)
        .delimiter(b';')
        .quote(b'\'')
        .terminator(csv::Terminator::Any(b'\n'));
    let expected = sequential(data, &builder);

    let mut idxbuf = io::Cursor::new(vec![]);
    {
        let mut rdr = builder.from_reader(io::Cursor::new(data));
        SparseIndex::create(&mut rdr, 2, &mut idxbuf).unwrap();
    }
    let mut idx = SparseIndex::open(idxbuf).unwrap();
    assert_eq!(idx.len(), 3);

    for k in [2u64, 0, 1] {
        let mut rdr = builder.from_reader(io::Cursor::new(data));
        idx.verify(&mut rdr).unwrap();
        idx.seek_record(&mut rdr, k).unwrap();
        let mut record = csv::ByteRecord::new();
        assert!(rdr.read_byte_record(&mut record).unwrap());
        let got = record.iter().map(|f| f.to_vec()).collect::<Vec<_>>();
        assert_eq!(&got, &expected[k as usize].0, "record {}", k);
        assert_eq!(
            record.position().unwrap(),
            &expected[k as usize].1,
            "record {}",
            k
        );
    }

    // The default dialect must not validate against this index.
    let mut rdr = default_builder().from_reader(io::Cursor::new(data));
    let err = idx.verify(&mut rdr).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Stale(StaleError::Dialect)));
}

#[test]
fn get_out_of_range_errors() {
    let data = &b"a\nb\n"[..];
    let idxbuf = build_index(data, 2);
    let mut idx = SparseIndex::open(idxbuf).unwrap();
    assert!(idx.get(2).is_err());
    assert!(idx.get(u64::MAX).is_err());
}
