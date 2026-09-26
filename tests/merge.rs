//! Tests for header-aligned streaming merges (csv::HeaderMerger).

use std::cell::Cell;
use std::io::{self, Read};
use std::rc::Rc;

use csv::{HeaderMerger, MergeError, Writer};

/// A reader that counts how many bytes have been read from it.
struct CountingReader<R> {
    inner: R,
    count: Rc<Cell<usize>>,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.count.set(self.count.get() + n);
        Ok(n)
    }
}

/// A reader that fails with an IO error after yielding a fixed number of
/// bytes.
struct FailAfter {
    inner: io::Cursor<Vec<u8>>,
    remaining: usize,
}

impl Read for FailAfter {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "simulated read failure",
            ));
        }
        let take = self.remaining.min(buf.len());
        let n = self.inner.read(&mut buf[..take])?;
        self.remaining -= n;
        Ok(n)
    }
}

fn merge_to_vec(merger: &mut HeaderMerger) -> Vec<u8> {
    merger.prepare().unwrap();
    let mut wtr = Writer::from_writer(vec![]);
    merger.write_all(&mut wtr).unwrap();
    wtr.into_inner().unwrap()
}

#[test]
fn merge_aligns_columns_by_name() {
    let mut merger = HeaderMerger::new();
    merger.add_reader("id,name\n1,alice\n2,bob\n".as_bytes());
    merger.add_reader("age,id\n30,3\n".as_bytes());

    merger.prepare().unwrap();
    // First input's order wins; new columns are appended in order of
    // first appearance.
    let fields: Vec<&[u8]> = merger.headers().unwrap().iter().collect();
    let expected: Vec<&[u8]> = vec![b"id", b"name", b"age"];
    assert_eq!(fields, expected);

    let mut wtr = Writer::from_writer(vec![]);
    let n = merger.write_all(&mut wtr).unwrap();
    assert_eq!(n, 3);
    let out = wtr.into_inner().unwrap();
    // Missing columns become empty fields; every value stays in its
    // column.
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "id,name,age\n1,alice,\n2,bob,\n3,,30\n"
    );
}

#[test]
fn duplicate_header_is_an_error() {
    let mut merger = HeaderMerger::new();
    merger.add_reader("id,name\n1,alice\n".as_bytes());
    merger.add_reader("age,id,age\n30,2,31\n".as_bytes());
    match merger.prepare() {
        Err(MergeError::DuplicateHeader { input, name }) => {
            assert_eq!(input, 1);
            assert_eq!(name, b"age");
        }
        other => panic!("expected DuplicateHeader, got {:?}", other),
    }
}

#[test]
fn empty_input_is_an_error() {
    let mut merger = HeaderMerger::new();
    merger.add_reader("id,name\n1,alice\n".as_bytes());
    merger.add_reader("".as_bytes());
    match merger.prepare() {
        Err(MergeError::MissingHeader { input }) => assert_eq!(input, 1),
        other => panic!("expected MissingHeader, got {:?}", other),
    }
}

#[test]
fn non_utf8_column_names_match_by_raw_bytes() {
    // \xff is not valid UTF-8; names must still align by raw bytes.
    let input1: &[u8] = b"id,\xffname\n1,a\n";
    let input2: &[u8] = b"\xffname,age\nb,30\n";
    let mut merger = HeaderMerger::new();
    merger.add_reader(input1);
    merger.add_reader(input2);
    let out = merge_to_vec(&mut merger);
    let expected: &[u8] = b"id,\xffname,age\n1,a,\n,b,30\n";
    assert_eq!(out, expected);
}

#[test]
fn column_names_are_not_trimmed() {
    // "name " (trailing space) is a different column than "name".
    let mut merger = HeaderMerger::new();
    merger.add_reader("id,name\n1,a\n".as_bytes());
    merger.add_reader("name ,id\nx,2\n".as_bytes());
    let out = merge_to_vec(&mut merger);
    assert_eq!(String::from_utf8(out).unwrap(), "id,name,name \n1,a,\n2,,x\n");
}

#[test]
fn quoted_newline_fields_survive_projection() {
    let mut merger = HeaderMerger::new();
    merger.add_reader("id,name\n1,\"a\nb\"\n".as_bytes());
    merger.add_reader("age,id\n30,2\n".as_bytes());
    let out = merge_to_vec(&mut merger);
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "id,name,age\n1,\"a\nb\",\n2,,30\n"
    );
}

#[test]
fn short_record_reports_input_and_position() {
    let mut merger = HeaderMerger::new();
    merger.add_reader("id,name\n1,alice\n".as_bytes());
    merger.add_reader("age,id\n30\n".as_bytes());
    merger.prepare().unwrap();

    let mut wtr = Writer::from_writer(vec![]);
    match merger.write_all(&mut wtr) {
        Err(MergeError::UnequalLengths {
            input,
            position,
            expected_len,
            len,
        }) => {
            assert_eq!(input, 1);
            assert_eq!(expected_len, 2);
            assert_eq!(len, 1);
            let pos = position.expect("position should be available");
            assert_eq!(pos.line(), 2);
            assert_eq!(pos.record(), 1);
        }
        other => panic!("expected UnequalLengths, got {:?}", other),
    }
    // The already-written prefix is left as-is (not rolled back).
    let out = wtr.into_inner().unwrap();
    assert_eq!(String::from_utf8(out).unwrap(), "id,name,age\n1,alice,\n");
}

#[test]
fn extra_fields_record_is_an_error() {
    let mut merger = HeaderMerger::new();
    merger.add_reader("id,name\n1,alice,extra\n".as_bytes());
    merger.prepare().unwrap();

    let mut wtr = Writer::from_writer(vec![]);
    match merger.write_all(&mut wtr) {
        Err(MergeError::UnequalLengths { input, len, .. }) => {
            assert_eq!(input, 0);
            assert_eq!(len, 3);
        }
        other => panic!("expected UnequalLengths, got {:?}", other),
    }
    // Only the header made it out before the failure.
    let out = wtr.into_inner().unwrap();
    assert_eq!(String::from_utf8(out).unwrap(), "id,name\n");
}

#[test]
fn mid_read_failure_reports_input_record_and_prefix() {
    // The second input fails after its header and first data record.
    let input2 = b"age,id\n30,2\n31,3\n".to_vec();
    let fail_at = "age,id\n30,2\n".len();
    let mut merger = HeaderMerger::new();
    merger.add_reader("id,name\n1,alice\n".as_bytes());
    merger.add_reader(FailAfter {
        inner: io::Cursor::new(input2),
        remaining: fail_at,
    });
    merger.prepare().unwrap();

    let mut wtr = Writer::from_writer(vec![]);
    match merger.write_all(&mut wtr) {
        Err(MergeError::Read { input, record, .. }) => {
            assert_eq!(input, 1);
            // One data record was read from input 1 before the failure.
            assert_eq!(record, 1);
        }
        other => panic!("expected Read, got {:?}", other),
    }
    // The prefix written before the failure is intact and not rolled
    // back.
    let out = wtr.into_inner().unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "id,name,age\n1,alice,\n2,,30\n"
    );
}

#[test]
fn headers_are_validated_before_any_output_or_body_reads() {
    // A large first input and a second input with a bad header: prepare
    // must fail without writing output and without draining the first
    // input's body.
    let rows = "1,alice\n".repeat(3000);
    let input1 = format!("id,name\n{}", rows);
    let total = input1.len();
    let count = Rc::new(Cell::new(0usize));
    let rdr1 = CountingReader {
        inner: io::Cursor::new(input1.into_bytes()),
        count: count.clone(),
    };
    let mut merger = HeaderMerger::new();
    merger.add_reader(rdr1);
    merger.add_reader("age,age\n".as_bytes());

    match merger.prepare() {
        Err(MergeError::DuplicateHeader { input, .. }) => {
            assert_eq!(input, 1)
        }
        other => panic!("expected DuplicateHeader, got {:?}", other),
    }
    // The first input's body was not consumed while checking headers.
    assert!(
        count.get() < total,
        "read {} bytes, expected less than {}",
        count.get(),
        total
    );
}

#[test]
fn prepare_does_not_read_the_body() {
    // Streaming proof: with a body far larger than any internal buffer,
    // prepare() only reads a small prefix of the input.
    let rows = "1,alice\n".repeat(3000);
    let input1 = format!("id,name\n{}", rows);
    let total = input1.len();
    let count = Rc::new(Cell::new(0usize));
    let rdr1 = CountingReader {
        inner: io::Cursor::new(input1.into_bytes()),
        count: count.clone(),
    };
    let mut merger = HeaderMerger::new();
    merger.add_reader(rdr1);
    merger.add_reader("age,id\n30,2\n".as_bytes());
    merger.prepare().unwrap();

    let after_prepare = count.get();
    assert!(
        after_prepare < total / 2,
        "prepare read {} of {} body bytes",
        after_prepare,
        total
    );

    // Draining the merge reads the rest.
    let mut wtr = Writer::from_writer(vec![]);
    let n = merger.write_all(&mut wtr).unwrap();
    assert_eq!(n, 3001);
    assert_eq!(count.get(), total);
    drop(wtr);
}
