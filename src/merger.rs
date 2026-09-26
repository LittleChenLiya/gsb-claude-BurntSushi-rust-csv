/*!
Stream multiple CSV inputs into one output, aligning fields by header name.

The main type is [HeaderMerger]. It reads the header of every input up
front, computes a merged output header (the first input's column order wins,
columns first seen in later inputs are appended in order of first
appearance) and then streams records one at a time: inputs are drained in
order and every record is projected onto the merged header before being
written. Missing columns are emitted as empty fields.

Column names are compared as raw bytes (see [ByteRecord]): no UTF-8
validation is performed and no whitespace is trimmed. Nothing is buffered
beyond the current record, so inputs of any size can be merged. No output
is written until every header has been validated successfully.

If an error occurs while data records are being written, the error reports
which input failed and where, but any output that was already written is
left as-is: the written prefix is *not* rolled back automatically.
*/

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::io;

use crate::{ByteRecord, ErrorKind, Position, Reader, ReaderBuilder, Writer};

/// An error that can occur while merging CSV inputs by header name.
///
/// Every variant that occurs after the header phase identifies the failing
/// input by its 0-based index (in the order inputs were added) and the
/// position of the offending record. Note that any output written before
/// the error is left untouched; it is not rolled back.
#[derive(Debug)]
pub enum MergeError {
    /// An input had no header record at all (e.g. it was empty).
    MissingHeader {
        /// The 0-based index of the offending input.
        input: usize,
    },
    /// A single input's header contained the same column name twice.
    ///
    /// Column names are compared as raw bytes; the offending name is
    /// reported as bytes because it is not required to be valid UTF-8.
    DuplicateHeader {
        /// The 0-based index of the offending input.
        input: usize,
        /// The raw bytes of the duplicated column name.
        name: Vec<u8>,
    },
    /// A data record had a different number of fields than its input's
    /// header (this covers both short records and records with extra
    /// fields).
    UnequalLengths {
        /// The 0-based index of the offending input.
        input: usize,
        /// The position of the bad record, if available.
        position: Option<Position>,
        /// The number of fields expected (the input's header length).
        expected_len: u64,
        /// The number of fields the record actually had.
        len: u64,
    },
    /// An underlying CSV error occurred while reading a data record.
    Read {
        /// The 0-based index of the failing input.
        input: usize,
        /// The 0-based index of the record that was being read when the
        /// error occurred (i.e. the number of data records already read
        /// successfully from this input).
        record: u64,
        /// The underlying CSV error.
        source: crate::Error,
    },
    /// An error occurred while writing to the output.
    Write(crate::Error),
}

impl fmt::Display for MergeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            MergeError::MissingHeader { input } => write!(
                f,
                "input {} has no header record (is it empty?)",
                input
            ),
            MergeError::DuplicateHeader { input, ref name } => write!(
                f,
                "input {} has a duplicate column name: {:?}",
                input,
                String::from_utf8_lossy(name)
            ),
            MergeError::UnequalLengths {
                input,
                ref position,
                expected_len,
                len,
            } => {
                write!(
                    f,
                    "input {}: record has {} fields but its header has {}",
                    input, len, expected_len
                )?;
                if let Some(pos) = position {
                    write!(
                        f,
                        " (record {}, line {}, byte {})",
                        pos.record(),
                        pos.line(),
                        pos.byte()
                    )?;
                }
                write!(f, "; any output already written is not rolled back")
            }
            MergeError::Read { input, record, ref source } => write!(
                f,
                "input {}: failed to read data record {}: {}; \
                 any output already written is not rolled back",
                input, record, source
            ),
            MergeError::Write(ref err) => {
                write!(f, "failed to write merged output: {}", err)
            }
        }
    }
}

impl StdError for MergeError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match *self {
            MergeError::Read { ref source, .. } => Some(source),
            MergeError::Write(ref err) => Some(err),
            _ => None,
        }
    }
}

/// Merges several CSV readers into a single output, aligning fields by
/// header name.
///
/// # How it works
///
/// 1. Inputs are added with [HeaderMerger::add] or
///    [HeaderMerger::add_reader].
/// 2. [HeaderMerger::prepare] reads and validates the header of every
///    input and computes the merged output header: the first input's
///    column order is kept, and columns first seen in later inputs are
///    appended in order of first appearance. Duplicate column names within
///    a single input are rejected. No output is written and no data
///    records are read during this phase.
/// 3. [HeaderMerger::write_header] and [HeaderMerger::write_next] (or
///    the convenience [HeaderMerger::write_all]) stream the data: inputs
///    are drained in order, one record at a time, and each record is
///    projected onto the merged header. Columns missing from an input are
///    written as empty fields.
///
/// Column names are compared as raw bytes. They are not required to be
/// valid UTF-8 and leading/trailing whitespace is significant.
///
/// Records are never accumulated in memory; only the current record and
/// the per-input column layouts are kept.
///
/// # Errors
///
/// If reading or writing fails while data records are being emitted, the
/// error reports the 0-based input index and the record position. Any
/// output already written to the underlying writer is left as-is and is
/// **not** rolled back.
///
/// # Example
///
/// ~~~no_run
/// use std::error::Error;
///
/// fn main() -> Result<(), Box<dyn Error>> {
///     let mut merger = csv::HeaderMerger::new();
///     merger.add_reader("id,name\n1,alice\n".as_bytes());
///     merger.add_reader("age,id\n30,2\n".as_bytes());
///     merger.prepare()?;
///
///     let mut wtr = csv::Writer::from_writer(std::io::stdout());
///     merger.write_all(&mut wtr)?;
///     wtr.flush()?;
///     Ok(())
/// }
/// ~~~
pub struct HeaderMerger {
    readers: Vec<Reader<Box<dyn io::Read>>>,
    /// Merged output header, computed by prepare.
    headers: Option<ByteRecord>,
    /// For each input, a map from that input's field index to the merged
    /// output column index.
    layouts: Vec<Vec<usize>>,
    /// Index of the input currently being drained.
    current: usize,
    /// Number of data records read from the current input so far.
    current_record: u64,
    /// Reusable buffer for the record being read.
    scratch: ByteRecord,
    /// Reusable buffer for the projected output record.
    projected: ByteRecord,
    /// Whether the merged header has been written yet.
    wrote_header: bool,
    /// Total number of data records written.
    records_written: u64,
}

impl HeaderMerger {
    /// Create a new merger with no inputs.
    pub fn new() -> HeaderMerger {
        HeaderMerger {
            readers: vec![],
            headers: None,
            layouts: vec![],
            current: 0,
            current_record: 0,
            scratch: ByteRecord::new(),
            projected: ByteRecord::new(),
            wrote_header: false,
            records_written: 0,
        }
    }

    /// Add an input CSV reader.
    ///
    /// The reader must not have read any records yet: its first record is
    /// treated as the header. Use [ReaderBuilder] to customize the
    /// reader (delimiter, quoting, ...) and box the underlying source,
    /// e.g. ReaderBuilder::new().from_reader(Box::new(file)).
    ///
    /// Inputs are merged in the order they are added.
    pub fn add(&mut self, rdr: Reader<Box<dyn io::Read>>) -> &mut Self {
        self.readers.push(rdr);
        self
    }

    /// Add an input from anything that implements io::Read, using the
    /// default reader configuration.
    ///
    /// This is a convenience wrapper around [HeaderMerger::add].
    pub fn add_reader<R: io::Read + 'static>(&mut self, rdr: R) -> &mut Self {
        let boxed: Box<dyn io::Read> = Box::new(rdr);
        self.add(ReaderBuilder::new().from_reader(boxed))
    }

    /// Read and validate the header of every input and compute the merged
    /// output header.
    ///
    /// This must be called before any output is written. It reads only
    /// headers: no data records are consumed and nothing is written to the
    /// output during this phase.
    ///
    /// # Errors
    ///
    /// Returns [MergeError::MissingHeader] if an input has no header
    /// record, [MergeError::DuplicateHeader] if a single input repeats a
    /// column name, and [MergeError::Read] if a header cannot be read.
    pub fn prepare(&mut self) -> Result<&ByteRecord, MergeError> {
        let mut merged = ByteRecord::new();
        let mut columns: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut layouts = Vec::with_capacity(self.readers.len());
        for (input, rdr) in self.readers.iter_mut().enumerate() {
            let header = rdr.byte_headers().map_err(|err| {
                MergeError::Read { input, record: 0, source: err }
            })?;
            if header.is_empty() {
                return Err(MergeError::MissingHeader { input });
            }
            let mut layout = Vec::with_capacity(header.len());
            for field in header.iter() {
                match columns.get(field) {
                    Some(&index) => {
                        if layout.contains(&index) {
                            return Err(MergeError::DuplicateHeader {
                                input,
                                name: field.to_vec(),
                            });
                        }
                        layout.push(index);
                    }
                    None => {
                        let index = merged.len();
                        merged.push_field(field);
                        columns.insert(field.to_vec(), index);
                        layout.push(index);
                    }
                }
            }
            layouts.push(layout);
        }
        self.layouts = layouts;
        self.headers = Some(merged);
        Ok(self.headers.as_ref().unwrap())
    }

    /// Return the merged output header, if prepare has completed.
    pub fn headers(&self) -> Option<&ByteRecord> {
        self.headers.as_ref()
    }

    /// Return the total number of data records written so far.
    pub fn records_written(&self) -> u64 {
        self.records_written
    }

    /// Write the merged header record to the output.
    ///
    /// This is called automatically by [HeaderMerger::write_all] and by
    /// the first [HeaderMerger::write_next] call, so it only needs to be
    /// called explicitly to interleave other output. Calling it more than
    /// once has no effect.
    pub fn write_header<W: io::Write>(
        &mut self,
        wtr: &mut Writer<W>,
    ) -> Result<(), MergeError> {
        if self.wrote_header {
            return Ok(());
        }
        let headers = self
            .headers
            .as_ref()
            .expect("prepare must be called before writing merged output");
        wtr.write_byte_record(headers).map_err(MergeError::Write)?;
        self.wrote_header = true;
        Ok(())
    }

    /// Read the next data record from the inputs and write it, projected
    /// onto the merged header, to the output.
    ///
    /// Returns Ok(true) if a record was written and Ok(false) when
    /// every input has been exhausted. The merged header is written first
    /// if it hasn't been already.
    ///
    /// # Errors
    ///
    /// The error identifies the failing input and record. Any output
    /// already written is left as-is and is not rolled back.
    pub fn write_next<W: io::Write>(
        &mut self,
        wtr: &mut Writer<W>,
    ) -> Result<bool, MergeError> {
        self.write_header(wtr)?;
        loop {
            if self.current >= self.readers.len() {
                return Ok(false);
            }
            let input = self.current;
            let result =
                self.readers[input].read_byte_record(&mut self.scratch);
            let got_record = result.map_err(|err| match err.into_kind() {
                ErrorKind::UnequalLengths { pos, expected_len, len } => {
                    MergeError::UnequalLengths {
                        input,
                        position: pos,
                        expected_len,
                        len,
                    }
                }
                kind => MergeError::Read {
                    input,
                    record: self.current_record,
                    source: crate::Error::new(kind),
                },
            })?;
            if !got_record {
                self.current += 1;
                self.current_record = 0;
                continue;
            }
            self.current_record += 1;
            let layout = &self.layouts[input];
            let width = self.headers.as_ref().unwrap().len();
            self.projected.clear();
            for out in 0..width {
                match layout.iter().position(|&index| index == out) {
                    Some(field) => {
                        self.projected.push_field(&self.scratch[field])
                    }
                    None => self.projected.push_field(b""),
                }
            }
            wtr.write_byte_record(&self.projected)
                .map_err(MergeError::Write)?;
            self.records_written += 1;
            return Ok(true);
        }
    }

    /// Write the merged header and then stream every data record from all
    /// inputs to the output.
    ///
    /// Returns the number of data records written. This does not flush the
    /// writer; call flush on it if needed.
    ///
    /// # Errors
    ///
    /// The error identifies the failing input and record. Any output
    /// already written is left as-is and is not rolled back.
    pub fn write_all<W: io::Write>(
        &mut self,
        wtr: &mut Writer<W>,
    ) -> Result<u64, MergeError> {
        while self.write_next(wtr)? {}
        Ok(self.records_written)
    }
}
