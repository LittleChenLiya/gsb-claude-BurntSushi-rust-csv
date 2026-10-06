/*!
A sparse on-disk index of safely seekable CSV record positions.

Unlike [`RandomAccessSimple`](crate::RandomAccessSimple), which stores the
byte offset of *every* record, a [`SparseIndex`] stores the complete
[`csv::Position`] (byte offset, line number and record number) of only every
Nth record. The record at index `0` is always indexed. Random access to an
arbitrary record `k` is implemented by seeking the CSV reader to the nearest
preceding checkpoint and then parsing forward the at most `N - 1` records
until record `k` is reached. Because every checkpoint carries the line
number (and not just the byte offset), records containing quoted line breaks
(CRLF or LF inside a quoted field) are crossed correctly and the position
information reported by the CSV reader stays accurate.

Alongside the checkpoint table, the index stores:

* the total size of the indexed CSV data in bytes,
* a 128-bit FNV-1a digest (fingerprint) of the complete CSV data, and
* a 32 byte fingerprint of the complete parsing dialect used to create the
  index (delimiter, terminator, quote, escape, quoting flags, comment,
  trimming, header handling and flexible handling).

An opened index can therefore be rejected with [`ErrorKind::Stale`] if the
source file has changed (even if its size is unchanged, the digest differs)
or if any parsing setting differs.

# On disk format

All integers are big endian. The layout is:

* a fixed 96 byte header, followed by
* a table of 24 byte checkpoint records (`record: u64`, `byte: u64`,
  `line: u64`), sorted by record number.

The current format version is `1` and the format is considered stable.

# Example: random access via a sparse index

```
use std::error::Error;
use std::io;

use csv::ReaderBuilder;
use csv_index::sparse::StaleError;
use csv_index::SparseIndex;

# fn main() { example().unwrap(); }
fn example() -> Result<(), Box<dyn Error>> {
    // Note the CRLF and the line break embedded inside a quoted field: the
    // sparse index stores each checkpoint's line number, so recovery lands
    // at the right place regardless.
    let data = "\
city,country
\"Boston
MA\",United States
Concord,United States
Tokyo,Japan
";

    // Build the index, saving a checkpoint for every second record.
    let mut idxbuf = io::Cursor::new(vec![]);
    {
        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(io::Cursor::new(data));
        SparseIndex::create(&mut rdr, 2, &mut idxbuf)?;
    }

    // Open and validate the index against a fresh reader over the same data.
    let mut idx = SparseIndex::open(idxbuf)?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(io::Cursor::new(data));
    idx.verify(&mut rdr)?;

    // Jump straight to record 3.
    idx.seek_record(&mut rdr, 3)?;
    let mut record = csv::ByteRecord::new();
    rdr.read_byte_record(&mut record)?;
    assert_eq!(&record[0], b"Tokyo");

    // If the source data changes (even without a length change), verify
    // detects the tampering and refuses the stale index.
    let tampered: &str = "XXXX,country\n\"Boston\nMA\",United States\nConcord,United States\nTokyo,Japan\n";
    let mut stale_rdr = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(io::Cursor::new(tampered));
    assert_eq!(tampered.len(), data.len());
    match idx.verify(&mut stale_rdr) {
        Err(err) => assert!(matches!(
            err.kind(),
            csv_index::sparse::ErrorKind::Stale(
                StaleError::SourceDigest
            )
        )),
        Ok(()) => panic!("expected the tampered index to be rejected"),
    }
    Ok(())
}
```
*/

use std::fmt;
use std::io;
use std::result;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use csv;

/// The magic number at the start of every sparse index file (`CSVSPAR1`).
pub const MAGIC: &[u8; 8] = b"CSVSPAR1";

/// The version of the on disk format produced and understood by this crate.
pub const FORMAT_VERSION: u16 = 1;

/// The length in bytes of the fixed index header.
pub const HEADER_LEN: u64 = 96;

/// The length in bytes of a single checkpoint entry.
pub const CHECKPOINT_LEN: u64 = 24;

/// The length in bytes of the parsing dialect fingerprint.
pub const DIALECT_LEN: usize = 32;

// Offsets within the fixed header. Exposed so that other tools (and tests)
// can inspect or patch the documented binary format.

/// Byte offset of the format `version: u16` field in the header.
pub const OFF_VERSION: u64 = 8;
/// Byte offset of the `step: u64` field in the header.
pub const OFF_STEP: u64 = 16;
/// Byte offset of the total `records: u64` field in the header.
pub const OFF_RECORDS: u64 = 24;
/// Byte offset of the `checkpoints: u64` count field in the header.
pub const OFF_CHECKPOINTS: u64 = 32;
/// Byte offset of the `source_len: u64` field in the header.
pub const OFF_SOURCE_LEN: u64 = 40;
/// Byte offset of the source `digest: u128` field in the header.
pub const OFF_DIGEST: u64 = 48;
/// Byte offset of the 32 byte dialect fingerprint in the header.
pub const OFF_DIALECT: u64 = 64;

// Offsets within a checkpoint entry.

/// Offset of the checkpoint `record: u64` field.
pub const OFF_CP_RECORD: u64 = 0;
/// Offset of the checkpoint `byte: u64` field.
pub const OFF_CP_BYTE: u64 = 8;
/// Offset of the checkpoint `line: u64` field.
pub const OFF_CP_LINE: u64 = 16;

/// A type alias for `Result<T, csv_index::sparse::Error>`.
pub type Result<T> = result::Result<T, Error>;

/// An error that can occur while creating, opening or using a sparse index.
#[derive(Debug)]
pub struct Error(Box<ErrorKind>);

impl Error {
    /// Return the specific kind of this error.
    pub fn kind(&self) -> &ErrorKind {
        &self.0
    }
}

/// The specific kind of a sparse index [`Error`](struct.Error.html).
#[derive(Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The index data is structurally invalid (bad magic, truncated,
    /// non-monotonic checkpoint numbers or offsets, and so on).
    ///
    /// The string contains a human readable description of the problem.
    Corrupt(String),

    /// The index is structurally valid, but it no longer matches the CSV
    /// data it indexes or the parsing dialect of the reader used to open it.
    Stale(StaleError),

    /// A CSV parsing error occurred while building the index or skipping
    /// records after a checkpoint.
    Csv(csv::Error),

    /// An underlying IO error occurred.
    Io(io::Error),
}

/// Describes why an otherwise structurally valid index is stale and must not
/// be used.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StaleError {
    /// The indexed CSV source file has a different byte length than when the
    /// index was created.
    SourceSize,
    /// The indexed CSV source file has the same length, but its contents have
    /// changed (the digest differs).
    SourceDigest,
    /// The CSV parsing dialect (delimiter, quote, terminator, ...) differs
    /// from the one stored in the index.
    Dialect,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self.kind() {
            ErrorKind::Corrupt(ref msg) => {
                write!(f, "corrupt sparse CSV index: {}", msg)
            }
            ErrorKind::Stale(StaleError::SourceSize) => write!(
                f,
                "stale sparse CSV index: source file size has changed"
            ),
            ErrorKind::Stale(StaleError::SourceDigest) => write!(
                f,
                "stale sparse CSV index: source file contents have changed"
            ),
            ErrorKind::Stale(StaleError::Dialect) => write!(
                f,
                "stale sparse CSV index: CSV parsing dialect differs"
            ),
            ErrorKind::Csv(ref err) => write!(f, "CSV error: {}", err),
            ErrorKind::Io(ref err) => write!(f, "IO error: {}", err),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self.kind() {
            ErrorKind::Csv(ref err) => Some(err),
            ErrorKind::Io(ref err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error(Box::new(ErrorKind::Io(err)))
    }
}

impl From<csv::Error> for Error {
    fn from(err: csv::Error) -> Error {
        Error(Box::new(ErrorKind::Csv(err)))
    }
}

fn corrupt(msg: impl Into<String>) -> Error {
    Error(Box::new(ErrorKind::Corrupt(msg.into())))
}

/// A sparse index permitting random access to CSV records.
///
/// The index stores a complete [`csv::Position`] for only every `step`th
/// record (always including record `0`), together with metadata used to
/// detect whether the index is still valid for its source data and parsing
/// dialect. See the [module documentation](index.html) for details.
#[derive(Debug)]
pub struct SparseIndex<R> {
    /// The reader containing the index itself.
    rdr: R,
    /// A checkpoint is stored for every `step`th record.
    step: u64,
    /// The total number of records (including any header record) indexed.
    len: u64,
    /// The number of checkpoints in the table.
    ncheck: u64,
    /// The size of the source CSV data when the index was created.
    src_len: u64,
    /// The FNV-1a 128 digest of the source CSV data.
    digest: u128,
    /// The parsing dialect fingerprint.
    dialect: [u8; DIALECT_LEN],
}

impl<W: io::Write> SparseIndex<W> {
    /// Build a sparse index for the given CSV reader and write it to `wtr`.
    ///
    /// A complete seekable position is stored for every `step`th record,
    /// where the first record (index `0`, including the header record if the
    /// reader is configured with `has_headers(true)`) is always indexed.
    ///
    /// `step` must be at least `1`.
    ///
    /// The given reader is read in full until EOF. The reader must be fresh:
    /// it must not have read any records yet, since the source data is hashed
    /// and scanned straight from its beginning.
    pub fn create<R: io::Read + io::Seek>(
        rdr: &mut csv::Reader<R>,
        step: u64,
        mut wtr: W,
    ) -> Result<()> {
        if step == 0 {
            return Err(corrupt("checkpoint step must be at least 1"));
        }
        // First, hash the entire raw source straight from the underlying
        // file and record its size. `get_mut` hands us the file itself; the
        // reader's BufReader is empty because no records were read yet.
        let (src_len, digest) = {
            let file = rdr.get_mut();
            file.seek(io::SeekFrom::Start(0))?;
            let mut hasher = Fnv128::new();
            let mut buf = [0u8; 16 * (1 << 10)];
            let mut len: u64 = 0;
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                len = len.checked_add(n as u64).ok_or_else(|| {
                    corrupt("source file is larger than 2^64 bytes")
                })?;
                hasher.update(&buf[..n]);
            }
            file.seek(io::SeekFrom::Start(0))?;
            (len, hasher.finish())
        };
        let dialect = dialect_of(rdr);

        // (record, byte, line) triples for each checkpoint.
        let mut checkpoints: Vec<(u64, u64, u64)> = Vec::new();
        let mut record_idx: u64 = 0;

        // If the reader treats the first record as a header, read it
        // explicitly so that the header is included in the index as record 0.
        if rdr.has_headers() {
            let header = rdr.byte_headers()?;
            if !header.is_empty() {
                let pos = header.position().expect("position on header row");
                checkpoints.push((record_idx, pos.byte(), pos.line()));
                record_idx += 1;
            }
        }

        let mut record = csv::ByteRecord::new();
        while rdr.read_byte_record(&mut record)? {
            let pos = record.position().expect("position on row");
            if record_idx % step == 0 {
                checkpoints.push((record_idx, pos.byte(), pos.line()));
            }
            record_idx += 1;
        }

        let ncheck = checkpoints.len() as u64;

        // Header (exactly HEADER_LEN bytes).
        wtr.write_all(MAGIC)?;
        wtr.write_u16::<BigEndian>(FORMAT_VERSION)?;
        wtr.write_u16::<BigEndian>(0)?; // reserved
        wtr.write_u32::<BigEndian>(0)?; // reserved
        wtr.write_u64::<BigEndian>(step)?;
        wtr.write_u64::<BigEndian>(record_idx)?;
        wtr.write_u64::<BigEndian>(ncheck)?;
        wtr.write_u64::<BigEndian>(src_len)?;
        wtr.write_u64::<BigEndian>((digest >> 64) as u64)?;
        wtr.write_u64::<BigEndian>(digest as u64)?;
        wtr.write_all(&dialect)?;

        // Checkpoint table.
        for &(record, byte, line) in &checkpoints {
            wtr.write_u64::<BigEndian>(record)?;
            wtr.write_u64::<BigEndian>(byte)?;
            wtr.write_u64::<BigEndian>(line)?;
        }
        wtr.flush()?;
        Ok(())
    }
}

impl<R: io::Read + io::Seek> SparseIndex<R> {
    /// Open and validate a sparse index previously written by
    /// [`SparseIndex::create`](struct.SparseIndex.html#method.create).
    ///
    /// This verifies the structural integrity of the index, including the
    /// magic number, format version, checkpoint table size and the ordering
    /// of every checkpoint. It does *not* access the indexed CSV data; use
    /// [`verify`](struct.SparseIndex.html#method.verify) to check that the
    /// source file and parsing dialect still match the index.
    pub fn open(mut rdr: R) -> Result<SparseIndex<R>> {
        let file_len = rdr.seek(io::SeekFrom::End(0))?;
        if file_len < HEADER_LEN {
            return Err(corrupt(format!(
                "index is too small: {} bytes but header requires {} bytes",
                file_len, HEADER_LEN
            )));
        }

        rdr.seek(io::SeekFrom::Start(0))?;
        let mut magic = [0u8; 8];
        rdr.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(corrupt("bad magic number (not a sparse CSV index)"));
        }
        let version = rdr.read_u16::<BigEndian>()?;
        if version != FORMAT_VERSION {
            return Err(corrupt(format!(
                "unsupported index format version {} (supported: {})",
                version, FORMAT_VERSION
            )));
        }
        if rdr.read_u16::<BigEndian>()? != 0
            || rdr.read_u32::<BigEndian>()? != 0
        {
            return Err(corrupt("non-zero reserved header bytes"));
        }
        let step = rdr.read_u64::<BigEndian>()?;
        if step == 0 {
            return Err(corrupt("checkpoint step is zero"));
        }
        let len = rdr.read_u64::<BigEndian>()?;
        let ncheck = rdr.read_u64::<BigEndian>()?;
        let src_len = rdr.read_u64::<BigEndian>()?;
        let digest_hi = rdr.read_u64::<BigEndian>()?;
        let digest_lo = rdr.read_u64::<BigEndian>()?;
        let mut dialect = [0u8; DIALECT_LEN];
        rdr.read_exact(&mut dialect)?;
        for &b in &dialect[13..] {
            if b != 0 {
                return Err(corrupt("non-zero reserved dialect bytes"));
            }
        }
        let digest = ((digest_hi as u128) << 64) | (digest_lo as u128);

        // The remainder of the file must be exactly `ncheck` entries.
        let body_len = file_len - HEADER_LEN;
        if body_len % CHECKPOINT_LEN != 0 {
            return Err(corrupt(format!(
                "checkpoint table has a partial entry: {} trailing bytes",
                body_len % CHECKPOINT_LEN
            )));
        }
        let entries = body_len / CHECKPOINT_LEN;
        if entries != ncheck {
            return Err(corrupt(format!(
                "header reports {} checkpoints but table contains {}",
                ncheck, entries
            )));
        }

        // The number of checkpoints must match `len` and `step`.
        let expected = if len == 0 {
            0
        } else {
            1u64.checked_add((len - 1) / step)
                .ok_or_else(|| corrupt("checkpoint count overflow"))?
        };
        if ncheck != expected {
            return Err(corrupt(format!(
                "header reports {} checkpoints, but {} records with step {} \
                 require {}",
                ncheck, len, step, expected
            )));
        }

        // Validate every checkpoint: record numbers exactly follow
        // 0, step, 2*step, ... while byte offsets and line numbers strictly
        // increase. Every offset must point inside the source data.
        let mut prev_byte: Option<u64> = None;
        let mut prev_line: Option<u64> = None;
        for i in 0..ncheck {
            rdr.seek(io::SeekFrom::Start(HEADER_LEN + i * CHECKPOINT_LEN))?;
            let record = rdr.read_u64::<BigEndian>()?;
            let byte = rdr.read_u64::<BigEndian>()?;
            let line = rdr.read_u64::<BigEndian>()?;

            let want_record = i
                .checked_mul(step)
                .ok_or_else(|| corrupt("checkpoint record number overflow"))?;
            if record != want_record {
                return Err(corrupt(format!(
                    "checkpoint #{} has record number {} (expected {})",
                    i, record, want_record
                )));
            }
            if line == 0 {
                return Err(corrupt(format!(
                    "checkpoint for record {} has line number 0",
                    record
                )));
            }
            if byte >= src_len {
                return Err(corrupt(format!(
                    "checkpoint for record {} points at byte offset {} which \
                     is outside the {} byte source file",
                    record, byte, src_len
                )));
            }
            if let Some(prev_byte) = prev_byte {
                if byte <= prev_byte {
                    return Err(corrupt(format!(
                        "checkpoint byte offset {} for record {} does not \
                         follow the previous offset {}",
                        byte, record, prev_byte
                    )));
                }
            }
            if let Some(prev_line) = prev_line {
                if line <= prev_line {
                    return Err(corrupt(format!(
                        "checkpoint line number {} for record {} does not \
                         follow the previous line {}",
                        line, record, prev_line
                    )));
                }
            }
            prev_byte = Some(byte);
            prev_line = Some(line);
        }

        Ok(SparseIndex { rdr, step, len, ncheck, src_len, digest, dialect })
    }

    /// Verify that this index matches the CSV data and parsing dialect of
    /// `rdr`.
    ///
    /// This reads (and hashes) the complete underlying source from its
    /// beginning. The reader must be fresh (no records read yet), like the
    /// reader passed to
    /// [`create`](struct.SparseIndex.html#method.create). On success, the
    /// reader's underlying data is positioned at byte `0`, so it can be
    /// seeked or read immediately afterwards.
    ///
    /// This returns an
    /// [`ErrorKind::Stale`](enum.ErrorKind.html#variant.Stale) error if the
    /// source size changed, the source contents changed (same size but
    /// different digest) or the parsing dialect differs.
    pub fn verify<S: io::Read + io::Seek>(
        &mut self,
        rdr: &mut csv::Reader<S>,
    ) -> Result<()> {
        if dialect_of(rdr) != self.dialect {
            return Err(Error(Box::new(ErrorKind::Stale(
                StaleError::Dialect,
            ))));
        }
        let file = rdr.get_mut();
        file.seek(io::SeekFrom::Start(0))?;
        let mut hasher = Fnv128::new();
        let mut buf = [0u8; 16 * (1 << 10)];
        let mut len: u64 = 0;
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            len = len.checked_add(n as u64).ok_or_else(|| {
                corrupt("source file is larger than 2^64 bytes")
            })?;
            hasher.update(&buf[..n]);
        }
        file.seek(io::SeekFrom::Start(0))?;
        if len != self.src_len {
            return Err(Error(Box::new(ErrorKind::Stale(
                StaleError::SourceSize,
            ))));
        }
        if hasher.finish() != self.digest {
            return Err(Error(Box::new(ErrorKind::Stale(
                StaleError::SourceDigest,
            ))));
        }
        Ok(())
    }

    /// Return the checkpoint nearest to, and not after, record `k`.
    ///
    /// On success, this returns the seekable [`csv::Position`] of the
    /// checkpoint and the number of records that must be parsed from the
    /// checkpoint to reach record `k` (a value between `0` and `step - 1`).
    ///
    /// Seek the CSV reader to the returned position and then skip the
    /// returned number of records (or use
    /// [`seek_record`](struct.SparseIndex.html#method.seek_record)) to read
    /// record `k`.
    pub fn get(&mut self, k: u64) -> Result<(csv::Position, u64)> {
        if k >= self.len {
            let err = io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "invalid record index {} (there are {} records)",
                    k, self.len
                ),
            );
            return Err(Error(Box::new(ErrorKind::Io(err))));
        }
        let cp_index = k / self.step;
        let cp_record = cp_index * self.step;
        self.rdr.seek(io::SeekFrom::Start(
            HEADER_LEN + cp_index * CHECKPOINT_LEN,
        ))?;
        let record = self.rdr.read_u64::<BigEndian>()?;
        let byte = self.rdr.read_u64::<BigEndian>()?;
        let line = self.rdr.read_u64::<BigEndian>()?;
        debug_assert_eq!(record, cp_record);

        let mut pos = csv::Position::new();
        pos.set_byte(byte).set_line(line).set_record(cp_record);
        Ok((pos, k - cp_record))
    }

    /// Seek the given CSV reader to record `k`.
    ///
    /// This seeks to the nearest checkpoint preceding `k` (or to `k` itself
    /// when it is a checkpoint) and then parses forward, discarding records,
    /// until the reader is positioned immediately before record `k`. The
    /// next record read from `rdr` will therefore be record `k`.
    ///
    /// Forward parsing correctly crosses records that contain embedded
    /// (quoted) line breaks.
    ///
    /// The index should be
    /// [`verify`](struct.SparseIndex.html#method.verify)-ied against `rdr`
    /// before calling this.
    ///
    /// Returns the number of records skipped after the checkpoint.
    pub fn seek_record<S: io::Read + io::Seek>(
        &mut self,
        rdr: &mut csv::Reader<S>,
        k: u64,
    ) -> Result<u64> {
        let (pos, skip) = self.get(k)?;
        rdr.seek(pos)?;
        if skip > 0 {
            let mut record = csv::ByteRecord::new();
            for _ in 0..skip {
                if !rdr.read_byte_record(&mut record)? {
                    return Err(corrupt(format!(
                        "checkpoint for record {} led to premature EOF while \
                         skipping {} records",
                        k, skip
                    )));
                }
            }
        }
        Ok(skip)
    }

    /// Return the total number of records (including any header record) in
    /// this index.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Return true if and only if this index contains zero records.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Return the checkpoint interval: a position is stored for every
    /// `step`th record.
    pub fn step(&self) -> u64 {
        self.step
    }

    /// Return the number of checkpoints stored in this index.
    pub fn checkpoint_len(&self) -> u64 {
        self.ncheck
    }

    /// Return the size in bytes of the source CSV data recorded when this
    /// index was created.
    pub fn source_len(&self) -> u64 {
        self.src_len
    }

    /// Consume this index, returning the underlying reader.
    pub fn into_inner(self) -> R {
        self.rdr
    }
}

/// Compute the fixed 32 byte dialect fingerprint for a reader.
fn dialect_of<R: io::Read + io::Seek>(
    rdr: &csv::Reader<R>,
) -> [u8; DIALECT_LEN] {
    let mut d = [0u8; DIALECT_LEN];
    d[0] = rdr.delimiter();
    d[1] = rdr.quote();
    match rdr.escape() {
        None => d[2] = 0,
        Some(b) => {
            d[2] = 1;
            d[3] = b;
        }
    }
    d[4] = u8::from(rdr.double_quote());
    d[5] = u8::from(rdr.quoting());
    match rdr.comment() {
        None => d[6] = 0,
        Some(b) => {
            d[6] = 1;
            d[7] = b;
        }
    }
    match rdr.terminator() {
        csv::Terminator::CRLF => d[8] = 0,
        csv::Terminator::Any(b) => {
            d[8] = 1;
            d[9] = b;
        }
        _ => d[8] = 0,
    }
    d[10] = match rdr.trim() {
        csv::Trim::None => 0,
        csv::Trim::Headers => 1,
        csv::Trim::Fields => 2,
        csv::Trim::All => 3,
        _ => 0,
    };
    d[11] = u8::from(rdr.has_headers());
    d[12] = u8::from(rdr.is_flexible());
    // Bytes 13..32 stay zero and are validated as reserved on open.
    d
}

/// A streaming FNV-1a 128 bit hasher.
///
/// FNV-1a is a simple, deterministic, non-cryptographic hash. Two files
/// with identical FNV-1a digests are treated as identical; the 128 bit
/// width makes accidental collisions astronomically unlikely.
#[derive(Clone, Copy)]
struct Fnv128 {
    state: u128,
}

const FNV_OFFSET_BASIS_128: u128 = 0x6c62272e07bb0142_62b821756295c58d;
const FNV_PRIME_128: u128 = 0x0000000001000000_000000000000013b;

impl Fnv128 {
    fn new() -> Fnv128 {
        Fnv128 { state: FNV_OFFSET_BASIS_128 }
    }

    fn update(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.state ^= b as u128;
            self.state = self.state.wrapping_mul(FNV_PRIME_128);
        }
    }

    fn finish(self) -> u128 {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CHECKPOINT_LEN, DIALECT_LEN, HEADER_LEN, OFF_CHECKPOINTS, OFF_CP_BYTE,
        OFF_CP_LINE, OFF_CP_RECORD, OFF_DIALECT, OFF_DIGEST, OFF_RECORDS,
        OFF_SOURCE_LEN, OFF_STEP, OFF_VERSION,
    };

    // Keep the documented header layout in sync with the code.
    #[test]
    fn header_layout_is_stable() {
        assert_eq!(HEADER_LEN as usize, 64 + DIALECT_LEN);
        assert_eq!(CHECKPOINT_LEN, 24);
        assert_eq!(OFF_VERSION, 8);
        assert_eq!(OFF_STEP, 16);
        assert_eq!(OFF_RECORDS, 24);
        assert_eq!(OFF_CHECKPOINTS, 32);
        assert_eq!(OFF_SOURCE_LEN, 40);
        assert_eq!(OFF_DIGEST, 48);
        assert_eq!(OFF_DIALECT, 64);
        assert_eq!(OFF_CP_RECORD, 0);
        assert_eq!(OFF_CP_BYTE, 8);
        assert_eq!(OFF_CP_LINE, 16);
    }
}
