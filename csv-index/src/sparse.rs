/*!
A sparse random access index for CSV data.

Where [`crate::RandomAccessSimple`] stores the byte offset of *every* record, a
sparse index only stores a full [`csv::Position`] (byte offset, line number
and record index) once every `N` records. Seeking to an arbitrary record `k`
is done by seeking to the nearest checkpoint at or before `k` and then
scanning forward at most `N-1` records. Because checkpoints are real
`csv::Position`s captured at record boundaries, seeking is safe even when
records contain quoted fields with embedded newlines, and line numbers
reported after a seek remain correct.

In addition to the checkpoints, the index stores a fingerprint of the source
data (its length and a digest of its contents) and a fingerprint of the full
parse dialect (delimiter, quote, escape, comment, double quote, quoting,
terminator, trim, flexible and header settings). When an index is opened, it
is validated against the current source metadata and dialect. If the CSV
data changed (even if its size stayed the same) or if any dialect setting
changed, the index is rejected as stale instead of silently returning wrong
results.

The on-disk format is:

```text
offset  len  field
0       8    magic: b"CSVSPIDX"
8       4    format version (u32, big endian), currently 1
12      8    checkpoint interval N (u64)
20      8    number of checkpoints (u64)
28      8    total number of records (u64)
36      8    source length in bytes (u64)
44      8    source digest (u64, FNV-1a)
52      8    dialect fingerprint (u64, FNV-1a of the dialect block)
60      16   dialect block (see `Dialect::encode`)
76      ...  checkpoints, 24 bytes each: record, byte, line (u64s)
```

Checkpoints are validated when the index is opened: record indices must be
strictly increasing multiples of the interval starting at 0, and every
checkpoint byte offset must lie within the source data.
*/

use std::fs::File;
use std::io;
use std::path::Path;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use csv;

/// The magic bytes at the start of every sparse index.
const MAGIC: &[u8; 8] = b"CSVSPIDX";
/// The current version of the sparse index format.
const VERSION: u32 = 1;
/// The length in bytes of the encoded dialect block.
const DIALECT_LEN: usize = 16;

/// The FNV-1a offset basis for 64 bit hashes.
const FNV_OFFSET: u64 = 0xcbf29ce484222325;
/// The FNV-1a prime for 64 bit hashes.
const FNV_PRIME: u64 = 0x100000001b3;

/// Update a 64 bit FNV-1a hash with the given bytes.
fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Create a `csv::Error` from an `io::ErrorKind::InvalidData` error.
fn invalid_data(msg: String) -> csv::Error {
    csv::Error::from(io::Error::new(io::ErrorKind::InvalidData, msg))
}

/// A complete description of the CSV parse dialect.
///
/// This mirrors every parser-affecting setting of `csv::ReaderBuilder`. It
/// is stored in the sparse index so that an index built with one dialect is
/// never used with a reader configured with a different dialect.
///
/// The default value matches the defaults of `csv::ReaderBuilder`.
#[derive(Clone, Copy, Debug)]
pub struct Dialect {
    /// The field delimiter. Defaults to `b','`.
    pub delimiter: u8,
    /// The quote byte. Defaults to `b'"'`.
    pub quote: u8,
    /// The escape byte. Defaults to `None`.
    pub escape: Option<u8>,
    /// The comment byte. Defaults to `None`.
    pub comment: Option<u8>,
    /// Whether two consecutive quotes escape a quote. Defaults to `true`.
    pub double_quote: bool,
    /// Whether quoting is enabled at all. Defaults to `true`.
    pub quoting: bool,
    /// The record terminator. Defaults to `csv::Terminator::CRLF`.
    pub terminator: csv::Terminator,
    /// The trim behavior. Defaults to `csv::Trim::None`.
    pub trim: csv::Trim,
    /// Whether records with varying field counts are allowed. Defaults to
    /// `false`.
    pub flexible: bool,
    /// Whether the first record is interpreted as a header. Defaults to
    /// `true`.
    pub has_headers: bool,
}

impl Default for Dialect {
    fn default() -> Dialect {
        Dialect {
            delimiter: b',',
            quote: b'"',
            escape: None,
            comment: None,
            double_quote: true,
            quoting: true,
            terminator: csv::Terminator::CRLF,
            trim: csv::Trim::None,
            flexible: false,
            has_headers: true,
        }
    }
}

impl Dialect {
    /// Encode this dialect into a fixed size 16 byte block.
    ///
    /// The encoding is unambiguous: two dialects encode to the same block
    /// if and only if every setting is equal.
    fn encode(&self) -> [u8; DIALECT_LEN] {
        let mut b = [0u8; DIALECT_LEN];
        b[0] = self.delimiter;
        b[1] = self.quote;
        b[2] = self.escape.is_some() as u8;
        b[3] = self.escape.unwrap_or(0);
        b[4] = self.comment.is_some() as u8;
        b[5] = self.comment.unwrap_or(0);
        b[6] = self.double_quote as u8;
        b[7] = self.quoting as u8;
        b[8] = match self.terminator {
            csv::Terminator::CRLF => 0xFF,
            csv::Terminator::Any(term) => term,
            // `csv::Terminator` is non-exhaustive; treat unknown variants
            // like the default so encoding remains total.
            _ => 0xFF,
        };
        b[9] = match self.trim {
            csv::Trim::None => 0,
            csv::Trim::Headers => 1,
            csv::Trim::Fields => 2,
            csv::Trim::All => 3,
            _ => 0,
        };
        b[10] = self.flexible as u8;
        b[11] = self.has_headers as u8;
        // b[12..16] are reserved and must be zero.
        b
    }

    /// Decode a dialect from a 16 byte block previously written by
    /// `Dialect::encode`.
    fn decode(b: &[u8; DIALECT_LEN]) -> csv::Result<Dialect> {
        let term = match b[8] {
            0xFF => csv::Terminator::CRLF,
            term => csv::Terminator::Any(term),
        };
        let trim = match b[9] {
            0 => csv::Trim::None,
            1 => csv::Trim::Headers,
            2 => csv::Trim::Fields,
            3 => csv::Trim::All,
            tag => {
                return Err(invalid_data(format!(
                    "invalid trim tag {} in sparse index dialect",
                    tag
                )));
            }
        };
        Ok(Dialect {
            delimiter: b[0],
            quote: b[1],
            escape: if b[2] != 0 { Some(b[3]) } else { None },
            comment: if b[4] != 0 { Some(b[5]) } else { None },
            double_quote: b[6] != 0,
            quoting: b[7] != 0,
            terminator: term,
            trim: trim,
            flexible: b[10] != 0,
            has_headers: b[11] != 0,
        })
    }

    /// Return a 64 bit fingerprint of this dialect.
    pub fn fingerprint(&self) -> u64 {
        fnv1a(FNV_OFFSET, &self.encode())
    }

    /// Return a `csv::ReaderBuilder` configured with this dialect.
    ///
    /// The reader built from it parses CSV data exactly the way the data was
    /// parsed when an index storing this dialect was created.
    pub fn reader_builder(&self) -> csv::ReaderBuilder {
        let mut b = csv::ReaderBuilder::new();
        b.delimiter(self.delimiter)
            .quote(self.quote)
            .escape(self.escape)
            .comment(self.comment)
            .double_quote(self.double_quote)
            .quoting(self.quoting)
            .terminator(self.terminator)
            .trim(self.trim)
            .flexible(self.flexible)
            .has_headers(self.has_headers);
        b
    }
}

/// A fingerprint of the CSV source data: its length and a digest of its
/// contents.
///
/// The digest is a 64 bit FNV-1a hash over every byte of the source. Two
/// sources with the same length but different contents (e.g. a file that
/// was modified in place without changing its size) have different digests
/// with overwhelming probability, so stale indexes are detected even when
/// the file size is unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceMeta {
    len: u64,
    digest: u64,
}

impl SourceMeta {
    /// Create source metadata from a known length and digest.
    pub fn new(len: u64, digest: u64) -> SourceMeta {
        SourceMeta { len: len, digest: digest }
    }

    /// Compute source metadata by reading the given reader to EOF.
    pub fn from_reader<R: io::Read>(mut rdr: R) -> io::Result<SourceMeta> {
        let mut hash = FNV_OFFSET;
        let mut len = 0u64;
        let mut buf = [0u8; 8192];
        loop {
            let n = rdr.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hash = fnv1a(hash, &buf[..n]);
            len += n as u64;
        }
        Ok(SourceMeta { len: len, digest: hash })
    }

    /// Compute source metadata for the file at the given path.
    pub fn from_path<P: AsRef<Path>>(path: P) -> io::Result<SourceMeta> {
        SourceMeta::from_reader(io::BufReader::new(File::open(path)?))
    }

    /// The length of the source data in bytes.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// The digest of the source data.
    pub fn digest(&self) -> u64 {
        self.digest
    }
}

/// A single checkpoint in a sparse index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Checkpoint {
    /// The index of the record at this checkpoint.
    record: u64,
    /// The byte offset at which the record starts.
    byte: u64,
    /// The line number at which the record starts.
    line: u64,
}

impl Checkpoint {
    fn position(&self) -> csv::Position {
        let mut pos = csv::Position::new();
        pos.set_byte(self.byte).set_line(self.line).set_record(self.record);
        pos
    }
}

/// A sparse random access index over CSV data.
///
/// The index stores a full `csv::Position` every `interval` records, along
/// with the source metadata and the parse dialect the index was built with.
/// Use [`SparseRandomAccess`] to read arbitrary records with it.
#[derive(Clone, Debug)]
pub struct SparseIndex {
    interval: u64,
    total_records: u64,
    source: SourceMeta,
    dialect: Dialect,
    dialect_fingerprint: u64,
    checkpoints: Vec<Checkpoint>,
}

impl SparseIndex {
    /// Write a sparse index for the CSV data read by `rdr` to `wtr`.
    ///
    /// A checkpoint is stored every `interval` records (the first record is
    /// always a checkpoint). `interval` must be at least 1.
    ///
    /// `meta` must be the source metadata of the exact CSV data that `rdr`
    /// reads, and `dialect` must describe the parse settings that `rdr` was
    /// built with. Both are stored in the index and verified when the index
    /// is opened again.
    ///
    /// All records are indexed, including the first record even if the
    /// reader is configured to interpret it as a header record.
    pub fn create<R: io::Read, W: io::Write>(
        rdr: &mut csv::Reader<R>,
        interval: u64,
        meta: &SourceMeta,
        dialect: &Dialect,
        mut wtr: W,
    ) -> csv::Result<()> {
        if interval == 0 {
            return Err(invalid_data(
                "sparse index interval must be at least 1".to_string(),
            ));
        }
        let mut checkpoints: Vec<Checkpoint> = vec![];
        let mut total: u64 = 0;
        // If the reader is configured to read a header, then read that
        // first. (The CSV reader otherwise will not yield the header
        // record when calling `read_byte_record`.)
        if rdr.has_headers() {
            let header = rdr.byte_headers()?;
            if !header.is_empty() {
                let pos = header.position().expect("position on header row");
                if pos.record() % interval == 0 {
                    checkpoints.push(Checkpoint {
                        record: pos.record(),
                        byte: pos.byte(),
                        line: pos.line(),
                    });
                }
                total += 1;
            }
        }
        let mut record = csv::ByteRecord::new();
        while rdr.read_byte_record(&mut record)? {
            let pos = record.position().expect("position on row");
            if pos.record() % interval == 0 {
                checkpoints.push(Checkpoint {
                    record: pos.record(),
                    byte: pos.byte(),
                    line: pos.line(),
                });
            }
            total += 1;
        }

        wtr.write_all(MAGIC)?;
        wtr.write_u32::<BigEndian>(VERSION)?;
        wtr.write_u64::<BigEndian>(interval)?;
        wtr.write_u64::<BigEndian>(checkpoints.len() as u64)?;
        wtr.write_u64::<BigEndian>(total)?;
        wtr.write_u64::<BigEndian>(meta.len())?;
        wtr.write_u64::<BigEndian>(meta.digest())?;
        wtr.write_u64::<BigEndian>(dialect.fingerprint())?;
        wtr.write_all(&dialect.encode())?;
        for cp in &checkpoints {
            wtr.write_u64::<BigEndian>(cp.record)?;
            wtr.write_u64::<BigEndian>(cp.byte)?;
            wtr.write_u64::<BigEndian>(cp.line)?;
        }
        wtr.flush()?;
        Ok(())
    }

    /// Open a sparse index and validate it against the given source
    /// metadata and dialect.
    ///
    /// This returns an error if the index is corrupt (truncated, bad magic,
    /// non-increasing record numbers, checkpoint offsets outside the source
    /// data) or if it is stale, i.e. the source data changed (its length or
    /// digest differ) or any dialect setting changed since the index was
    /// created.
    pub fn open<R: io::Read>(
        rdr: R,
        meta: &SourceMeta,
        dialect: &Dialect,
    ) -> csv::Result<SparseIndex> {
        let idx = SparseIndex::open_unchecked(rdr)?;
        idx.validate(meta, dialect)?;
        Ok(idx)
    }

    /// Open a sparse index, validating only its internal structure.
    ///
    /// The source metadata and dialect stored in the index are *not*
    /// checked. Prefer `open` unless the checks are done separately.
    pub fn open_unchecked<R: io::Read>(
        mut rdr: R,
    ) -> csv::Result<SparseIndex> {
        let mut magic = [0u8; 8];
        rdr.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(invalid_data(
                "invalid sparse index magic bytes".to_string(),
            ));
        }
        let version = rdr.read_u32::<BigEndian>()?;
        if version != VERSION {
            return Err(invalid_data(format!(
                "unsupported sparse index version {}",
                version
            )));
        }
        let interval = rdr.read_u64::<BigEndian>()?;
        let count = rdr.read_u64::<BigEndian>()?;
        let total = rdr.read_u64::<BigEndian>()?;
        let source_len = rdr.read_u64::<BigEndian>()?;
        let source_digest = rdr.read_u64::<BigEndian>()?;
        let dialect_fingerprint = rdr.read_u64::<BigEndian>()?;
        let mut block = [0u8; DIALECT_LEN];
        rdr.read_exact(&mut block)?;
        let dialect = Dialect::decode(&block)?;
        if dialect_fingerprint != dialect.fingerprint() {
            return Err(invalid_data(
                "sparse index dialect fingerprint mismatch".to_string(),
            ));
        }
        if interval == 0 {
            return Err(invalid_data(
                "sparse index interval must be at least 1".to_string(),
            ));
        }
        if count > (1 << 32) {
            // Sanity check: a checkpoint count this large implies a
            // corrupt header, since each checkpoint needs 24 bytes.
            return Err(invalid_data(format!(
                "sparse index checkpoint count {} is unreasonably large",
                count
            )));
        }
        let mut checkpoints = Vec::with_capacity(count as usize);
        let mut prev: Option<Checkpoint> = None;
        for _ in 0..count {
            let cp = Checkpoint {
                record: rdr.read_u64::<BigEndian>()?,
                byte: rdr.read_u64::<BigEndian>()?,
                line: rdr.read_u64::<BigEndian>()?,
            };
            if cp.record >= total && total > 0 {
                return Err(invalid_data(format!(
                    "sparse index checkpoint record {} is out of range \
                     (there are {} records)",
                    cp.record, total
                )));
            }
            if cp.record % interval != 0 {
                return Err(invalid_data(format!(
                    "sparse index checkpoint record {} is not a multiple \
                     of the interval {}",
                    cp.record, interval
                )));
            }
            if cp.byte >= source_len && source_len > 0 {
                return Err(invalid_data(format!(
                    "sparse index checkpoint offset {} is outside the \
                     source data ({} bytes)",
                    cp.byte, source_len
                )));
            }
            if let Some(prev) = prev {
                if cp.record <= prev.record {
                    return Err(invalid_data(format!(
                        "sparse index checkpoint records are not strictly \
                         increasing ({} follows {})",
                        cp.record, prev.record
                    )));
                }
                if cp.byte < prev.byte {
                    return Err(invalid_data(format!(
                        "sparse index checkpoint offsets are not \
                         increasing ({} follows {})",
                        cp.byte, prev.byte
                    )));
                }
            } else if cp.record != 0 {
                return Err(invalid_data(format!(
                    "sparse index first checkpoint is at record {}, \
                     expected record 0",
                    cp.record
                )));
            }
            prev = Some(cp);
            checkpoints.push(cp);
        }
        if total > 0 && checkpoints.is_empty() {
            return Err(invalid_data(
                "sparse index has records but no checkpoints".to_string(),
            ));
        }
        Ok(SparseIndex {
            interval: interval,
            total_records: total,
            source: SourceMeta::new(source_len, source_digest),
            dialect: dialect,
            dialect_fingerprint: dialect_fingerprint,
            checkpoints: checkpoints,
        })
    }

    /// Validate this index against the given source metadata and dialect.
    ///
    /// Returns an error if the source data or the parse dialect changed
    /// since the index was created.
    pub fn validate(
        &self,
        meta: &SourceMeta,
        dialect: &Dialect,
    ) -> csv::Result<()> {
        if self.source != *meta {
            return Err(invalid_data(format!(
                "stale sparse index: source data changed \
                 (index: {} bytes, digest {:#018x}; \
                 current: {} bytes, digest {:#018x})",
                self.source.len(),
                self.source.digest(),
                meta.len(),
                meta.digest()
            )));
        }
        if dialect.fingerprint() != self.dialect_fingerprint
            || dialect.encode() != self.dialect.encode()
        {
            return Err(invalid_data(
                "stale sparse index: CSV parse dialect changed".to_string(),
            ));
        }
        Ok(())
    }

    /// Return the number of records in the indexed CSV data (including a
    /// header record, if any).
    pub fn len(&self) -> u64 {
        self.total_records
    }

    /// Return true if and only if the indexed CSV data has no records.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the checkpoint interval this index was built with.
    pub fn interval(&self) -> u64 {
        self.interval
    }

    /// Return the dialect this index was built with.
    pub fn dialect(&self) -> &Dialect {
        &self.dialect
    }

    /// Return the source metadata this index was built with.
    pub fn source(&self) -> &SourceMeta {
        &self.source
    }

    /// Return the position of the nearest checkpoint at or before record
    /// `k`, or `None` if `k` is out of range.
    ///
    /// Seeking a CSV reader to this position and then reading
    /// `k - position.record()` records yields the `k`th record.
    pub fn checkpoint_for(&self, k: u64) -> Option<csv::Position> {
        if k >= self.total_records {
            return None;
        }
        let i = match self.checkpoints.binary_search_by(|cp| cp.record.cmp(&k))
        {
            Ok(i) => i,
            Err(i) => i.checked_sub(1)?,
        };
        Some(self.checkpoints[i].position())
    }
}

/// A random access reader over CSV data backed by a sparse index.
///
/// Reading record `k` seeks the underlying CSV reader to the nearest
/// checkpoint at or before `k` and scans forward from there. Since
/// checkpoints are full `csv::Position`s, this works correctly even when
/// the skipped records contain quoted fields with embedded newlines.
pub struct SparseRandomAccess<R> {
    rdr: csv::Reader<R>,
    idx: SparseIndex,
}

impl<R: io::Read + io::Seek> SparseRandomAccess<R> {
    /// Create a random access reader over `rdr` using the given index.
    ///
    /// The CSV reader is built from the dialect stored in the index, with
    /// headers disabled so that every record (including a header record, if
    /// the data has one) can be read by its absolute record index.
    pub fn from_reader(rdr: R, idx: SparseIndex) -> SparseRandomAccess<R> {
        let mut builder = idx.dialect().reader_builder();
        builder.has_headers(false);
        SparseRandomAccess { rdr: builder.from_reader(rdr), idx: idx }
    }

    /// Create a random access reader from an existing CSV reader and an
    /// index.
    ///
    /// The reader must have been built with the same dialect stored in the
    /// index and must have headers disabled, otherwise record indices will
    /// not line up with the index.
    pub fn from_csv_reader(
        rdr: csv::Reader<R>,
        idx: SparseIndex,
    ) -> SparseRandomAccess<R> {
        SparseRandomAccess { rdr: rdr, idx: idx }
    }

    /// Read the record at index `k`.
    ///
    /// Returns `Ok(None)` if `k` is greater than or equal to the number of
    /// records in the index.
    pub fn get(&mut self, k: u64) -> csv::Result<Option<csv::ByteRecord>> {
        let pos = match self.idx.checkpoint_for(k) {
            None => return Ok(None),
            Some(pos) => pos,
        };
        self.rdr.seek(pos.clone())?;
        let mut record = csv::ByteRecord::new();
        let mut current = pos.record();
        loop {
            if !self.rdr.read_byte_record(&mut record)? {
                return Err(invalid_data(format!(
                    "unexpected end of CSV data while scanning from \
                     record {} to record {}",
                    pos.record(),
                    k
                )));
            }
            if current == k {
                return Ok(Some(record.clone()));
            }
            current += 1;
        }
    }

    /// Return the number of records in the index.
    pub fn len(&self) -> u64 {
        self.idx.len()
    }

    /// Return true if and only if the index has no records.
    pub fn is_empty(&self) -> bool {
        self.idx.is_empty()
    }

    /// Return a reference to the underlying sparse index.
    pub fn index(&self) -> &SparseIndex {
        &self.idx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dialect_roundtrip() {
        let d = Dialect::default();
        let d2 = Dialect::decode(&d.encode()).unwrap();
        assert_eq!(d.fingerprint(), d2.fingerprint());
        assert_eq!(d.encode(), d2.encode());
    }

    #[test]
    fn dialect_fingerprint_changes() {
        let d = Dialect::default();
        let fp = d.fingerprint();
        let mut d2 = d;
        d2.delimiter = b';';
        assert_ne!(fp, d2.fingerprint());
        let mut d3 = d;
        d3.quote = b'\'';
        assert_ne!(fp, d3.fingerprint());
        let mut d4 = d;
        d4.double_quote = false;
        assert_ne!(fp, d4.fingerprint());
    }

    #[test]
    fn source_meta_digest() {
        let a = SourceMeta::from_reader(&b"hello"[..]).unwrap();
        let b = SourceMeta::from_reader(&b"hellp"[..]).unwrap();
        assert_eq!(a.len(), b.len());
        assert_ne!(a.digest(), b.digest());
    }
}
