//! Record-count-sharded CSV writing.
//!
//! A [`ShardedWriter`] writes CSV records into a sequence of shard files.
//! Each shard is a standalone CSV file that starts with the configured
//! header row and contains at most `capacity` data records. Records are
//! counted as whole records, so newlines embedded in quoted fields never
//! cause a shard to be split in the middle of a record.
//!
//! Shards are first written to a temporary path and only published (via an
//! atomic rename to their final path) once every record of the shard has
//! been written and flushed successfully. If writing a shard fails, the
//! temporary file is discarded and the records buffered for that shard are
//! rewritten to a fresh temporary file on the next call. Already published
//! shards are never touched, and the manifest only registers shards that
//! were actually published.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::{ByteRecord, Result, Writer, WriterBuilder};

/// Information about a single published shard.
#[derive(Debug, Clone)]
pub struct ShardInfo {
    /// The zero-based index of the shard. Shards are numbered in publication
    /// order starting at `0`.
    pub index: usize,
    /// The final, published path of the shard file.
    pub path: PathBuf,
    /// The number of data records contained in the shard.
    pub records: usize,
}

/// The location of an original record within a published shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardLocation {
    /// The index of the shard the record was published to.
    pub shard: usize,
    /// The zero-based position of the record within that shard.
    pub record: usize,
}

/// The manifest of a finished [`ShardedWriter`].
///
/// The manifest only lists shards that were successfully published and the
/// locations of records that were confirmed written.
#[derive(Debug, Clone)]
pub struct ShardManifest {
    /// All published shards, in publication order.
    pub shards: Vec<ShardInfo>,
    /// A mapping from the original record index to the record's location
    /// within its published shard.
    pub mapping: Vec<ShardLocation>,
}

type ShardFactory =
    Box<dyn FnMut(usize, &Path) -> io::Result<Box<dyn Write>>>;

/// A builder for configuring a [`ShardedWriter`].
pub struct ShardedWriterBuilder {
    dir: PathBuf,
    prefix: String,
    capacity: usize,
    buffer_capacity: usize,
    headers: Option<ByteRecord>,
    factory: Option<ShardFactory>,
}

impl std::fmt::Debug for ShardedWriterBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedWriterBuilder")
            .field("dir", &self.dir)
            .field("prefix", &self.prefix)
            .field("capacity", &self.capacity)
            .field("buffer_capacity", &self.buffer_capacity)
            .field("headers", &self.headers)
            .field("factory", &self.factory.as_ref().map(|_| "..."))
            .finish()
    }
}

impl ShardedWriterBuilder {
    /// Create a new builder that writes shard files into `dir` using file
    /// names starting with `prefix`.
    ///
    /// The directory is created (including any missing parent directories)
    /// when the builder is built.
    pub fn new<P: AsRef<Path>>(dir: P, prefix: &str) -> ShardedWriterBuilder {
        ShardedWriterBuilder {
            dir: dir.as_ref().to_path_buf(),
            prefix: prefix.to_string(),
            capacity: 10_000,
            buffer_capacity: 8 * 1024,
            headers: None,
            factory: None,
        }
    }

    /// Set the maximum number of data records per shard.
    ///
    /// The default is `10_000`. The capacity must be at least `1`.
    pub fn capacity(
        &mut self,
        capacity: usize,
    ) -> &mut ShardedWriterBuilder {
        self.capacity = capacity;
        self
    }

    /// Set the capacity of the internal buffer used when writing each shard.
    ///
    /// The default is `8192`. A smaller buffer causes writes to reach the
    /// underlying writer more frequently, which is useful for testing fault
    /// injection.
    pub fn buffer_capacity(
        &mut self,
        buffer_capacity: usize,
    ) -> &mut ShardedWriterBuilder {
        self.buffer_capacity = buffer_capacity;
        self
    }

    /// Set the header row that is written at the top of every shard.
    ///
    /// If no headers are set, shards are written without a header row.
    pub fn headers<R: AsRef<[u8]>>(
        &mut self,
        headers: &[R],
    ) -> &mut ShardedWriterBuilder {
        let mut record = ByteRecord::with_capacity(headers.len(), 0);
        for field in headers {
            record.push_field(field.as_ref());
        }
        self.headers = Some(record);
        self
    }

    /// Set a factory that creates the writer for each shard's temporary
    /// file.
    ///
    /// The factory is called with the shard index and the temporary path and
    /// should return a writer that writes to that path. The default factory
    /// creates a regular file at the temporary path.
    ///
    /// This is primarily useful for testing fault injection.
    pub fn factory<F>(&mut self, factory: F) -> &mut ShardedWriterBuilder
    where
        F: FnMut(usize, &Path) -> io::Result<Box<dyn Write>> + 'static,
    {
        self.factory = Some(Box::new(factory));
        self
    }

    /// Build the [`ShardedWriter`].
    pub fn build(&mut self) -> io::Result<ShardedWriter> {
        if self.capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shard capacity must be at least 1",
            ));
        }
        fs::create_dir_all(&self.dir)?;
        let factory = self.factory.take().unwrap_or_else(|| {
            Box::new(|_index: usize, path: &Path| {
                File::create(path)
                    .map(|file| Box::new(file) as Box<dyn Write>)
            })
        });
        Ok(ShardedWriter {
            dir: self.dir.clone(),
            prefix: self.prefix.clone(),
            capacity: self.capacity,
            buffer_capacity: self.buffer_capacity,
            headers: self.headers.clone(),
            factory,
            shards: Vec::new(),
            mapping: Vec::new(),
            current: None,
            current_records: Vec::new(),
        })
    }
}

/// A writer that splits CSV records into independently headed shard files.
///
/// See the [module documentation](index.html) for details.
pub struct ShardedWriter {
    dir: PathBuf,
    prefix: String,
    capacity: usize,
    buffer_capacity: usize,
    headers: Option<ByteRecord>,
    factory: ShardFactory,
    shards: Vec<ShardInfo>,
    mapping: Vec<ShardLocation>,
    current: Option<CurrentShard>,
    current_records: Vec<ByteRecord>,
}

struct CurrentShard {
    index: usize,
    temp: PathBuf,
    wtr: Writer<Box<dyn Write>>,
}

impl std::fmt::Debug for ShardedWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedWriter")
            .field("dir", &self.dir)
            .field("prefix", &self.prefix)
            .field("capacity", &self.capacity)
            .field("buffer_capacity", &self.buffer_capacity)
            .field("headers", &self.headers)
            .field("shards", &self.shards)
            .field("mapping", &self.mapping)
            .field("current_index", &self.current.as_ref().map(|c| c.index))
            .field("current_records", &self.current_records.len())
            .finish()
    }
}

impl ShardedWriter {
    /// Create a new builder for writing shards into `dir` with file names
    /// starting with `prefix`.
    pub fn builder<P: AsRef<Path>>(
        dir: P,
        prefix: &str,
    ) -> ShardedWriterBuilder {
        ShardedWriterBuilder::new(dir, prefix)
    }

    /// Write a single record.
    ///
    /// If the record fills the current shard, the shard is flushed and
    /// published before this method returns. If writing the shard fails,
    /// the temporary shard is discarded and the error is returned; the
    /// record is buffered and will be rewritten to a fresh temporary shard
    /// on the next call.
    pub fn write_record<I, T>(&mut self, record: I) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        let mut byte_record = ByteRecord::new();
        for field in record {
            byte_record.push_field(field.as_ref());
        }
        self.write_byte_record(&byte_record)
    }

    /// Write a single [`ByteRecord`](crate::ByteRecord).
    ///
    /// See [`write_record`](ShardedWriter::write_record) for details.
    pub fn write_byte_record(&mut self, record: &ByteRecord) -> Result<()> {
        let mut just_started = false;
        if self.current.is_none() {
            // Rewrite any records buffered from a previous failed attempt.
            self.start_shard()?;
            just_started = true;
        }
        // If this record is already buffered and the shard was just
        // rewritten, this is a retry after a failed publish: the record is
        // already in `current_records` and must not be written again.
        let is_retry =
            just_started && self.current_records.last() == Some(record);
        if !is_retry {
            let result = self
                .current
                .as_mut()
                .unwrap()
                .wtr
                .write_byte_record(record);
            if let Err(err) = result {
                // The temporary shard may be incomplete. Drop it; the
                // record is rewritten to a fresh temporary shard on retry.
                self.abandon_current();
                return Err(err);
            }
            self.current_records.push(record.clone());
        }
        if self.current_records.len() >= self.capacity {
            self.publish_current()?;
        }
        Ok(())
    }

    /// Finish writing and return the manifest of published shards.
    ///
    /// Any records still buffered in the current shard are flushed and
    /// published before the manifest is returned.
    pub fn finish(mut self) -> Result<ShardManifest> {
        if self.current.is_some() {
            self.publish_current()?;
        }
        Ok(ShardManifest {
            shards: self.shards,
            mapping: self.mapping,
        })
    }

    /// Open a new temporary shard and write the header and any records
    /// buffered from a previous failed attempt.
    fn start_shard(&mut self) -> Result<()> {
        let index = self.shards.len();
        let temp = self.temp_path(index);
        let sink = match (self.factory)(index, &temp) {
            Ok(sink) => sink,
            Err(err) => return Err(err.into()),
        };
        let mut wtr = WriterBuilder::new()
            .has_headers(self.headers.is_some())
            .buffer_capacity(self.buffer_capacity)
            .from_writer(sink);
        if let Some(headers) = &self.headers {
            if let Err(err) = wtr.write_byte_record(headers) {
                drop(wtr);
                let _ = fs::remove_file(&temp);
                return Err(err);
            }
        }
        for record in &self.current_records {
            if let Err(err) = wtr.write_byte_record(record) {
                drop(wtr);
                let _ = fs::remove_file(&temp);
                return Err(err);
            }
        }
        self.current = Some(CurrentShard { index, temp, wtr });
        Ok(())
    }

    /// Flush the current shard, publish it, and register it in the manifest.
    fn publish_current(&mut self) -> Result<()> {
        let current = match self.current.take() {
            Some(current) => current,
            None => return Ok(()),
        };
        let mut wtr = current.wtr;
        if let Err(err) = wtr.flush() {
            drop(wtr);
            let _ = fs::remove_file(&current.temp);
            return Err(err.into());
        }
        drop(wtr);
        let final_path = self.final_path(current.index);
        if let Err(err) = fs::rename(&current.temp, &final_path) {
            let _ = fs::remove_file(&current.temp);
            return Err(err.into());
        }
        let record_count = self.current_records.len();
        for record_index in 0..record_count {
            self.mapping.push(ShardLocation {
                shard: current.index,
                record: record_index,
            });
        }
        self.shards.push(ShardInfo {
            index: current.index,
            path: final_path,
            records: record_count,
        });
        self.current_records.clear();
        Ok(())
    }

    /// Discard the current temporary shard without registering it.
    fn abandon_current(&mut self) {
        if let Some(current) = self.current.take() {
            drop(current.wtr);
            let _ = fs::remove_file(&current.temp);
        }
    }

    fn temp_path(&self, index: usize) -> PathBuf {
        self.dir
            .join(format!(".{}-{:04}.tmp", self.prefix, index))
    }

    fn final_path(&self, index: usize) -> PathBuf {
        self.dir.join(format!("{}-{:04}.csv", self.prefix, index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_zero_is_rejected() {
        let result = ShardedWriter::builder("some-dir", "data")
            .capacity(0)
            .build();
        assert!(result.is_err());
    }
}
