//! 按记录数分片的 CSV 写出组件。
//!
//! [`Sharder`] 把一串 CSV 记录按固定容量切成若干片，每片独立携带表头，
//! 并维护一份只登记“已确认发布”数据的分片清单（manifest）。
//!
//! 设计要点：
//!
//! * 按记录切分，不按物理换行切分：分片边界只落在记录边界上，
//!   引用字段里内嵌的换行仍属于同一条记录，永远不会被拆开。
//! * 先写临时路径，成功后发布：每片先写入临时目录下的 `*.part`
//!   文件，`flush` 成功后才原子地 rename 到正式路径，随后才登记进清单。
//! * 失败不破坏已发布片：当前片写出失败时，仅清理该片的临时文件，
//!   之前已发布的片与清单保持一致，不被回滚或污染。
//! * 重试不重复登记：重新构造 [`Sharder`] 时会加载既有清单，
//!   重新喂入已发布片覆盖的记录时只返回归属、不再重写也不重复登记，
//!   只补写缺失的后续片。
//!
//! 每写入一条记录，[`Sharder::write_record`] 返回一个 [`Assignment`]，
//! 给出“原记录序号 -> （片号， 片内记录号）”的归属。片内记录号从 0 开始，
//! 不含表头行。
//!
//! 定向验收命令：
//!
//! ```text
//! cargo test --test shard
//! cargo run --example csv-shard -- target/csv-shard-example
//! ```
//!
//! # Example
//!
//! ```no_run
//! use csv::ByteRecord;
//! use csv::shard::Sharder;
//!
//! let header = ByteRecord::from(vec!["id", "note"]);
//! let mut sharder = Sharder::in_dir("target/shards", &header, 2)?;
//! let records = vec![
//!     ByteRecord::from(vec!["1", "plain"]),
//!     ByteRecord::from(vec!["2", "with\nembedded newline"]),
//!     ByteRecord::from(vec!["3", "tail"]),
//! ];
//! for rec in &records {
//!     let a = sharder.write_record(rec)?;
//!     println!("record {} -> shard {} row {}", a.record, a.shard, a.row);
//! }
//! let manifest = sharder.finish()?;
//! for entry in manifest {
//!     println!("shard {} -> {} ({} records)", entry.index, entry.file, entry.records);
//! }
//! # Ok::<(), std::io::Error>(())
//! ```

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use crate::{ByteRecord, ReaderBuilder, Writer, WriterBuilder};

/// 一条原始记录到分片内位置的归属映射。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Assignment {
    /// 原始记录的全局序号（从 0 开始，按喂入顺序）。
    pub record: usize,
    /// 所属分片号（从 0 开始）。
    pub shard: usize,
    /// 片内记录号（从 0 开始，不含表头行）。
    pub row: usize,
}

/// 清单中一个已确认发布的分片条目。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardEntry {
    /// 分片号。
    pub index: usize,
    /// 发布后的文件名（相对输出目录）。
    pub file: String,
    /// 片内数据记录数（不含表头）。
    pub records: usize,
}

/// 正在写出、尚未发布的当前分片。
struct CurrentShard<W: io::Write> {
    index: usize,
    tmp_path: PathBuf,
    writer: Writer<W>,
    rows: usize,
}

/// 按记录数分片的 CSV 写出器。
///
/// `W` 是底层 writer 类型，`F` 是“为某一片的临时路径创建 writer”的工厂。
/// 通过自定义工厂可以注入故障 writer（见 `tests/shard.rs`）。
pub struct Sharder<W, F>
where
    W: io::Write,
    F: FnMut(usize, &Path) -> io::Result<W>,
{
    out_dir: PathBuf,
    tmp_dir: PathBuf,
    header: ByteRecord,
    capacity: usize,
    factory: F,
    /// 只登记已确认发布分片的清单。
    manifest: Vec<ShardEntry>,
    current: Option<CurrentShard<W>>,
    /// 下一条待喂入记录的全局序号。
    next_record: usize,
}

impl Sharder<File, fn(usize, &Path) -> io::Result<File>> {
    /// 在 `dir` 下创建一个分片写出器，每片最多 `capacity` 条记录。
    ///
    /// 临时文件写入 `dir/.tmp`，发布后的分片与清单位于 `dir` 下。
    pub fn in_dir<P: AsRef<Path>>(
        dir: P,
        header: &ByteRecord,
        capacity: usize,
    ) -> io::Result<Self> {
        let out_dir = dir.as_ref().to_path_buf();
        let tmp_dir = out_dir.join(".tmp");
        Sharder::with_factory(out_dir, tmp_dir, header, capacity, |_shard, path| {
            File::create(path)
        })
    }
}

impl<W, F> Sharder<W, F>
where
    W: io::Write,
    F: FnMut(usize, &Path) -> io::Result<W>,
{
    /// 完整构造器：显式指定输出目录、临时目录与 writer 工厂。
    ///
    /// 工厂签名为 `(分片号, 临时路径) -> io::Result<writer>`，
    /// 测试可借此注入按分片号失败的故障 writer。
    pub fn with_factory(
        out_dir: PathBuf,
        tmp_dir: PathBuf,
        header: &ByteRecord,
        capacity: usize,
        factory: F,
    ) -> io::Result<Self> {
        if capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shard capacity must be at least 1",
            ));
        }
        fs::create_dir_all(&out_dir)?;
        fs::create_dir_all(&tmp_dir)?;
        // 清理上一次失败遗留的临时文件，保证重试从干净状态开始。
        for entry in fs::read_dir(&tmp_dir)? {
            let path = entry?.path();
            if path.extension().map_or(false, |ext| ext == "part") {
                fs::remove_file(&path)?;
            }
        }
        let manifest = load_manifest(&out_dir)?;
        Ok(Sharder {
            out_dir,
            tmp_dir,
            header: header.clone(),
            capacity,
            factory,
            manifest,
            current: None,
            next_record: 0,
        })
    }

    /// 写入一条记录，返回它归属的（片号， 片内记录号）。
    ///
    /// 若该记录落在已发布的分片内（重试场景），只返回归属、不重复写出。
    /// 当前片写满时会立即封存：flush 成功 -> 发布 -> 登记清单。
    /// 写出失败时仅清理当前片临时文件，已发布片与清单不受影响。
    pub fn write_record(&mut self, record: &ByteRecord) -> io::Result<Assignment> {
        let shard = self.next_record / self.capacity;
        let row = self.next_record % self.capacity;
        let assignment = Assignment { record: self.next_record, shard, row };

        if shard < self.manifest.len() {
            // 已确认发布的分片：重试时跳过，不重复登记。
            self.next_record += 1;
            return Ok(assignment);
        }

        if self.current.is_none() {
            self.open_shard(shard)?;
        }
        let result = (|| {
            let cur = self.current.as_mut().expect("current shard just opened");
            cur.writer.write_byte_record(record)?;
            cur.rows += 1;
            if cur.rows == self.capacity {
                self.seal_current()?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.next_record += 1;
                Ok(assignment)
            }
            Err(err) => {
                self.abort_current();
                Err(err)
            }
        }
    }

    /// 封存未写满的尾片并返回当前清单（只含已确认发布的分片）。
    pub fn finish(mut self) -> io::Result<Vec<ShardEntry>> {
        if let Err(err) = self.seal_current() {
            self.abort_current();
            return Err(err);
        }
        Ok(self.manifest)
    }

    /// 当前清单的快照（只含已确认发布的分片）。
    pub fn manifest(&self) -> &[ShardEntry] {
        &self.manifest
    }

    /// 为 `shard` 号分片打开临时文件并写入表头。
    fn open_shard(&mut self, shard: usize) -> io::Result<()> {
        let tmp_path = self.tmp_dir.join(format!("shard-{:05}.csv.part", shard));
        let inner = (self.factory)(shard, &tmp_path)?;
        let mut writer = WriterBuilder::new().has_headers(false).from_writer(inner);
        writer.write_byte_record(&self.header)?;
        self.current = Some(CurrentShard { index: shard, tmp_path, writer, rows: 0 });
        Ok(())
    }

    /// 封存当前片：flush -> 发布（rename） -> 登记清单。
    fn seal_current(&mut self) -> io::Result<()> {
        let cur = match self.current.take() {
            Some(cur) => cur,
            None => return Ok(()),
        };
        let mut writer = cur.writer;
        if let Err(err) = writer.flush() {
            // 封存失败：清理该片的临时文件，已发布片与清单不受影响。
            drop(writer);
            let _ = fs::remove_file(&cur.tmp_path);
            return Err(err);
        }
        drop(writer);
        let file = format!("shard-{:05}.csv", cur.index);
        fs::rename(&cur.tmp_path, self.out_dir.join(&file))?;
        self.manifest.push(ShardEntry { index: cur.index, file, records: cur.rows });
        self.persist_manifest()?;
        Ok(())
    }

    /// 当前片失败：丢弃 writer 并删除临时文件，已发布片不受影响。
    fn abort_current(&mut self) {
        if let Some(cur) = self.current.take() {
            drop(cur.writer);
            let _ = fs::remove_file(&cur.tmp_path);
        }
    }

    /// 把清单原子地写回输出目录（先写临时文件再 rename）。
    fn persist_manifest(&self) -> io::Result<()> {
        let tmp = self.tmp_dir.join("manifest.csv.part");
        let mut wtr = WriterBuilder::new().has_headers(false).from_path(&tmp)?;
        wtr.write_record(&["shard", "file", "records"])?;
        for entry in &self.manifest {
            wtr.write_record(&[
                entry.index.to_string(),
                entry.file.clone(),
                entry.records.to_string(),
            ])?;
        }
        wtr.flush()?;
        drop(wtr);
        fs::rename(&tmp, self.out_dir.join("manifest.csv"))?;
        Ok(())
    }
}

/// 从输出目录加载清单；只保留对应分片文件确实存在的条目。
fn load_manifest(out_dir: &Path) -> io::Result<Vec<ShardEntry>> {
    let path = out_dir.join("manifest.csv");
    if !path.exists() {
        return Ok(vec![]);
    }
    let mut rdr = ReaderBuilder::new().has_headers(true).from_path(&path)?;
    let mut entries = vec![];
    for result in rdr.records() {
        let record = result?;
        let parse = |field: &str| {
            field
                .parse::<usize>()
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
        };
        let entry = ShardEntry {
            index: parse(&record[0])?,
            file: record[1].to_string(),
            records: parse(&record[2])?,
        };
        if out_dir.join(&entry.file).exists() {
            entries.push(entry);
        }
    }
    entries.sort_by_key(|entry| entry.index);
    Ok(entries)
}
