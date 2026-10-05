// 分片写出组件的定向验收测试。
//
// 定向命令：
//   cargo test --test shard
//   cargo run --example csv-shard -- target/csv-shard-example
//
// 验收点：
// 1. 容量 2 的五条记录生成三片，每片独立带表头；
// 2. 带内嵌换行的引用字段记录不被拆开；
// 3. 第三片写失败时，前两片与清单仍一致，临时文件被清理；
// 4. 重试不重复登记旧片，只补写缺失的第三片。

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use csv::shard::{Assignment, Sharder};
use csv::{ByteRecord, ReaderBuilder};

/// 故障 writer：当分片号等于 `fail_shard` 时，所有写/flush 都报错。
struct FailWriter {
    inner: File,
    fail: bool,
}

impl Write for FailWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.fail {
            return Err(io::Error::new(io::ErrorKind::Other, "injected write failure"));
        }
        self.inner.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.fail {
            return Err(io::Error::new(io::ErrorKind::Other, "injected flush failure"));
        }
        self.inner.flush()
    }
}

fn header() -> ByteRecord {
    ByteRecord::from(vec!["id", "name", "note"])
}

/// 五条记录，其中第 4 条（下标 3）的字段内嵌换行。
fn records() -> Vec<ByteRecord> {
    vec![
        ByteRecord::from(vec!["1", "alice", "plain"]),
        ByteRecord::from(vec!["2", "bob", "quoted,\"comma\""]),
        ByteRecord::from(vec!["3", "carol", "line one\nline two\nline three"]),
        ByteRecord::from(vec!["4", "dave", "tail"]),
        ByteRecord::from(vec!["5", "erin", "last"]),
    ]
}

fn test_dir(name: &str) -> PathBuf {
    let dir = Path::new("target").join("shard-test").join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn read_shard(dir: &Path, index: usize) -> (csv::StringRecord, Vec<csv::StringRecord>) {
    let path = dir.join(format!("shard-{:05}.csv", index));
    let mut rdr = ReaderBuilder::new().has_headers(true).from_path(&path).unwrap();
    let headers = rdr.headers().unwrap().clone();
    let rows = rdr.records().collect::<Result<Vec<_>, _>>().unwrap();
    (headers, rows)
}

fn part_files(dir: &Path) -> Vec<PathBuf> {
    let tmp = dir.join(".tmp");
    if !tmp.exists() {
        return vec![];
    }
    fs::read_dir(tmp)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map_or(false, |e| e == "part"))
        .collect()
}

#[test]
fn capacity_two_five_records_makes_three_shards() {
    let dir = test_dir("three-shards");
    let mut sharder = Sharder::in_dir(&dir, &header(), 2).unwrap();

    let mut assignments = vec![];
    for rec in &records() {
        assignments.push(sharder.write_record(rec).unwrap());
    }
    let manifest = sharder.finish().unwrap();

    // 归属：原记录号 -> (片号, 片内记录号)。
    assert_eq!(
        assignments,
        vec![
            Assignment { record: 0, shard: 0, row: 0 },
            Assignment { record: 1, shard: 0, row: 1 },
            Assignment { record: 2, shard: 1, row: 0 },
            Assignment { record: 3, shard: 1, row: 1 },
            Assignment { record: 4, shard: 2, row: 0 },
        ]
    );

    // 三片，记录数 2/2/1，清单只含这三条。
    assert_eq!(manifest.len(), 3);
    assert_eq!(manifest[0].records, 2);
    assert_eq!(manifest[1].records, 2);
    assert_eq!(manifest[2].records, 1);

    // 每片独立带表头。
    for shard in 0..3 {
        let (headers, rows) = read_shard(&dir, shard);
        assert_eq!(headers, csv::StringRecord::from(vec!["id", "name", "note"]));
        assert_eq!(rows.len(), manifest[shard].records);
    }

    // 内嵌换行的记录（id=3 的记录，落在第 2 片第 1 行）不被拆开：
    // 第 2 片解析后恰好 2 条记录，且字段保留完整的三行内容。
    let (_headers, rows) = read_shard(&dir, 1);
    assert_eq!(rows[0].get(0), Some("3"));
    assert_eq!(rows[0].get(2), Some("line one\nline two\nline three"));

    // 发布完成后没有遗留临时文件。
    assert!(part_files(&dir).is_empty());
}

#[test]
fn failed_third_shard_keeps_published_shards_and_manifest() {
    let dir = test_dir("fail-third");
    let out_dir = dir.clone();
    let tmp_dir = dir.join(".tmp");
    // 故障注入：分片 2 的 writer 必定失败。
    let factory = |shard: usize, path: &Path| -> io::Result<FailWriter> {
        Ok(FailWriter { inner: File::create(path)?, fail: shard == 2 })
    };
    let mut sharder =
        Sharder::with_factory(out_dir.clone(), tmp_dir, &header(), 2, factory).unwrap();

    // 前四条（前两片）正常写出并发布。
    for rec in &records()[..4] {
        sharder.write_record(rec).unwrap();
    }
    // 第五条触发第三片：记录先进入 csv 缓冲区，注入故障在封存
    // （flush）时表面化，因此 finish 必须返回错误。
    sharder.write_record(&records()[4]).unwrap();
    assert!(sharder.finish().is_err(), "third shard seal must fail");

    // 清单仍只含前两片（磁盘上的 manifest.csv 是权威记录）。

    // 前两片已发布且内容完整，第三片没有正式文件。
    assert!(out_dir.join("shard-00000.csv").exists());
    assert!(out_dir.join("shard-00001.csv").exists());
    assert!(!out_dir.join("shard-00002.csv").exists());
    let (_h, rows0) = read_shard(&out_dir, 0);
    let (_h, rows1) = read_shard(&out_dir, 1);
    assert_eq!(rows0.len(), 2);
    assert_eq!(rows1.len(), 2);
    assert_eq!(rows1[0].get(2), Some("line one\nline two\nline three"));

    // 清单文件与内存一致：只有两条已确认发布记录。
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(out_dir.join("manifest.csv"))
        .unwrap();
    assert_eq!(rdr.records().count(), 2);

    // 失败片的临时文件已清理。
    assert!(part_files(&out_dir).is_empty());
}

#[test]
fn retry_after_failure_does_not_duplicate_entries() {
    let dir = test_dir("retry");
    let out_dir = dir.clone();
    let tmp_dir = dir.join(".tmp");

    // 第一次：第三片注入故障，只能发布前两片。
    {
        let factory = |shard: usize, path: &Path| -> io::Result<FailWriter> {
            Ok(FailWriter { inner: File::create(path)?, fail: shard == 2 })
        };
        let mut sharder = Sharder::with_factory(
            out_dir.clone(),
            tmp_dir.clone(),
            &header(),
            2,
            factory,
        )
        .unwrap();
        for rec in &records() {
            // 第五条所在的第三片在 finish 封存时失败。
            let _ = sharder.write_record(rec);
        }
        assert!(sharder.finish().is_err());
    }
    assert_eq!(
        std::fs::read_to_string(out_dir.join("manifest.csv"))
            .unwrap()
            .lines()
            .count(),
        3 // 表头 + 两条已发布分片
    );

    // 第二次（重试）：正常 writer，重新喂入全部五条记录。
    let mut sharder = Sharder::in_dir(&out_dir, &header(), 2).unwrap();
    let mut assignments = vec![];
    for rec in &records() {
        assignments.push(sharder.write_record(rec).unwrap());
    }
    let manifest = sharder.finish().unwrap();

    // 归属映射仍然覆盖全部五条记录。
    assert_eq!(
        assignments,
        vec![
            Assignment { record: 0, shard: 0, row: 0 },
            Assignment { record: 1, shard: 0, row: 1 },
            Assignment { record: 2, shard: 1, row: 0 },
            Assignment { record: 3, shard: 1, row: 1 },
            Assignment { record: 4, shard: 2, row: 0 },
        ]
    );

    // 清单恰好三条，没有重复登记旧片。
    assert_eq!(manifest.len(), 3);
    assert_eq!(manifest[0].index, 0);
    assert_eq!(manifest[1].index, 1);
    assert_eq!(manifest[2].index, 2);
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(out_dir.join("manifest.csv"))
        .unwrap();
    assert_eq!(rdr.records().count(), 3);

    // 旧片未被重写：内容与前一次发布一致；第三片补齐，含表头 + 1 条记录。
    let (_h, rows1) = read_shard(&out_dir, 1);
    assert_eq!(rows1[0].get(2), Some("line one\nline two\nline three"));
    let (headers2, rows2) = read_shard(&out_dir, 2);
    assert_eq!(headers2, csv::StringRecord::from(vec!["id", "name", "note"]));
    assert_eq!(rows2.len(), 1);
    assert_eq!(rows2[0].get(0), Some("5"));
    assert!(part_files(&out_dir).is_empty());
}
