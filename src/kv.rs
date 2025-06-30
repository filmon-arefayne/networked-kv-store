use crate::{KvsError, error::Result};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::ffi::OsStr;

use std::io::SeekFrom;
use std::ops::Range;
use std::path::PathBuf;
use std::{
    collections::{BTreeMap, HashMap},
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, Write},
    path::Path,
};

#[derive(Deserialize, Serialize, Debug)]
enum LogEntry {
    Set { key: String, value: String },
    Remove { key: String },
}

impl LogEntry {
    fn set(key: String, value: String) -> Self {
        LogEntry::Set { key, value }
    }
    fn remove(key: String) -> Self {
        LogEntry::Remove { key }
    }
}

/// json serialised command position and length
struct CommandPos {
    generation: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((generation, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            generation,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}
/// A key-value store that persists data to disk
pub struct KvStore {
    path: PathBuf,
    // Map of reader IDs to buffered readers with position tracking
    readers: HashMap<u64, BufReaderWithPos<File>>,
    // writer of the current log file
    writer: BufWriterWithPos<File>,
    current_generation: u64,

    index: BTreeMap<String, CommandPos>,
    // number of stale commands that can be deleted during compaction
    uncompacted: u64,
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.stream_position()?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes_read = self.reader.read(buf)?;
        self.pos += bytes_read as u64;
        Ok(bytes_read)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}
struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.stream_position()?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes_written = self.writer.write(buf)?;
        self.pos += bytes_written as u64;
        Ok(bytes_written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

const COMPACTION_THRESHOLD: u64 = 1024 * 1024; // 1 MB

impl KvStore {
    /// Gets a value by key
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            let reader = self
                .readers
                .get_mut(&cmd_pos.generation)
                .expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let cmd_reader = reader.take(cmd_pos.len);
            if let LogEntry::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                return Ok(Some(value));
            } else {
                return Err(KvsError::UnexpectedCommandType);
            }
        }

        Ok(None)
    }

    /// Sets a value for a key
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let entry = LogEntry::set(key, value);
        let pos = self.writer.pos;

        serde_json::to_writer(&mut self.writer, &entry)?;
        self.writer.flush()?;
        if let LogEntry::Set { key, .. } = entry {
            if let Some(old_entry) = self
                .index
                .insert(key, (self.current_generation, pos..self.writer.pos).into())
            {
                self.uncompacted += old_entry.len;
            }
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    /// Removes a key and its associated value
    /// Returns an error if the key doesn't exist
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let entry = LogEntry::remove(key);
            serde_json::to_writer(&mut self.writer, &entry)?;
            self.writer.flush()?;
            if let LogEntry::Remove { key, .. } = entry {
                let old_cmd = self.index.remove(&key).expect("key not found");
                self.uncompacted += old_cmd.len;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// Opens a KvStore at a given directory path
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        std::fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();
        let generation_list = sorted_generation_list(&path)?;
        let mut uncompacted = 0;

        for &generation in &generation_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, generation))?)?;
            uncompacted += load(generation, &mut reader, &mut index)?;
            readers.insert(generation, reader);
        }
        let current_generation = generation_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, current_generation, &mut readers)?;

        Ok(KvStore {
            path,
            readers,
            writer,
            current_generation,
            index,
            uncompacted,
        })
    }

    /// Compacts the log by removing redundant entries
    fn compact(&mut self) -> Result<()> {
        let compaction_generation = self.current_generation + 1;
        self.current_generation += 2;
        self.writer = self.new_log_file(self.current_generation)?;

        let mut compaction_writer = self.new_log_file(compaction_generation)?;
        let mut new_pos = 0;
        for cmd_pos in &mut self.index.values_mut() {
            let reader = self
                .readers
                .get_mut(&cmd_pos.generation)
                .expect("Cannot find log reader");
            if reader.pos != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }
            let mut entry_reader = reader.take(cmd_pos.len);
            let len = std::io::copy(&mut entry_reader, &mut compaction_writer)?;
            *cmd_pos = (compaction_generation, new_pos..new_pos + len).into();
            new_pos += len;
        }
        compaction_writer.flush()?;

        let stale_gens: Vec<_> = self
            .readers
            .keys()
            .filter(|&&generation| generation < compaction_generation)
            .cloned()
            .collect();
        for stale_gen in stale_gens {
            self.readers.remove(&stale_gen);
            std::fs::remove_file(log_path(&self.path, stale_gen))?;
        }
        self.uncompacted = 0;
        Ok(())
    }

    /// Create a new log file
    fn new_log_file(&mut self, generation: u64) -> Result<BufWriterWithPos<File>> {
        new_log_file(&self.path, generation, &mut self.readers)
    }
}

/// New log file, updates the map with the reader
/// and returns the writer to the log
fn new_log_file(
    path: &Path,
    generation: u64,
    readers: &mut HashMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(path, generation);
    let writer = BufWriterWithPos::new(OpenOptions::new().create(true).append(true).open(&path)?)?;
    readers.insert(generation, BufReaderWithPos::new(File::open(&path)?)?);
    Ok(writer)
}

fn log_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("{generation}.log"))
}

/// generate a sorted list of generations from the log files in the given path
fn sorted_generation_list(path: &Path) -> Result<Vec<u64>> {
    let mut generations: Vec<u64> = std::fs::read_dir(path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    generations.sort_unstable();
    Ok(generations)
}

/// Load the whole log file and store value locations in the index map
///
fn load(
    generation: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, CommandPos>,
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<LogEntry>();
    let mut uncompacted = 0;
    while let Some(entry) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match entry? {
            LogEntry::Set { key, .. } => {
                if let Some(old_entry) = index.insert(key, (generation, pos..new_pos).into()) {
                    uncompacted += old_entry.len;
                }
            }
            LogEntry::Remove { key } => {
                if let Some(old_entry) = index.remove(&key) {
                    uncompacted += old_entry.len;
                }
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}
