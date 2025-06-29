use crate::{KvsError, error::Result};

use serde::{Deserialize, Serialize};

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::Path,
};

#[derive(Deserialize, Serialize, Debug)]
enum LogEntry {
    Get { key: String },
    Set { key: String, value: String },
    Remove { key: String },
}

/// A key-value store that persists data to disk
#[derive(Debug)]
pub struct KvStore {
    map: HashMap<String, String>,
    dir_path: Box<Path>,
}

const COMPACTION_SIZE: u64 = 1024 * 1024; // 1 MB

impl KvStore {
    /// Creates a new KvStore instance with the default path (current directory)
    pub fn new() -> Result<Self> {
        Self::open(std::path::Path::new("."))
    }

    fn get_log_path(&self) -> std::path::PathBuf {
        self.dir_path.join("kvstore.log")
    }

    fn get_temp_log_path(&self) -> std::path::PathBuf {
        self.dir_path.join("kvstore.log.tmp")
    }

    /// Gets a value by key
    pub fn get(&self, key: String) -> Option<String> {
        self.map.get(&key).cloned()
    }

    /// Sets a value for a key
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let entry = LogEntry::Set {
            key: key.clone(),
            value: value.clone(),
        };

        // Append the entry to the log file
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.get_log_path())?;

        writeln!(file, "{}", serde_json::to_string(&entry)?)?;

        // Update in-memory map
        self.map.insert(key, value);

        // Check if compaction is needed
        // Compact if file size is larger than 1MB
        if self.log_size()? > COMPACTION_SIZE {
            self.compact()?;
        }

        Ok(())
    }

    /// Removes a key and its associated value
    /// Returns an error if the key doesn't exist
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.map.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }

        let entry = LogEntry::Remove { key: key.clone() };

        // Append the removal entry to the log file
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.get_log_path())?;

        writeln!(file, "{}", serde_json::to_string(&entry)?)?;

        // Update in-memory map
        self.map.remove(&key);
        Ok(())
    }

    /// Opens a KvStore at a given directory path
    pub fn open(path: &Path) -> Result<KvStore> {
        // Convert to absolute path if it's relative
        let abs_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };

        // Ensure the directory exists
        std::fs::create_dir_all(&abs_path)?;

        let mut store = KvStore {
            map: HashMap::new(),
            dir_path: abs_path.into(),
        };

        let log_path = store.get_log_path();

        // Create file if it doesn't exist
        if !log_path.exists() {
            File::create(&log_path)?;
            return Ok(store);
        }

        // Read the log file and replay all operations
        let file = File::open(&log_path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            if let Ok(entry) = serde_json::from_str::<LogEntry>(&line) {
                match entry {
                    LogEntry::Set { key, value } => {
                        store.map.insert(key, value);
                    }
                    LogEntry::Remove { key } => {
                        store.map.remove(&key);
                    }
                    _ => {}
                }
            }
        }

        Ok(store)
    }

    /// Compacts the log by removing redundant entries
    fn compact(&mut self) -> Result<()> {
        let temp_path = self.get_temp_log_path();
        let mut temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&temp_path)?;

        // Write current state to temp file
        for (key, value) in &self.map {
            let entry = LogEntry::Set {
                key: key.clone(),
                value: value.clone(),
            };
            writeln!(temp_file, "{}", serde_json::to_string(&entry)?)?;
        }

        // Replace old log with new one
        std::fs::rename(temp_path, self.get_log_path())?;

        Ok(())
    }

    /// Returns the size of the log file in bytes
    fn log_size(&self) -> Result<u64> {
        Ok(std::fs::metadata(self.get_log_path())?.len())
    }
}
