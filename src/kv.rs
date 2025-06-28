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
    log_path: Box<Path>,
}

impl KvStore {
    /// Creates a new KvStore instance with the default log path
    pub fn new() -> Result<Self, std::io::Error> {
        let path = std::env::current_dir()?.join("kvstore.log");
        Self::open(path.as_path())
    }

    /// Gets a value by key
    pub fn get(&self, key: String) -> Option<String> {
        self.map.get(&key).cloned()
    }

    /// Sets a value for a key
    pub fn set(&mut self, key: String, value: String) -> Result<(), std::io::Error> {
        let entry = LogEntry::Set {
            key: key.clone(),
            value: value.clone(),
        };

        // Append the entry to the log file
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;

        writeln!(file, "{}", serde_json::to_string(&entry)?)?;

        // Update in-memory map
        self.map.insert(key, value);

        // Check if compaction is needed
        // Compact if file size is larger than 1MB
        if self.log_size()? > 1024 * 1024 {
            self.compact()?;
        }

        Ok(())
    }

    /// Removes a key and its associated value
    /// Returns an error if the key doesn't exist
    pub fn remove(&mut self, key: String) -> Result<(), std::io::Error> {
        if !self.map.contains_key(&key) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Key not found",
            ));
        }

        let entry = LogEntry::Remove { key: key.clone() };

        // Append the removal entry to the log file
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;

        writeln!(file, "{}", serde_json::to_string(&entry)?)?;

        // Update in-memory map
        self.map.remove(&key);
        Ok(())
    }

    /// Opens a KvStore from a file path
    pub fn open(path: &Path) -> Result<KvStore, std::io::Error> {
        let mut store = KvStore {
            map: HashMap::new(),
            log_path: path.into(),
        };

        // Ensure the parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Create file if it doesn't exist
        if !path.exists() {
            File::create(path)?;
            return Ok(store);
        }

        // Read the log file and replay all operations
        let file = File::open(path)?;
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
    fn compact(&mut self) -> Result<(), std::io::Error> {
        let temp_path = self.log_path.with_extension("tmp");
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
        std::fs::rename(temp_path, &self.log_path)?;

        Ok(())
    }

    /// Returns the size of the log file in bytes
    fn log_size(&self) -> Result<u64, std::io::Error> {
        Ok(std::fs::metadata(&self.log_path)?.len())
    }
}
