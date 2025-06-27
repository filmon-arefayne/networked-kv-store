use std::collections::HashMap;

/// inner kvstore
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    /// Creates a new KvStore instance
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Gets a value by key
    pub fn get(&self, key: &str) -> Option<String> {
        self.map.get(key).cloned()
    }

    /// Sets a value for a key
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// Removes a key and its associated value
    pub fn remove(&mut self, key: &str) {
        self.map.remove(key);
    }
}
