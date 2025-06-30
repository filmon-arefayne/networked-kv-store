use std::fmt::Display;

/// domain error for the key-value store
#[derive(Debug)]
pub enum KvsError {
    /// Represents an I/O error
    IoError(std::io::Error),
    /// Represents a serialization/deserialization error
    /// using Serde for JSON handling
    SerdeError(serde_json::Error),
    /// Represents a key not found error
    KeyNotFound,
    /// Represents an unexpected error
    UnexpectedCommandType,
}

impl Display for KvsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KvsError::IoError(e) => write!(f, "I/O error: {e}"),
            KvsError::SerdeError(e) => write!(f, "Serialization error: {e}"),
            KvsError::KeyNotFound => write!(f, "Key not found"),
            KvsError::UnexpectedCommandType => write!(f, "Unexpected command type"),
        }
    }
}
impl From<std::io::Error> for KvsError {
    fn from(error: std::io::Error) -> Self {
        KvsError::IoError(error)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(error: serde_json::Error) -> Self {
        KvsError::SerdeError(error)
    }
}

/// Result type for the domain Error
pub type Result<T> = std::result::Result<T, KvsError>;
