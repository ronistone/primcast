use crate::types::*;
use crate::LogEntry;
use serde::{Deserialize, Serialize};

// #[cfg(feature = "rocksdb-persistence")]
pub mod rocksdb_impl;
// #[cfg(feature = "lmdb-persistence")]
pub mod lmdb_impl;
// #[cfg(feature = "sled-persistence")]
pub mod sled_impl;

// #[cfg(feature = "rocksdb-persistence")]
pub use rocksdb_impl::RocksDBPersistence;
// #[cfg(feature = "lmdb-persistence")]
pub use lmdb_impl::LMDBPersistence;
// #[cfg(feature = "sled-persistence")]
pub use sled_impl::SledPersistence;

#[derive(Debug)]
pub enum PersistenceError {
    Io(std::io::Error),
    Serialization(Box<bincode::ErrorKind>),
    NotFound,
    Database(String),
}

impl From<std::io::Error> for PersistenceError {
    fn from(err: std::io::Error) -> Self {
        PersistenceError::Io(err)
    }
}

impl From<Box<bincode::ErrorKind>> for PersistenceError {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        PersistenceError::Serialization(err)
    }
}

impl std::fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PersistenceError::Io(e) => write!(f, "IO error: {}", e),
            PersistenceError::Serialization(e) => write!(f, "Serialization error: {}", e),
            PersistenceError::NotFound => write!(f, "Not found"),
            PersistenceError::Database(e) => write!(f, "Database error: {}", e),
        }
    }
}

impl std::error::Error for PersistenceError {}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PersistedLogEntry {
    pub epoch: Epoch,
    pub idx: u64,
    pub entry: LogEntry,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReplicaMetadata {
    pub gid: Gid,
    pub pid: Pid,
    pub promised_epoch: Epoch,
    pub log_epochs: Vec<(Epoch, u64)>,
    pub safe_len: u64,
    pub clock: Clock,
}

pub trait PersistenceLayer: Send + Sync {
    // Log entry operations
    fn put_log_entry(&mut self, epoch: Epoch, idx: u64, entry: &LogEntry) -> Result<(), PersistenceError>;
    fn get_log_entry(&self, idx: u64) -> Result<Option<(Epoch, LogEntry)>, PersistenceError>;
    fn list_log_entries(&self) -> Result<Vec<PersistedLogEntry>, PersistenceError>;
    fn truncate_log(&mut self, from_idx: u64) -> Result<(), PersistenceError>;
    
    // Metadata operations
    fn put_metadata(&mut self, metadata: &ReplicaMetadata) -> Result<(), PersistenceError>;
    fn get_metadata(&self) -> Result<Option<ReplicaMetadata>, PersistenceError>;
    
    // Utility operations
    fn flush(&mut self) -> Result<(), PersistenceError>;
    fn close(&mut self) -> Result<(), PersistenceError>;
    fn clear_all(&mut self) -> Result<(), PersistenceError>;
}