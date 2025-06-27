use crate::timed_print;
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;
use sled::{Db, Tree};
use std::path::Path;

pub struct SledPersistence {
    db: Db,
    log_tree: Tree,
    metadata_tree: Tree,
}

impl SledPersistence {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, PersistenceError> {
        let db = sled::open(path)
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        let log_tree = db.open_tree("log")
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        let metadata_tree = db.open_tree("metadata")
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        Ok(Self {
            db,
            log_tree,
            metadata_tree,
        })
    }
    
    fn log_key(idx: u64) -> [u8; 8] {
        idx.to_be_bytes()
    }
    
    const METADATA_KEY: &'static [u8] = b"replica_metadata";
}

impl PersistenceLayer for SledPersistence {
    fn put_log_entry(&mut self, epoch: Epoch, idx: u64, entry: &LogEntry) -> Result<(), PersistenceError> {
        // timed_print!("[BEGIN-Sled] Put log entry: idx={}, epoch={}", idx, epoch);
        let persisted_entry = PersistedLogEntry {
            epoch,
            idx,
            entry: entry.clone(),
        };
        
        let key = Self::log_key(idx);
        let value = bincode::serialize(&persisted_entry)?;
        
        self.log_tree.insert(key, value)
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        // timed_print!("[END-Sled] Put log entry: idx={}, epoch={}", idx, epoch);

        Ok(())
    }
    
    fn get_log_entry(&self, idx: u64) -> Result<Option<(Epoch, LogEntry)>, PersistenceError> {
        let key = Self::log_key(idx);
        
        match self.log_tree.get(key)
            .map_err(|e| PersistenceError::Database(e.to_string()))? {
            Some(bytes) => {
                let persisted: PersistedLogEntry = bincode::deserialize(&bytes)?;
                Ok(Some((persisted.epoch, persisted.entry)))
            }
            None => Ok(None),
        }
    }
    
    fn list_log_entries(&self) -> Result<Vec<PersistedLogEntry>, PersistenceError> {
        let mut entries = Vec::new();
        
        for item in self.log_tree.iter() {
            let (_, value) = item.map_err(|e| PersistenceError::Database(e.to_string()))?;
            let persisted: PersistedLogEntry = bincode::deserialize(&value)?;
            entries.push(persisted);
        }
        
        // Sort by index to ensure correct order
        entries.sort_by_key(|e| e.idx);
        Ok(entries)
    }
    
    fn truncate_log(&mut self, from_idx: u64) -> Result<(), PersistenceError> {
        let mut keys_to_remove = Vec::new();
        
        for item in self.log_tree.iter() {
            let (key, _) = item.map_err(|e| PersistenceError::Database(e.to_string()))?;
            let idx = u64::from_be_bytes(
                key.as_ref().try_into()
                    .map_err(|_| PersistenceError::Database("Invalid key format".to_string()))?
            );
            
            if idx >= from_idx {
                keys_to_remove.push(key.to_vec());
            }
        }
        
        for key in keys_to_remove {
            self.log_tree.remove(key)
                .map_err(|e| PersistenceError::Database(e.to_string()))?;
        }
        
        Ok(())
    }
    
    fn put_metadata(&mut self, metadata: &ReplicaMetadata) -> Result<(), PersistenceError> {
        let value = bincode::serialize(metadata)?;
        
        self.metadata_tree.insert(Self::METADATA_KEY, value)
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        Ok(())
    }
    
    fn get_metadata(&self) -> Result<Option<ReplicaMetadata>, PersistenceError> {
        match self.metadata_tree.get(Self::METADATA_KEY)
            .map_err(|e| PersistenceError::Database(e.to_string()))? {
            Some(bytes) => {
                let metadata: ReplicaMetadata = bincode::deserialize(&bytes)?;
                Ok(Some(metadata))
            }
            None => Ok(None),
        }
    }
    
    fn flush(&mut self) -> Result<(), PersistenceError> {
        self.db.flush()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        Ok(())
    }
    
    fn close(&mut self) -> Result<(), PersistenceError> {
        // Sled closes automatically when dropped
        Ok(())
    }
    
    fn clear_all(&mut self) -> Result<(), PersistenceError> {
        self.log_tree.clear()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        self.metadata_tree.clear()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        Ok(())
    }
}