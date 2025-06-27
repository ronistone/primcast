use crate::timed_print;

use super::*;
use lmdb::{Environment, Database, Transaction, WriteFlags, Cursor};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct LMDBPersistence {
    env: Environment,
    log_db: Database,
    metadata_db: Database,
}

impl LMDBPersistence {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, PersistenceError> {
        std::fs::create_dir_all(path.as_ref())?;
        
        let env = Environment::new()
            .set_max_readers(126)
            .set_max_dbs(2)
            .set_map_size(10 * 1024 * 1024 * 1024) // 1GB
            .set_flags(lmdb::EnvironmentFlags::NO_SYNC | lmdb::EnvironmentFlags::NO_LOCK)
            .open(path.as_ref())
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        let log_db = env.create_db(Some("log"), lmdb::DatabaseFlags::empty())
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        let metadata_db = env.create_db(Some("metadata"), lmdb::DatabaseFlags::empty())
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        Ok(Self {
            env,
            log_db,
            metadata_db,
        })
    }
    
    fn log_key(idx: u64) -> [u8; 8] {
        idx.to_be_bytes()
    }
    
    const METADATA_KEY: &'static [u8] = b"replica_metadata";
}

impl PersistenceLayer for LMDBPersistence {
    fn put_log_entry(&mut self, epoch: Epoch, idx: u64, entry: &LogEntry) -> Result<(), PersistenceError> {
        // timed_print!("[BEGIN-LMDB] Put log entry: idx={}, epoch={}", idx, epoch);
        let persisted_entry = PersistedLogEntry {
            epoch,
            idx,
            entry: entry.clone(),
        };
        
        let key = Self::log_key(idx);
        let value = bincode::serialize(&persisted_entry)?;
        
        let mut txn = self.env.begin_rw_txn()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        txn.put(self.log_db, &key, &value, WriteFlags::empty())
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        txn.commit()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;

        // timed_print!("[END-LMDB] Put log entry: idx={}, epoch={}", idx, epoch);
        
        Ok(())
    }
    
    fn get_log_entry(&self, idx: u64) -> Result<Option<(Epoch, LogEntry)>, PersistenceError> {
        let key = Self::log_key(idx);
        
        let txn = self.env.begin_ro_txn()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        match txn.get(self.log_db, &key) {
            Ok(bytes) => {
                let persisted: PersistedLogEntry = bincode::deserialize(bytes)?;
                Ok(Some((persisted.epoch, persisted.entry)))
            }
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(PersistenceError::Database(e.to_string())),
        }
    }
    
    fn list_log_entries(&self) -> Result<Vec<PersistedLogEntry>, PersistenceError> {
        let mut entries = Vec::new();
        
        let txn = self.env.begin_ro_txn()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        let mut cursor = txn.open_ro_cursor(self.log_db)
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        for (_, value) in cursor.iter() {
            let persisted: PersistedLogEntry = bincode::deserialize(value)?;
            entries.push(persisted);
        }
        
        // Sort by index to ensure correct order
        entries.sort_by_key(|e| e.idx);
        Ok(entries)
    }
    
    fn truncate_log(&mut self, from_idx: u64) -> Result<(), PersistenceError> {
        let mut txn = self.env.begin_rw_txn()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        let mut cursor = txn.open_rw_cursor(self.log_db)
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        let mut keys_to_delete = Vec::new();
        
        for (key, _) in cursor.iter() {
            let idx = u64::from_be_bytes(
                key.try_into()
                    .map_err(|_| PersistenceError::Database("Invalid key format".to_string()))?
            );
            
            if idx >= from_idx {
                keys_to_delete.push(key.to_vec());
            }
        }
        
        drop(cursor);
        
        for key in keys_to_delete {
            txn.del(self.log_db, &key, None)
                .map_err(|e| PersistenceError::Database(e.to_string()))?;
        }
        
        txn.commit()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        Ok(())
    }
    
    fn put_metadata(&mut self, metadata: &ReplicaMetadata) -> Result<(), PersistenceError> {
        let value = bincode::serialize(metadata)?;
        
        let mut txn = self.env.begin_rw_txn()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        txn.put(self.metadata_db, &Self::METADATA_KEY, &value, WriteFlags::empty())
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        txn.commit()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        Ok(())
    }
    
    fn get_metadata(&self) -> Result<Option<ReplicaMetadata>, PersistenceError> {
        let txn = self.env.begin_ro_txn()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        match txn.get(self.metadata_db, &Self::METADATA_KEY) {
            Ok(bytes) => {
                let metadata: ReplicaMetadata = bincode::deserialize(bytes)?;
                Ok(Some(metadata))
            }
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(PersistenceError::Database(e.to_string())),
        }
    }
    
    fn flush(&mut self) -> Result<(), PersistenceError> {
        self.env.sync(true)
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        Ok(())
    }
    
    fn close(&mut self) -> Result<(), PersistenceError> {
        // LMDB closes automatically when dropped
        Ok(())
    }
    
    fn clear_all(&mut self) -> Result<(), PersistenceError> {
        let mut txn = self.env.begin_rw_txn()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        // Clear log database
        {
            let mut cursor = txn.open_rw_cursor(self.log_db)
                .map_err(|e| PersistenceError::Database(e.to_string()))?;
            
            let mut keys_to_delete = Vec::new();
            for (key, _) in cursor.iter() {
                keys_to_delete.push(key.to_vec());
            }
            
            drop(cursor);
            
            for key in keys_to_delete {
                txn.del(self.log_db, &key, None)
                    .map_err(|e| PersistenceError::Database(e.to_string()))?;
            }
        }
        
        // Clear metadata
        match txn.del(self.metadata_db, &Self::METADATA_KEY, None) {
            Ok(_) => {},
            Err(lmdb::Error::NotFound) => {}, // OK if not found
            Err(e) => return Err(PersistenceError::Database(e.to_string())),
        }
        
        txn.commit()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        Ok(())
    }
}