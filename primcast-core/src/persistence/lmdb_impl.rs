
use super::*;
use lmdb::{Environment, Database, Transaction, WriteFlags, Cursor};
use std::path::Path;

pub struct LMDBPersistence {
    env: Environment,
    log_db: Database,
    metadata_db: Database,
    /// Write-behind buffer for log entries. One rw txn per appended entry made
    /// persistence the dominant cost of both the append and the delivery path
    /// (both run while the replica lock is held); entries are batched into a
    /// single txn instead. Reads consult the buffer first, and it is committed
    /// on flush/close and before any read-modify operation on the log db.
    /// The env already runs with NO_SYNC, so this does not weaken durability
    /// beyond what the configuration already accepts.
    write_buf: std::collections::BTreeMap<u64, Vec<u8>>,
}

/// Entries buffered before a commit is forced.
const WRITE_BUF_LIMIT: usize = 256;

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
            write_buf: Default::default(),
        })
    }

    /// Commit everything buffered in one transaction.
    fn commit_buffer(&mut self) -> Result<(), PersistenceError> {
        if self.write_buf.is_empty() {
            return Ok(());
        }
        let mut txn = self
            .env
            .begin_rw_txn()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        for (idx, value) in &self.write_buf {
            txn.put(self.log_db, &Self::log_key(*idx), value, WriteFlags::empty())
                .map_err(|e| PersistenceError::Database(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        self.write_buf.clear();
        Ok(())
    }
    
    fn log_key(idx: u64) -> [u8; 8] {
        idx.to_be_bytes()
    }
    
    const METADATA_KEY: &'static [u8] = b"replica_metadata";
}

impl PersistenceLayer for LMDBPersistence {
    fn put_log_entry(&mut self, epoch: Epoch, idx: u64, entry: &LogEntry) -> Result<(), PersistenceError> {
        let persisted_entry = PersistedLogEntry {
            epoch,
            idx,
            entry: entry.clone(),
        };
        let value = bincode::serialize(&persisted_entry)?;
        self.write_buf.insert(idx, value);
        if self.write_buf.len() >= WRITE_BUF_LIMIT {
            self.commit_buffer()?;
        }
        Ok(())
    }

    fn get_log_entry(&self, idx: u64) -> Result<Option<(Epoch, LogEntry)>, PersistenceError> {
        if let Some(value) = self.write_buf.get(&idx) {
            let persisted: PersistedLogEntry = bincode::deserialize(value)?;
            return Ok(Some((persisted.epoch, persisted.entry)));
        }
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
        for value in self.write_buf.values() {
            entries.push(bincode::deserialize::<PersistedLogEntry>(value)?);
        }
        
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

    fn count_log_entries(&self) -> Result<usize, PersistenceError> {

        // Count keys via a raw cursor scan — no bincode deserialization, no Vec
        // materialization/sort. Unlike `list_log_entries`, cost stays cheap even
        // as the log grows into the hundreds of thousands of entries.
        let txn = self.env.begin_ro_txn()
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        let mut cursor = txn.open_ro_cursor(self.log_db)
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        let committed = cursor.iter().count();
        // add buffered entries that are not in the db yet
        let buffered_new = self
            .write_buf
            .keys()
            .filter(|idx| txn.get(self.log_db, &Self::log_key(**idx)).is_err())
            .count();
        Ok(committed + buffered_new)
    }

    fn truncate_log(&mut self, from_idx: u64) -> Result<(), PersistenceError> {
        self.write_buf.retain(|idx, _| *idx < from_idx);
        self.commit_buffer()?;
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
        self.commit_buffer()?;
        self.env.sync(true)
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        Ok(())
    }
    
    fn close(&mut self) -> Result<(), PersistenceError> {
        // LMDB closes automatically when dropped, but buffered writes must land
        self.commit_buffer()?;
        Ok(())
    }
    
    fn clear_all(&mut self) -> Result<(), PersistenceError> {
        self.write_buf.clear();
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