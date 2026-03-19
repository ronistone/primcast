use super::*;
use rocksdb::{DB, Options, ColumnFamilyDescriptor, WriteBatch};
use std::path::Path;

pub struct RocksDBPersistence {
    db: DB,
}

impl RocksDBPersistence {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, PersistenceError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
        opts.set_max_write_buffer_number(3);
        opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB
        opts.set_level_zero_file_num_compaction_trigger(4);
        
        let cfs = vec![
            ColumnFamilyDescriptor::new("log", Options::default()),
            ColumnFamilyDescriptor::new("metadata", Options::default()),
        ];
        
        let db = DB::open_cf_descriptors(&opts, path, cfs)
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
            
        Ok(Self { db })
    }
    
    fn log_cf(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle("log").expect("log column family should exist")
    }
    
    fn metadata_cf(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle("metadata").expect("metadata column family should exist")
    }
    
    fn log_key(idx: u64) -> [u8; 8] {
        idx.to_be_bytes()
    }
    
    const METADATA_KEY: &'static [u8] = b"replica_metadata";
}

impl PersistenceLayer for RocksDBPersistence {
    fn put_log_entry(&mut self, epoch: Epoch, idx: u64, entry: &LogEntry) -> Result<(), PersistenceError> {
        // timed_print!("[BEGIN-rocks] Put log entry: idx={}, epoch={}", idx, epoch);
        let persisted_entry = PersistedLogEntry {
            epoch,
            idx,
            entry: entry.clone(),
        };
        
        let key = Self::log_key(idx);
        let value = bincode::serialize(&persisted_entry)?;
        
        self.db.put_cf(self.log_cf(), key, value)
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        // timed_print!("[END-Rocks] Put log entry: idx={}, epoch={}", idx, epoch);
        Ok(())
    }
    
    fn get_log_entry(&self, idx: u64) -> Result<Option<(Epoch, LogEntry)>, PersistenceError> {
        let key = Self::log_key(idx);
        
        match self.db.get_cf(self.log_cf(), key)
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
        let iter = self.db.iterator_cf(self.log_cf(), rocksdb::IteratorMode::Start);
        
        for item in iter {
            let (_, value) = item.map_err(|e| PersistenceError::Database(e.to_string()))?;
            let persisted: PersistedLogEntry = bincode::deserialize(&value)?;
            entries.push(persisted);
        }
        
        // Sort by index to ensure correct order
        entries.sort_by_key(|e| e.idx);
        Ok(entries)
    }
    
    fn truncate_log(&mut self, from_idx: u64) -> Result<(), PersistenceError> {
        let mut batch = WriteBatch::default();
        let iter = self.db.iterator_cf(self.log_cf(), rocksdb::IteratorMode::Start);
        
        for item in iter {
            let (key, _) = item.map_err(|e| PersistenceError::Database(e.to_string()))?;
            let idx = u64::from_be_bytes(
                key.as_ref().try_into()
                    .map_err(|_| PersistenceError::Database("Invalid key format".to_string()))?
            );
            
            if idx >= from_idx {
                batch.delete_cf(self.log_cf(), key);
            }
        }
        
        self.db.write(batch)
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        Ok(())
    }
    
    fn put_metadata(&mut self, metadata: &ReplicaMetadata) -> Result<(), PersistenceError> {
        let value = bincode::serialize(metadata)?;
        self.db.put_cf(self.metadata_cf(), Self::METADATA_KEY, value)
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        Ok(())
    }
    
    fn get_metadata(&self) -> Result<Option<ReplicaMetadata>, PersistenceError> {
        match self.db.get_cf(self.metadata_cf(), Self::METADATA_KEY)
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
        // RocksDB closes automatically when dropped
        Ok(())
    }
    
    fn clear_all(&mut self) -> Result<(), PersistenceError> {
        // Clear log column family
        let iter = self.db.iterator_cf(self.log_cf(), rocksdb::IteratorMode::Start);
        let mut batch = WriteBatch::default();
        
        for item in iter {
            let (key, _) = item.map_err(|e| PersistenceError::Database(e.to_string()))?;
            batch.delete_cf(self.log_cf(), key);
        }
        
        // Clear metadata column family
        batch.delete_cf(self.metadata_cf(), Self::METADATA_KEY);
        
        self.db.write(batch)
            .map_err(|e| PersistenceError::Database(e.to_string()))?;
        
        Ok(())
    }
}