// use clap::Parser;
// // use primcast_core::persistence::{PersistenceLayer, ReplicaMetadata, PersistedLogEntry};
// // use primcast_core::{LogEntry, types::Clock, types::MsgId, types::Gid, types::Pid, types::Epoch};
// // use bytes::Bytes;
// use primcast_core::{LogEntry, types::Clock, types::MsgId, types::Gid, types::Pid, types::Epoch};
// use bytes::Bytes;

// // Import persistence traits and types from the crate root or appropriate module
// use primcast_core::persistence::*;

// // Import specific persistence implementations
// // #[cfg(feature = "lmdb-persistence")]
// use primcast_core::LMDBPersistence;
// // #[cfg(feature = "rocksdb-persistence")]
// use primcast_core::RocksDBPersistence;
// // #[cfg(feature = "sled-persistence")]
// use primcast_core::SledPersistence;


// #[derive(Parser, Debug)]
// #[clap(author, version, about, long_about = None)]
// struct Args {
//     #[clap(long, short)]
//     database: String,
    
//     #[clap(long, short, default_value = "lmdb")]
//     backend: String, // lmdb, rocksdb, sled
// }

// fn create_test_log_entry(idx: u64) -> LogEntry {
//     LogEntry {
//         local_ts: idx,
//         msg_id: idx as u128,
//         msg: Bytes::from(format!("Test message {}", idx)),
//         dest: vec![Gid(0), Gid(1)].into_iter().collect(),
//         final_ts: Some(idx + 100),
//     }
// }

// fn create_test_metadata() -> ReplicaMetadata {
//     ReplicaMetadata {
//         gid: Gid(0),
//         pid: Pid(1),
//         promised_epoch: Epoch::initial(),
//         log_epochs: vec![(Epoch::initial(), 0), (Epoch::initial().next_for(Pid(1)), 5)],
//         safe_len: 3,
//         clock: 42,
//     }
// }

// fn test_persistence_backend(mut persistence: Box<dyn PersistenceLayer>, backend_name: &str) -> Result<(), Box<dyn std::error::Error>> {
//     println!("Testing {} backend...", backend_name);
    
//     // Clear any existing data
//     persistence.clear_all()?;
    
//     // Test 1: Write and read log entries
//     println!("  Testing log entries...");
//     let test_entries = vec![
//         (Epoch::initial(), 0, create_test_log_entry(0)),
//         (Epoch::initial(), 1, create_test_log_entry(1)),
//         (Epoch::initial().next_for(Pid(1)), 2, create_test_log_entry(2)),
//     ];
    
//     // Write entries
//     for (epoch, idx, entry) in &test_entries {
//         persistence.put_log_entry(*epoch, *idx, entry)?;
//         println!("    Wrote entry {} for epoch {}", idx, epoch);
//     }
    
//     // Read entries back
//     for (expected_epoch, idx, expected_entry) in &test_entries {
//         if let Some((epoch, entry)) = persistence.get_log_entry(*idx)? {
//             assert_eq!(epoch, *expected_epoch, "Epoch mismatch for entry {}", idx);
//             assert_eq!(entry.msg_id, expected_entry.msg_id, "MsgId mismatch for entry {}", idx);
//             assert_eq!(entry.local_ts, expected_entry.local_ts, "Timestamp mismatch for entry {}", idx);
//             println!("    Successfully read entry {}", idx);
//         } else {
//             panic!("Entry {} not found", idx);
//         }
//     }
    
//     // Test 2: List all entries
//     println!("  Testing list_log_entries...");
//     let all_entries = persistence.list_log_entries()?;
//     assert_eq!(all_entries.len(), test_entries.len(), "Wrong number of entries listed");
    
//     // Verify they're sorted by index
//     for (i, persisted) in all_entries.iter().enumerate() {
//         assert_eq!(persisted.idx, i as u64, "Entries not properly sorted");
//         println!("    Listed entry {}: epoch={}, msg_id={}", persisted.idx, persisted.epoch, persisted.entry.msg_id);
//     }
    
//     // Test 3: Metadata operations
//     println!("  Testing metadata...");
//     let test_metadata = create_test_metadata();
    
//     // Should be empty initially
//     assert!(persistence.get_metadata()?.is_none(), "Metadata should be empty initially");
    
//     // Write metadata
//     persistence.put_metadata(&test_metadata)?;
//     println!("    Wrote metadata");
    
//     // Read metadata back
//     if let Some(read_metadata) = persistence.get_metadata()? {
//         assert_eq!(read_metadata.gid, test_metadata.gid, "GID mismatch");
//         assert_eq!(read_metadata.pid, test_metadata.pid, "PID mismatch");
//         assert_eq!(read_metadata.promised_epoch, test_metadata.promised_epoch, "Epoch mismatch");
//         assert_eq!(read_metadata.log_epochs, test_metadata.log_epochs, "Log epochs mismatch");
//         assert_eq!(read_metadata.safe_len, test_metadata.safe_len, "Safe length mismatch");
//         assert_eq!(read_metadata.clock, test_metadata.clock, "Clock mismatch");
//         println!("    Successfully read metadata");
//     } else {
//         panic!("Metadata not found after writing");
//     }
    
//     // Test 4: Truncation
//     println!("  Testing truncation...");
//     persistence.truncate_log(1)?; // Remove entries with idx >= 1
    
//     // Should only have entry 0 left
//     let remaining_entries = persistence.list_log_entries()?;
//     assert_eq!(remaining_entries.len(), 1, "Wrong number of entries after truncation");
//     assert_eq!(remaining_entries[0].idx, 0, "Wrong entry remaining after truncation");
//     println!("    Truncation successful, {} entries remaining", remaining_entries.len());
    
//     // Test 5: Flush
//     println!("  Testing flush...");
//     persistence.flush()?;
//     println!("    Flush successful");
    
//     println!("  {} backend tests completed successfully!\n", backend_name);
//     Ok(())
// }

// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let args = Args::parse();
    
//     // Create database directory
//     std::fs::create_dir_all(&args.database)?;
    
//     match args.backend.as_str() {
//         // #[cfg(feature = "lmdb-persistence")]
//         "lmdb" => {
//             let persistence = LMDBPersistence::new(&args.database)?;
//             test_persistence_backend(Box::new(persistence), "LMDB")?;
//         }
        
//         // #[cfg(feature = "rocksdb-persistence")]
//         // "rocksdb" => {
//         //     let persistence = persistence::RocksDBPersistence::new(&args.database)?;
//         //     test_persistence_backend(Box::new(persistence), "RocksDB")?;
//         // }
        
//         // #[cfg(feature = "sled-persistence")]
//         "sled" => {
//             let persistence = SledPersistence::new(&args.database)?;
//             test_persistence_backend(Box::new(persistence), "Sled")?;
//         }
        
//         "all" => {
//             // Test all available backends
//             // #[cfg(feature = "lmdb-persistence")]
//             {
//                 let lmdb_path = format!("{}/lmdb", args.database);
//                 std::fs::create_dir_all(&lmdb_path)?;
//                 let persistence = LMDBPersistence::new(&lmdb_path)?;
//                 test_persistence_backend(Box::new(persistence), "LMDB")?;
//             }
            
//             // #[cfg(feature = "rocksdb-persistence")]
//             // {
//             //     let rocksdb_path = format!("{}/rocksdb", args.database);
//             //     std::fs::create_dir_all(&rocksdb_path)?;
//             //     let persistence = persistence::RocksDBPersistence::new(&rocksdb_path)?;
//             //     test_persistence_backend(Box::new(persistence), "RocksDB")?;
//             // }
            
//             // #[cfg(feature = "sled-persistence")]
//             {
//                 let sled_path = format!("{}/sled", args.database);
//                 std::fs::create_dir_all(&sled_path)?;
//                 let persistence = SledPersistence::new(&sled_path)?;
//                 test_persistence_backend(Box::new(persistence), "Sled")?;
//             }
//         }
        
//         _ => {
//             eprintln!("Unknown backend: {}. Available: lmdb, rocksdb, sled, all", args.backend);
//             std::process::exit(1);
//         }
//     }
    
//     println!("All tests completed successfully!");
//     Ok(())
// }

fn main() {
    // This is a placeholder for the main function.
    // The actual test code will be run by the test framework.
    // println!("Run `cargo test` to execute the persistence tests.");
}