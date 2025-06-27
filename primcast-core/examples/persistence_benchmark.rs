use clap::Parser;
use primcast_core::{LogEntry, types::*};
use bytes::Bytes;
use std::time::Instant;
use primcast_core::persistence::*;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, short)]
    database: String,
    
    #[clap(long, short, default_value = "lmdb")]
    backend: String,
    
    #[clap(long, short, default_value = "1000")]
    entries: usize,
    
    #[clap(long, short, default_value = "100")]
    message_size: usize,
}

fn create_test_entry(idx: u64, msg_size: usize) -> LogEntry {
    let msg = "x".repeat(msg_size);
    LogEntry {
        local_ts: idx,
        msg_id: idx as u128,
        msg: Bytes::from(msg),
        dest: vec![Gid(0), Gid(1)].into_iter().collect(),
        final_ts: Some(idx + 100),
    }
}

fn benchmark_backend(
    mut persistence: Box<dyn PersistenceLayer>, 
    backend_name: &str, 
    num_entries: usize,
    msg_size: usize
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Benchmarking {} with {} entries of {} bytes each", backend_name, num_entries, msg_size);
    
    // Clear existing data
    persistence.clear_all()?;
    
    // Benchmark: Sequential writes
    let start = Instant::now();
    for i in 0..num_entries {
        let entry = create_test_entry(i as u64, msg_size);
        persistence.put_log_entry(Epoch::initial(), i as u64, &entry)?;
    }
    let write_duration = start.elapsed();
    
    // Flush to ensure all data is persisted
    persistence.flush()?;
    let flush_duration = start.elapsed();
    
    println!("  Write performance:");
    println!("    {} entries written in {:?}", num_entries, write_duration);
    println!("    {} entries/sec", (num_entries as f64 / write_duration.as_secs_f64()) as u64);
    println!("    Flush completed in {:?}", flush_duration);
    
    // Benchmark: Sequential reads
    let start = Instant::now();
    for i in 0..num_entries {
        let entry = persistence.get_log_entry(i as u64)?;
        assert!(entry.is_some(), "Entry {} not found", i);
    }
    let read_duration = start.elapsed();
    
    println!("  Read performance:");
    println!("    {} entries read in {:?}", num_entries, read_duration);
    println!("    {} entries/sec", (num_entries as f64 / read_duration.as_secs_f64()) as u64);
    
    // Benchmark: List all entries
    let start = Instant::now();
    let all_entries = persistence.list_log_entries()?;
    let list_duration = start.elapsed();
    
    println!("  List performance:");
    println!("    {} entries listed in {:?}", all_entries.len(), list_duration);
    assert_eq!(all_entries.len(), num_entries, "Wrong number of entries listed");
    
    // Benchmark: Random reads
    let start = Instant::now();
    for i in (0..num_entries).rev() {
        let entry = persistence.get_log_entry(i as u64)?;
        assert!(entry.is_some(), "Entry {} not found", i);
    }
    let random_read_duration = start.elapsed();
    
    println!("  Random read performance:");
    println!("    {} entries read in {:?}", num_entries, random_read_duration);
    println!("    {} entries/sec", (num_entries as f64 / random_read_duration.as_secs_f64()) as u64);
    
    println!();
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    
    std::fs::create_dir_all(&args.database)?;
    
    match args.backend.as_str() {
        // #[cfg(feature = "lmdb-persistence")]
        "lmdb" => {
            let persistence = LMDBPersistence::new(&args.database)?;
            benchmark_backend(Box::new(persistence), "LMDB", args.entries, args.message_size)?;
        }
        
        // #[cfg(feature = "rocksdb-persistence")]
        "rocksdb" => {
            let persistence = RocksDBPersistence::new(&args.database)?;
            benchmark_backend(Box::new(persistence), "RocksDB", args.entries, args.message_size)?;
        }
        
        // #[cfg(feature = "sled-persistence")]
        "sled" => {
            let persistence = SledPersistence::new(&args.database)?;
            benchmark_backend(Box::new(persistence), "Sled", args.entries, args.message_size)?;
        }
        
        "all" => {
            // #[cfg(feature = "lmdb-persistence")]
            {
                let lmdb_path = format!("{}/lmdb", args.database);
                std::fs::create_dir_all(&lmdb_path)?;
                let persistence = LMDBPersistence::new(&lmdb_path)?;
                benchmark_backend(Box::new(persistence), "LMDB", args.entries, args.message_size)?;
            }
            
            // #[cfg(feature = "rocksdb-persistence")]
            {
                let rocksdb_path = format!("{}/rocksdb", args.database);
                std::fs::create_dir_all(&rocksdb_path)?;
                let persistence = RocksDBPersistence::new(&rocksdb_path)?;
                benchmark_backend(Box::new(persistence), "RocksDB", args.entries, args.message_size)?;
            }
            
            // #[cfg(feature = "sled-persistence")]
            {
                let sled_path = format!("{}/sled", args.database);
                std::fs::create_dir_all(&sled_path)?;
                let persistence = SledPersistence::new(&sled_path)?;
                benchmark_backend(Box::new(persistence), "Sled", args.entries, args.message_size)?;
            }
        }
        
        _ => {
            eprintln!("Unknown backend: {}. Available: lmdb, rocksdb, sled, all", args.backend);
            std::process::exit(1);
        }
    }
    
    Ok(())
}