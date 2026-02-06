use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, SinkExt};

use primcast_core::config::{Config, PeerConfig};
use primcast_core::types::*;
use primcast_core::LogEntry;

use crate::conn::Conn;
use crate::messages::Message;
use crate::{Error, Shared};

const RECOVERY_BATCH_SIZE: usize = 50;
const RECOVERY_MAX_RETRIES: usize = 5;
const RECOVERY_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Run collaborative recovery with retry logic.
/// Returns Ok(()) when recovery is complete.
pub async fn run_recovery_with_retry(
    s: Arc<RwLock<Shared>>,
) -> Result<(), Error> {
    for attempt in 0..RECOVERY_MAX_RETRIES {
        match run_recovery(s.clone()).await {
            Ok(()) => {
                eprintln!("Recovery completed successfully");
                return Ok(());
            }
            Err(e) => {
                eprintln!("recovery attempt {} failed: {:?}", attempt + 1, e);
                if attempt < RECOVERY_MAX_RETRIES - 1 {
                    tokio::time::sleep(RECOVERY_RETRY_DELAY).await;
                }
            }
        }
    }
    Err(Error::Io(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "recovery failed after max retries",
    )))
}

/// Run collaborative recovery. Returns Ok(()) when recovery is complete.
/// The recovering node contacts all peers, assigns log ranges, and merges
/// the received entries.
pub async fn run_recovery(s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    let cfg: Config;
    let self_gid: Gid;
    let self_pid: Pid;
    let local_log_len: u64;
    let local_log_epoch: Epoch;
    let local_promised_epoch: Epoch;
    let local_log_epochs: Vec<(Epoch, u64)>;
    {
        let shared = s.read().await;
        cfg = shared.core.config.clone();
        self_gid = shared.core.gid;
        self_pid = shared.core.pid;
        let (promised, log_epoch, log_len, _, log_epochs) = shared.core.recovery_status();
        local_log_len = log_len;
        local_log_epoch = log_epoch;
        local_promised_epoch = promised;
        local_log_epochs = log_epochs;
    }

    let group = cfg.group(self_gid).ok_or_else(|| {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "group not found",
        ))
    })?;

    let peers: Vec<&PeerConfig> = group
        .peers
        .iter()
        .filter(|p| p.pid != self_pid)
        .collect();

    if peers.is_empty() {
        // Single-node group, nothing to recover from
        eprintln!("[Recovery] Single-node group, skipping recovery");
        let mut shared = s.write().await;
        let clock = shared.core.clock();
        shared.core.finalize_recovery(local_promised_epoch, clock)?;
        shared.update_tx.send(()).ok();
        return Ok(());
    }

    // Check if gap is small enough to skip collaborative recovery
    let gap = {
        let shared = s.read().await;
        let (_, log_len) = shared.core.log_status();
        // For simplicity, we always run recovery. In production, add threshold: if gap < 1000 { return }
        0
    };

    // --- Phase 1: Contact all peers with RecoveryRequest ---
    eprintln!("[Recovery] Phase 1 - Contacting {} peers", peers.len());
    let mut response_futs = FuturesUnordered::new();
    for peer in &peers {
        let peer = (*peer).clone();
        let self_gid = self_gid;
        let self_pid = self_pid;
        let local_promised_epoch = local_promised_epoch;
        let local_log_epoch = local_log_epoch;
        let local_log_len = local_log_len;
        let local_log_epochs = local_log_epochs.clone();

        response_futs.push(async move {
            let req = Message::RecoveryRequest {
                gid: self_gid,
                pid: self_pid,
                promised_epoch: local_promised_epoch,
                log_epoch: local_log_epoch,
                log_len: local_log_len,
                log_epochs: local_log_epochs,
            };
            let mut conn = Conn::request(
                (self_gid, self_pid),
                (self_gid, peer.pid),
                peer.addr(),
                req,
            )
            .await?;

            // Read RecoveryResponse
            match conn.recv().await? {
                Message::RecoveryResponse {
                    current_epoch,
                    leader_pid,
                    total_log_len,
                    log_epochs,
                } => Ok((conn, peer.pid, current_epoch, leader_pid, total_log_len, log_epochs)),
                m => Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected message during recovery: {:?}", m),
                ))),
            }
        });
    }

    // Collect responses
    let mut connections = Vec::new();
    let mut max_log_len: u64 = local_log_len;
    let mut cluster_epoch = local_promised_epoch;

    while let Some(result) = response_futs.next().await {
        match result {
            Ok((conn, pid, current_epoch, _leader_pid, total_log_len, _log_epochs)) => {
                eprintln!("[Recovery] Peer {:?} has log_len {}", pid, total_log_len);
                max_log_len = std::cmp::max(max_log_len, total_log_len);
                if current_epoch > cluster_epoch {
                    cluster_epoch = current_epoch;
                }
                connections.push((conn, pid, total_log_len));
            }
            Err(e) => {
                // Peer unavailable, continue with others
                eprintln!("recovery: peer unavailable: {:?}", e);
            }
        }
    }

    if connections.is_empty() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "no peers available for recovery",
        )));
    }

    let gap = max_log_len - local_log_len;
    if gap == 0 {
        // Already up to date, just finalize
        eprintln!("[Recovery] Already up to date");
        let mut shared = s.write().await;
        let clock = shared.core.clock();
        shared.core.finalize_recovery(cluster_epoch, clock)?;
        shared.update_tx.send(()).ok();
        return Ok(());
    }

    eprintln!("[Recovery] Phase 2 - Assigning ranges, gap = {}", gap);

    // --- Phase 2: Assign ranges and request log chunks ---
    // Sort connections by pid for deterministic assignment
    connections.sort_by_key(|(_, pid, _)| *pid);

    let num_peers = connections.len();
    let chunk_size = (gap + num_peers as u64 - 1) / num_peers as u64; // ceil division

    let mut chunk_futs = FuturesUnordered::new();

    for (i, (mut conn, pid, peer_log_len)) in connections.into_iter().enumerate() {
        let from_idx = local_log_len + (i as u64) * chunk_size;
        let to_idx = std::cmp::min(
            local_log_len + ((i as u64) + 1) * chunk_size,
            max_log_len,
        );

        // Don't assign range beyond what this peer actually has
        let effective_to = std::cmp::min(to_idx, peer_log_len);
        if from_idx >= effective_to {
            eprintln!("[Recovery] Peer {:?} has no entries in range [{}, {})", pid, from_idx, effective_to);
            continue;
        }

        eprintln!("[Recovery] Assigning peer {:?} range [{}, {})", pid, from_idx, effective_to);

        chunk_futs.push(async move {
            // Send range assignment
            conn.send(Message::RecoveryRangeAssign {
                from_idx,
                to_idx: effective_to,
            })
            .await?;

            // Receive chunks
            let mut entries: Vec<(u64, Epoch, LogEntry)> = Vec::new();
            loop {
                match conn.recv().await? {
                    Message::RecoveryLogChunk {
                        entries: chunk_entries,
                        is_last,
                    } => {
                        entries.extend(chunk_entries);
                        if is_last {
                            break;
                        }
                    }
                    m => {
                        return Err(Error::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("unexpected message during recovery chunk: {:?}", m),
                        )))
                    }
                }
            }
            Ok::<_, Error>((pid, from_idx, entries))
        });
    }

    // --- Phase 3: Collect all chunks and merge ---
    eprintln!("[Recovery] Phase 3 - Collecting chunks");
    let mut all_entries: Vec<(u64, Epoch, LogEntry)> = Vec::new();

    while let Some(result) = chunk_futs.next().await {
        match result {
            Ok((_pid, _from, entries)) => {
                all_entries.extend(entries);
            }
            Err(e) => {
                eprintln!("recovery: error receiving chunk: {:?}", e);
                // If a peer fails, we have a gap. Fail recovery and retry.
                return Err(e);
            }
        }
    }

    // Sort by idx to ensure correct order
    all_entries.sort_by_key(|(idx, _, _)| *idx);

    // Validate contiguity
    for (i, (idx, _, _)) in all_entries.iter().enumerate() {
        let expected = local_log_len + i as u64;
        if *idx != expected {
            return Err(Error::Core(primcast_core::Error::InvalidIndex {
                len: expected,
            }));
        }
    }

    eprintln!("[Recovery] Phase 4 - Applying {} entries", all_entries.len());

    // --- Phase 4: Apply entries to the local log ---
    {
        let mut shared = s.write().await;
        shared.core.recovery_append_batch(all_entries)?;
    }

    // --- Phase 5: Finalize recovery ---
    eprintln!("[Recovery] Phase 5 - Finalizing recovery");
    {
        let mut shared = s.write().await;
        let clock = shared.core.clock();
        shared.core.finalize_recovery(cluster_epoch, clock)?;

        // Notify watchers
        shared.update_tx.send(()).ok();
        let (log_epoch, log_len) = shared.core.log_status();
        let clock = shared.core.clock();
        shared.ack_tx[&self_gid].send((log_epoch, log_len, clock)).ok();
    }

    Ok(())
}
