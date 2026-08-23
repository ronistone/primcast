use primcast_core::types::*;
use primcast_core::LogEntry;
use primcast_core::LogSkeletonEntry;
use primcast_core::RemoteEntry;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    /// first message sent from each side upon connection
    Handshake {
        gid: Gid,
        pid: Pid,
    },

    ProposalStart,
    Proposal {
        msg_id: MsgId,
        msg: Bytes,
        dest: GidSet,
    },

    NewEpoch {
        epoch: Epoch,
    },
    Promise {
        log_epoch: Epoch,
        log_len: u64,
        clock: Clock,
    },
    StartEpochCheck {
        epoch: Epoch,
        log_epochs: Vec<(Epoch, u64)>,
    },
    Following {
        log_epoch: Epoch,
        log_len: u64,
    },
    StartEpochAccept {
        epoch: Epoch,
        prev_entry: (Epoch, u64),
        clock: Clock,
    },

    LogAppend {
        idx: u64,
        entry_epoch: Epoch,
        entry: LogEntry,
    },

    LogAppendRequest {
        idx: u64,
        entry_epoch: Epoch,
    },

    AckRequest,
    RemoteAckRequest,
    Ack {
        log_epoch: Epoch,
        log_len: u64,
        clock: u64,
    },

    RemoteLogRequest {
        dest: Gid,
        log_epoch: Epoch,
        next_idx: u64,
    },
    RemoteLogEpoch {
        log_epoch: Epoch,
    },
    RemoteLogAppend(RemoteEntry),

    // Recovery protocol messages
    RecoveryRequest {
        gid: Gid,
        pid: Pid,
        promised_epoch: Epoch,
        log_epoch: Epoch,
        log_len: u64,
        log_epochs: Vec<(Epoch, u64)>,
    },

    RecoveryResponse {
        current_epoch: Epoch,
        leader_pid: Pid,
        total_log_len: u64,
        log_epochs: Vec<(Epoch, u64)>,
    },

    RecoveryRangeAssign {
        from_idx: u64,
        to_idx: u64,
    },

    RecoveryLogChunk {
        entries: Vec<(u64, Epoch, LogEntry)>,
        is_last: bool,
    },

    RecoveryComplete,

    // Leader instructs follower to fetch a log range from peers (collaborative catch-up)
    CollaborativeRecoveryStart {
        from_idx: u64,
        to_idx: u64,
    },

    // Follower requests a specific log range from a peer
    LogRangeRequest {
        gid: Gid,
        from_idx: u64,
        to_idx: u64,
    },

    // === Multi-group collaborative recovery ===
    // Own-group request for the authoritative metadata skeleton of a log range.
    // The response carries no payload bytes, only ordering metadata.
    RecoverySkeletonRequest {
        from_idx: u64,
        to_idx: u64,
    },
    RecoverySkeletonChunk {
        entries: Vec<LogSkeletonEntry>,
        is_last: bool,
    },

    // Cross-group request: ask a co-destination group for payloads of messages
    // (identified by msg_id) that it also received. `gid` is the requesting group.
    CrossGroupPayloadRequest {
        gid: Gid,
        msg_ids: Vec<MsgId>,
    },
    CrossGroupPayloadResponse {
        payloads: Vec<(MsgId, Bytes)>,
        // msg_ids this group could not serve; requester falls back to own peers
        missing: Vec<MsgId>,
    },
}

// #[derive(Debug, Serialize, Deserialize)]
// pub struct PromiseRequest(pub Epoch);

// #[derive(Debug, Serialize, Deserialize)]
// pub enum PromiseResponse {
//     Promised { epoch: Epoch, log_len: u64, clock: Clock },
//     Following { epoch: Epoch, log_len: u64, clock: Clock },
//     EpochTooOld { current: Epoch },
// }

// #[derive(Debug, Serialize, Deserialize)]
// pub enum StartEpochRequest {
//     Check {
//         promised: Epoch,
//         log_epochs: Vec<(Epoch, u64)>,
//     },
//     Append {
//         promised: Epoch,
//         idx: u64,
//         entry: LogEntry,
//         entry_epoch: Epoch,
//     },
//     Accept {
//         promised: Epoch,
//         last_entry: (Epoch, u64),
//         clock: Clock,
//     },
// }

// #[derive(Debug, Serialize, Deserialize)]
// pub enum StartEpochResponse {
//     Ok { log_len: u64, clock: Clock },
//     EpochTooOld { current: Epoch },
//     InvalidAppend,
// }
