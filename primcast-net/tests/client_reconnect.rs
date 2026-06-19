/// Integration tests verifying client reconnect behaviour after primary failure.
///
/// Uses a mock TCP server to simulate a node that drops mid-flight, then
/// comes back and resumes serving requests normally.
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use primcast_core::types::{Clock, Gid, GidSet};
use primcast_net::codec::bincode_split;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Request {
    msg: Bytes,
    dest: GidSet,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Reply {
    ts: Clock,
    msg: Bytes,
    dest: GidSet,
}

fn local_gidset() -> GidSet {
    [Gid(0)].into_iter().collect()
}

/// Mock server: serves `first_replies` requests on the first connection, then drops.
/// On the second connection, serves indefinitely until the client disconnects.
async fn mock_server_drop_once(listener: TcpListener, first_replies: usize) {
    // Block scope so both codec halves (and the underlying socket) are dropped
    // together at the end, ensuring a TCP FIN is sent to the client.
    {
        let (sock, _) = listener.accept().await.unwrap();
        sock.set_nodelay(true).unwrap();
        let (mut rx, mut tx) = bincode_split::<Request, Reply, _>(sock);
        for _ in 0..first_replies {
            match rx.next().await {
                Some(Ok(req)) => {
                    tx.send(Reply { ts: Clock::default(), msg: req.msg, dest: req.dest })
                        .await
                        .ok();
                }
                _ => return,
            }
        }
        // rx and tx both drop here → socket drops → FIN sent to client.
    }

    let (sock2, _) = listener.accept().await.unwrap();
    sock2.set_nodelay(true).unwrap();
    let (mut rx2, mut tx2) = bincode_split::<Request, Reply, _>(sock2);
    while let Some(Ok(req)) = rx2.next().await {
        if tx2
            .send(Reply { ts: Clock::default(), msg: req.msg, dest: req.dest })
            .await
            .is_err()
        {
            break;
        }
    }
}

/// Reconnect loop that mirrors the client.rs pattern:
/// - Fresh semaphore per connection (avoids races with aborted send tasks).
/// - Restores in-flight permits before reconnecting.
/// - Returns once `goal` total replies have been received.
async fn run_reconnect_loop(
    addr: std::net::SocketAddr,
    total_outstanding: usize,
    goal: usize,
) -> usize {
    let mut total_replies = 0usize;

    loop {
        let sock = loop {
            match tokio::net::TcpStream::connect(addr).await {
                Ok(s) => break s,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        };
        sock.set_nodelay(true).unwrap();
        let (mut rx, mut tx) = bincode_split::<Reply, Request, _>(sock);

        // Fresh semaphore per connection, matching client.rs behaviour.
        let outstanding = Arc::new(Semaphore::new(total_outstanding));
        let outstanding_consumer = outstanding.clone();

        let send_handle = tokio::spawn(async move {
            while let Ok(permit) = outstanding_consumer.acquire().await {
                permit.forget();
                let req = Request { msg: Bytes::from_static(b"ping"), dest: local_gidset() };
                if tx.send(req).await.is_err() {
                    break;
                }
            }
        });

        loop {
            match rx.next().await {
                Some(Ok(_reply)) => {
                    outstanding.add_permits(1);
                    total_replies += 1;
                    if total_replies >= goal {
                        send_handle.abort();
                        return total_replies;
                    }
                }
                _ => break,
            }
        }

        // Restore permits for in-flight messages so the next connection starts at full capacity.
        let in_flight = total_outstanding.saturating_sub(outstanding.available_permits());
        if in_flight > 0 {
            outstanding.add_permits(in_flight);
        }
        send_handle.abort();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// After the primary drops (mid-flight), the client must reconnect and continue
/// receiving replies — no hang, no semaphore starvation.
#[tokio::test]
async fn reconnect_after_primary_failure_resumes_throughput() {
    const OUTSTANDING: usize = 3;
    const FIRST_REPLIES: usize = 2; // server replies to 2 then drops
    const GOAL: usize = 8;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(mock_server_drop_once(listener, FIRST_REPLIES));

    let total = tokio::time::timeout(
        Duration::from_secs(5),
        run_reconnect_loop(addr, OUTSTANDING, GOAL),
    )
    .await
    .expect("client must not hang after primary failure — reconnect loop must resume throughput");

    assert!(
        total >= GOAL,
        "expected at least {GOAL} replies across reconnects, got {total}"
    );
}
