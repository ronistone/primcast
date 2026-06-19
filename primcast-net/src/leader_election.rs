use zookeeper_async::{Acl, CreateMode, WatchedEvent, ZooKeeper};
use tokio::sync::mpsc;
use primcast_core::{config, timed_print};
use primcast_core::types::{Epoch, Gid, Pid};
use crate::Event;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct LeaderElection {
    gid: Gid,
    pid: Pid,
    cfg: config::Config,
    ev_tx: Vec<mpsc::UnboundedSender<Event>>,
    base_path: String,
    node_path: String,
}

impl LeaderElection {
    pub fn new(gid: Gid, pid: Pid, cfg: config::Config) -> Self {
        let election_node = format!("/ELECTION/g_{}", gid.0);
        let node_path = format!("{}/n_", election_node);
        let ev_tx = Vec::new();
        Self {
            gid,
            pid,
            cfg,
            ev_tx,
            base_path: election_node,
            node_path,
        }
    }

    pub fn subscribe(&mut self, tx: mpsc::UnboundedSender<Event>) {
        self.ev_tx.push(tx);
    }

    pub async fn publish(&self, epoch: u32, pid: Pid) {
        for tx in &self.ev_tx {
            tx.send(Event::InitiateEpoch(Epoch(epoch, pid.clone()))).unwrap_or(());
        }
    }

    pub async fn run(self) {
        loop {
            if let Err(e) = self.run_once().await {
                eprintln!("[LeaderElection] gid={} pid={} error: {}. Reconnecting in 1s...",
                          self.gid.0, self.pid.0, e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    async fn run_once(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let group_config = self.cfg.groups.iter()
            .find(|g| g.gid == self.gid)
            .ok_or("group config not found")?;

        let zk = tokio::time::timeout(
            Duration::from_secs(5),
            ZooKeeper::connect(
                &group_config.zookeeper_url,
                Duration::from_secs(5),
                |_: WatchedEvent| {},
            )
        )
        .await
        .map_err(|_| "ZK connect timeout")?
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        // Create election paths if they don't exist
        if zk.exists(self.base_path.as_str(), false).await?.is_none() {
            let _ = zk.create("/ELECTION", vec![], Acl::open_unsafe().to_vec(), CreateMode::Persistent).await;
            let _ = zk.create(self.base_path.as_str(), vec![], Acl::open_unsafe().to_vec(), CreateMode::Persistent).await;
        }

        let my_node = zk
            .create(
                self.node_path.as_str(),
                self.pid.to_bytes(),
                Acl::open_unsafe().to_vec(),
                CreateMode::EphemeralSequential,
            )
            .await?;

        'main: loop {
            let my_pid = self.pid.clone();
            let children = zk.get_children(self.base_path.as_str(), false).await?;
            let mut sorted_children = children;
            sorted_children.sort();

            if let Some((index, _)) = sorted_children.iter().enumerate()
                .find(|(_, node)| format!("{}/{}", self.base_path, node) == my_node)
            {
                if index == 0 {
                    timed_print!("I am the Leader: {}", self.pid.0);
                    self.publish(0, my_pid).await;
                    loop {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        match zk.exists(my_node.as_str(), false).await {
                            Ok(None) => return Err("leader node gone - session expired".into()),
                            Err(e)   => return Err(Box::new(e)),
                            Ok(Some(_)) => {}
                        }
                    }
                } else {
                    let leader = &sorted_children[0];
                    let data = zk
                        .get_data(format!("{}/{}", self.base_path, leader).as_str(), false)
                        .await?;
                    let data_str = std::str::from_utf8(&data.0)?;
                    let leader_pid = Pid::from_str(data_str)?;
                    self.publish(0, leader_pid).await;

                    let predecessor = format!("{}/{}", self.base_path, sorted_children[index - 1]);
                    loop {
                        if zk.exists(&predecessor, false).await?.is_none() {
                            continue 'main;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LeaderElection;
    use crate::Event;
    use primcast_core::config::Config;
    use primcast_core::types::{Epoch, Gid, Pid};
    use tokio::sync::mpsc;
    use std::time::Duration;

    fn test_config_with_zk(zk_url: &str) -> Config {
        let mut cfg = Config::new_for_test();
        for g in &mut cfg.groups {
            g.zookeeper_url = zk_url.to_string();
        }
        cfg
    }

    fn zk_url() -> String {
        std::env::var("ZK_URL").unwrap_or_else(|_| "127.0.0.1:2181".to_string())
    }

    // ── unit tests (no ZK) ────────────────────────────────────────────────────

    #[tokio::test]
    async fn subscribe_and_publish_delivers_event() {
        let mut le = LeaderElection::new(Gid(0), Pid(1), Config::new_for_test());
        let (tx, mut rx) = mpsc::unbounded_channel();
        le.subscribe(tx);

        le.publish(0, Pid(1)).await;

        let event = rx.try_recv().expect("event should be immediately available");
        assert!(matches!(event, Event::InitiateEpoch(Epoch(0, Pid(1)))));
    }

    #[tokio::test]
    async fn publish_to_multiple_subscribers_delivers_to_all() {
        let mut le = LeaderElection::new(Gid(0), Pid(0), Config::new_for_test());
        let (tx0, mut rx0) = mpsc::unbounded_channel();
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        le.subscribe(tx0);
        le.subscribe(tx1);

        le.publish(0, Pid(0)).await;

        assert!(matches!(rx0.try_recv().unwrap(), Event::InitiateEpoch(Epoch(0, Pid(0)))));
        assert!(matches!(rx1.try_recv().unwrap(), Event::InitiateEpoch(Epoch(0, Pid(0)))));
    }

    #[tokio::test]
    async fn publish_to_dropped_receiver_does_not_panic() {
        let mut le = LeaderElection::new(Gid(0), Pid(0), Config::new_for_test());
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        le.subscribe(tx);
        drop(rx);
        le.publish(0, Pid(0)).await; // must not panic
    }

    #[tokio::test]
    async fn run_once_returns_error_on_unreachable_zk() {
        // Port 1 is reserved and unreachable → error (not panic).
        let le = LeaderElection::new(Gid(0), Pid(0), test_config_with_zk("127.0.0.1:1"));
        let result = le.run_once().await;
        assert!(result.is_err(), "run_once should return Err on unreachable ZK, not panic");
    }

    #[tokio::test]
    async fn connect_timeout_on_unresponsive_server() {
        // TcpListener accepts the TCP connection but never speaks ZK protocol
        // → zookeeper-async handshake hangs → our 5s outer timeout must fire
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _listener = listener; // keep alive, never accept

        let le = LeaderElection::new(Gid(0), Pid(0), test_config_with_zk(&addr.to_string()));
        let start = std::time::Instant::now();
        let result = le.run_once().await;
        let elapsed = start.elapsed();

        assert!(result.is_err(), "run_once must return Err, not hang");
        assert!(elapsed < Duration::from_secs(10), "timed out too late: {elapsed:?}");
    }

    #[tokio::test]
    async fn run_retries_without_panic_on_bad_url() {
        let le = LeaderElection::new(Gid(0), Pid(0), test_config_with_zk("127.0.0.1:1"));
        // run() loops forever; if it panics the test process crashes instead of timing out.
        let result = tokio::time::timeout(Duration::from_secs(3), le.run()).await;
        assert!(result.is_err(), "run() must never return — timeout should fire");
    }

    // ── integration tests (require real ZK) ──────────────────────────────────
    // Run with: cargo test -p primcast-net -- --ignored

    #[tokio::test]
    #[ignore = "requires ZooKeeper at ZK_URL (default 127.0.0.1:2181)"]
    async fn single_node_becomes_leader() {
        let mut le = LeaderElection::new(Gid(10), Pid(0), test_config_with_zk(&zk_url()));
        let (tx, mut rx) = mpsc::unbounded_channel();
        le.subscribe(tx);
        tokio::spawn(le.run());

        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for leader election")
            .expect("event channel closed");

        assert!(
            matches!(event, Event::InitiateEpoch(Epoch(0, Pid(0)))),
            "single node should elect itself leader"
        );
    }

    #[tokio::test]
    #[ignore = "requires ZooKeeper at ZK_URL (default 127.0.0.1:2181)"]
    async fn leader_run_once_returns_when_node_disappears() {
        // Verifies run_once() returns an error quickly (not after 120s) when the
        // leader's ZK node is externally deleted, simulating session expiry.
        let cfg = test_config_with_zk(&zk_url());
        let gid = Gid(20);
        let le = LeaderElection::new(gid, Pid(0), cfg.clone());

        let handle = tokio::spawn(async move { le.run_once().await });

        // Give run_once time to create the node and enter the leader loop.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Second client deletes all election nodes for this gid, simulating
        // the leader's ephemeral node being cleaned up by ZK on session expiry.
        let group_cfg = cfg.groups.iter().find(|g| g.gid == gid).unwrap();
        let zk2 = zookeeper_async::ZooKeeper::connect(
            &group_cfg.zookeeper_url,
            Duration::from_secs(5),
            |_: zookeeper_async::WatchedEvent| {},
        )
        .await
        .expect("second ZK connect failed");

        let base = format!("/ELECTION/g_{}", gid.0);
        if let Ok(children) = zk2.get_children(base.as_str(), false).await {
            for child in children {
                let _ = zk2.delete(&format!("{base}/{child}"), None).await;
            }
        }

        // run_once must return within 5s — not hang for 120s.
        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("run_once did not return after node was deleted — leader loop is stuck")
            .expect("task should not panic");

        assert!(result.is_err(), "run_once should return Err when leader node is gone");
    }

    #[tokio::test]
    #[ignore = "requires ZooKeeper at ZK_URL (default 127.0.0.1:2181)"]
    async fn two_nodes_elect_consistent_leader() {
        let cfg = test_config_with_zk(&zk_url());
        let mut le0 = LeaderElection::new(Gid(11), Pid(0), cfg.clone());
        let mut le1 = LeaderElection::new(Gid(11), Pid(1), cfg);
        let (tx0, mut rx0) = mpsc::unbounded_channel();
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        le0.subscribe(tx0);
        le1.subscribe(tx1);
        tokio::spawn(le0.run());
        tokio::spawn(le1.run());

        let ev0 = tokio::time::timeout(Duration::from_secs(5), rx0.recv())
            .await.expect("timeout (node 0)").expect("channel closed (node 0)");
        let ev1 = tokio::time::timeout(Duration::from_secs(5), rx1.recv())
            .await.expect("timeout (node 1)").expect("channel closed (node 1)");

        let leader0 = match ev0 { Event::InitiateEpoch(Epoch(_, pid)) => pid, _ => panic!("unexpected event from node 0") };
        let leader1 = match ev1 { Event::InitiateEpoch(Epoch(_, pid)) => pid, _ => panic!("unexpected event from node 1") };
        assert_eq!(leader0, leader1, "both nodes must agree on the same leader");
    }
}
