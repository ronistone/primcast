use tokio_zookeeper::*;
use tokio::sync::mpsc;
use primcast_core::{config, timed_print};
use primcast_core::types::{Epoch, Gid, Pid};
use crate::Event;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use futures::StreamExt;

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

        let (zk, watcher) = ZooKeeper::connect(&group_config.zookeeper_url.parse()?)
            .await?;

        // Drain default watcher to avoid send errors on dropped receiver
        tokio::spawn(async move {
            let mut w = watcher;
            while w.next().await.is_some() {}
        });

        // Create election paths if they don't exist (no watch needed here)
        if zk.exists(self.base_path.as_str()).await?.is_none() {
            let _ = zk.create("/ELECTION", vec![], Acl::open_unsafe(), CreateMode::Persistent).await;
            let _ = zk.create(self.base_path.as_str(), vec![], Acl::open_unsafe(), CreateMode::Persistent).await;
        }

        let my_node = zk
            .create(
                self.node_path.as_str(),
                self.pid.to_bytes(),
                Acl::open_unsafe(),
                CreateMode::EphemeralSequential,
            )
            .await?
            .map_err(|e| format!("create node failed: {e:?}"))?;

        'main: loop {
            let my_pid = self.pid.clone();
            let children = zk.get_children(self.base_path.as_str()).await?
                .ok_or("election base path missing")?;
            let mut sorted_children = children;
            sorted_children.sort();

            if let Some((index, _)) = sorted_children.iter().enumerate()
                .find(|(_, node)| format!("{}/{}", self.base_path, node) == my_node)
            {
                if index == 0 {
                    timed_print!("I am the Leader: {}", self.pid.0);
                    self.publish(0, my_pid).await;
                    loop {
                        tokio::time::sleep(Duration::from_secs(120)).await;
                    }
                } else {
                    let leader = &sorted_children[0];
                    let data = zk
                        .get_data(format!("{}/{}", self.base_path, leader).as_str())
                        .await?
                        .ok_or("leader node missing")?;
                    let data_str = std::str::from_utf8(&data.0)?;
                    let leader_pid = Pid::from_str(data_str)?;
                    self.publish(0, leader_pid).await;

                    let predecessor = format!("{}/{}", self.base_path, sorted_children[index - 1]);
                    loop {
                        if zk.exists(&predecessor).await?.is_none() {
                            continue 'main;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }
    }
}
