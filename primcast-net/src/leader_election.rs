use std::sync::Arc;
use tokio_zookeeper::*;
use futures::prelude::*;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_zookeeper::error::Create;
use primcast_core::config;
use primcast_core::types::{Epoch, Gid, Pid};
use crate::{Event, Shared};

pub struct LeaderElection {
    gid: Gid,
    pid: Pid,
    cfg: config::Config,
    ev_tx: Vec<mpsc::UnboundedSender<Event>>,
    client: Option<ZooKeeper>,
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
            client: None,
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
        let group_config = self.cfg.groups.iter().find(|g| g.gid == self.gid).unwrap();
        let (zk, default_watcher) = ZooKeeper::connect(&group_config.zookeeper_url.parse().unwrap())
            .await
            .unwrap();
        println!("Connected to ZooKeeper {:?}", zk);


        if zk.watch().exists(self.base_path.as_str()).await.unwrap().is_none() {
            zk.create("/ELECTION", vec![], Acl::open_unsafe(), CreateMode::Persistent).await.unwrap();
            zk.create(self.base_path.as_str(), vec![], Acl::open_unsafe(), CreateMode::Persistent).await.unwrap();
        }

        let mut my_node: Result<String,Create>;
        // Create an ephemeral sequential node
        let pid = self.pid.clone();
        my_node = zk.create(self.node_path.as_str(), pid.to_bytes(), Acl::open_unsafe(), CreateMode::EphemeralSequential).await.unwrap();

        'main: loop {
            let my_pid = self.pid.clone();
            // Get the list of children nodes
            let children = zk.get_children(self.base_path.as_str()).await.unwrap();

            // Sort the children nodes
            let mut sorted_children = children.clone().unwrap();
            sorted_children.sort();

            // Determine if the current node is the leader
            if let Some((index, _)) = sorted_children.iter().enumerate().find(|(_, node)| format!("{}/{}", self.base_path, node) == my_node.clone().unwrap()) {
                if index == 0 {
                    println!("I am the leader");
                    let epoch_value: u32 = my_node.clone().unwrap().split("n_").last().unwrap().parse().unwrap();
                    self.publish(epoch_value, my_pid).await;
                    tokio::time::sleep(tokio::time::Duration::from_secs(120)).await;
                    break;
                } else {
                    let leader = sorted_children.get(0).unwrap();
                    let data = zk.get_data(format!("{}/{}", self.base_path, leader).as_str()).await.unwrap().unwrap();
                    let data_str = std::str::from_utf8(&data.0).unwrap();
                    let leader_pid = Pid::from_str(data_str);
                    let epoch_value: u32 = leader.split("n_").last().unwrap().parse().unwrap();

                    self.publish(epoch_value, leader_pid.unwrap()).await;

                    let predecessor = format!("{}/{}", self.base_path, sorted_children[index - 1]);

                    // Watch the predecessor node
                    loop {
                        if zk.exists(&predecessor).await.unwrap().is_none() {
                            continue 'main;
                        }
                        println!("Watching {}", predecessor);
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            }
        }

    }
}