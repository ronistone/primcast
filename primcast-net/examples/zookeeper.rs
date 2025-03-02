use std::sync::Arc;
use tokio_zookeeper::*;
use futures::prelude::*;
use tokio::sync::{Mutex, RwLock};

mod shared;

#[tokio::main]
async fn main() {
    let (zk, default_watcher) = ZooKeeper::connect(&"127.0.0.1:2181".parse().unwrap())
        .await
        .unwrap();

    println!("Connected to ZooKeeper {:?}", zk);


    let default_watcher = Arc::new(RwLock::new(default_watcher));
    tokio::spawn(async move {
        println!("Watching events");
        let mut lock = default_watcher.write().await;
        println!("Watching events: Get lock");
        loop {
            if let Some(event) = lock.next().await {
                println!("Received event {:?}", event);
            } else {
                println!("Fail to read event");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    });

    let election_node = "/ELECTION";
    let my_node_prefix = format!("{}/n_", election_node);

    // Ensure the election node exists
    if zk.watch().exists(election_node).await.unwrap().is_none() {
        zk.create(election_node, vec![], Acl::open_unsafe(), CreateMode::Persistent).await.unwrap();
    }

    // Create an ephemeral sequential node
    let mut my_node = zk.create(&my_node_prefix, vec![], Acl::open_unsafe(), CreateMode::EphemeralSequential).await.unwrap();

    'main: loop {
        let zk = zk.clone();
        // Get the list of children nodes
        let children = zk.get_children(election_node).await.unwrap();

        // Sort the children nodes
        let mut sorted_children = children.clone().unwrap();
        sorted_children.sort();

        // Determine if the current node is the leader
        if let Some((index, _)) = sorted_children.iter().enumerate().find(|(_, node)| format!("{}/{}", election_node, node) == my_node.clone().unwrap()) {
            if index == 0 {
                println!("I am the leader");
                tokio::time::sleep(tokio::time::Duration::from_secs(120)).await;
                break;
            } else {
                let predecessor = format!("{}/{}", election_node, sorted_children[index - 1]);
                let zk = Arc::new(Mutex::new(zk));

                // Watch the predecessor node
                loop {
                    if zk.lock().await.exists(&predecessor).await.unwrap().is_none() {
                        continue 'main;
                    }
                    println!("Watching {}", predecessor);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

}
