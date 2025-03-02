use std::cmp::min;
use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender};
use futures::SinkExt;
use futures::stream::{StreamExt};
use serde::de::DeserializeOwned;
use tokio::sync::RwLock;
use tokio::time::sleep;
use crate::codec::bincode_split;

#[derive(Debug, Serialize, Deserialize)]
pub struct Message<M>
    where M: Send + 'static

{
    pub peer_id: u16,
    pub message: MessageBase<M>,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum MessageBase<M>
    where M: Send + 'static

{
    ConnectionFailed { peer: u16 },
    ConnectionBroken { peer: u16 },
    ConnectionEstablished { peer: u16 },
    Custom(M),
}

pub struct Peer<M>
    where M: Send + 'static
{
    pub id: Arc<RwLock<u16>>,
    pub channel: Sender<MessageBase<M>>
}

pub struct NodeCommunication<M>
    where M: Send + 'static

{
    peers_channels: Arc<RwLock<HashMap<u16, Peer<M>>>>,
    receiver_channel: Option<Sender<Message<M>>>,
}

impl<M> NodeCommunication<M>
    where M: Serialize + DeserializeOwned + Send + 'static

{
    pub fn new() -> Self {
        Self {
            peers_channels: Arc::new(RwLock::new(HashMap::new())),
            receiver_channel: None,
        }
    }

    pub async fn start(&mut self, port: u16, peers: Vec<u16>, receiver_channel: Sender<Message<M>>) {
        let address = format!("0.0.0.0:{}", port);
        println!("Listening on {}", address);
        let listener = TcpListener::bind(address).await.unwrap();
        self.receiver_channel = Option::from(receiver_channel.clone());
        tokio::spawn(accept_connections(listener, receiver_channel.clone()));

        for peer in peers.iter() {
            tokio::spawn(connect_to_peer(receiver_channel.clone(), self.peers_channels.clone(), peer.clone()));
        }
    }


    pub async fn send_message(&mut self, peer: u16, message: MessageBase<M>) {
        let read = self.peers_channels.read().await;
        if read.contains_key(&peer) {
            if let Some(peer_lock) = read.get(&peer) {
                let _ = peer_lock.channel.send(message).await;
            } else {
                println!("Peer {} not found!", peer);
            }
        }
        drop(read);
    }

    pub async fn get_peers(&self) -> Vec<u16> {
        let read = self.peers_channels.read().await;
        let mut peers: Vec<u16> = read.keys().cloned().collect();
        drop(read);
        peers.sort_by(|a, b| b.cmp(a));
        peers
    }
}

async fn accept_connections<M>(listener: TcpListener, application_channel: Sender<Message<M>>)
    where M: Serialize + DeserializeOwned + Send + 'static
{
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let app_channel = application_channel.clone();
        let peer_port = socket.peer_addr().unwrap().port();
        let (_, sender_channel_rx) = tokio::sync::mpsc::channel::<MessageBase<M>>(1); // Only to use the same handler function, this will not be used
        let peer_id = Arc::new(RwLock::new(peer_port));

        tokio::spawn(start_handler(socket, app_channel, sender_channel_rx, peer_id.clone()));
    }
}

async fn connect_to_peer<M>(receiver_channel: Sender<Message<M>>, peers_channels: Arc<RwLock<HashMap<u16, Peer<M>>>>, peer: u16) -> bool
    where M: Serialize + DeserializeOwned + Send + 'static
{
    let mut reconnection_wait_time = 100;
    const MAX_RECONNECTION_WAIT_TIME: u64 = 4000;
    loop {
        let p = peer.clone();

        let application_channel = receiver_channel.clone();
        let (sender_channel_tx, sender_channel_rx) = tokio::sync::mpsc::channel::<MessageBase<M>>(10);
        let peer_id_lock = Arc::new(RwLock::new(p));

        let mut write = peers_channels.write().await;
        write.insert(p, Peer {
            id: peer_id_lock.clone(),
            channel: sender_channel_tx.clone()
        });
        drop(write);
        let app_channel = application_channel.clone();

        let peer_address = format!("127.0.0.1:{}", p);
        println!("Connecting to {}", peer_address);
        match TcpStream::connect(peer_address.clone()).await {
            Ok(socket) => {
                reconnection_wait_time = 500;
                start_handler(socket, app_channel, sender_channel_rx, peer_id_lock.clone()).await;
                continue;
            }
            Err(err) => {
                println!("error connecting to server at  --- {peer_address} --- : ERROR({err})");
                let mut write = peers_channels.write().await;
                write.remove(&p.clone());
                drop(write);
            }
        }
        sleep(tokio::time::Duration::from_millis(reconnection_wait_time)).await;
        if reconnection_wait_time < MAX_RECONNECTION_WAIT_TIME {
            reconnection_wait_time = min(reconnection_wait_time * 2, MAX_RECONNECTION_WAIT_TIME);
        }
    }
}


async fn start_handler<M>(
    socket: TcpStream,
    channel: Sender<Message<M>>,
    mut sender_channel_rx: Receiver<MessageBase<M>>,
    peer: Arc<RwLock<u16>>
)
    where M: Serialize + DeserializeOwned + Send + 'static
{
    let _ = socket.set_nodelay(true);
    let (mut rx, mut tx) = bincode_split(socket);
    {
        let peer_locked = peer.read().await;
        let _ = channel.send(Message {
            peer_id: *peer_locked,
            message: MessageBase::ConnectionEstablished { peer: *peer_locked }
        }).await;
        drop(peer_locked);
    }

    loop {
        tokio::select! {
            message = rx.next() => {
                match message {
                    Some(Ok(message)) => {
                        {
                            let peer_locked = peer.read().await;
                            let  _ = channel.send(Message{
                                peer_id: *peer_locked,
                                message: MessageBase::Custom(message)
                            }).await;
                            drop(peer_locked);
                        }
                    }
                    _ => {
                        break;
                    }
                }
            }
            Some(message) = sender_channel_rx.recv() => {
                match message {
                    MessageBase::Custom(message) => {
                        {
                            let  _ = tx.send(message).await;
                        }
                    },
                    _ => {}
                }
            }
            else => {
                println!("Channels closed!");
                break;
            }
        }
    }
}