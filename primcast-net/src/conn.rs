use std::net::SocketAddr;

use futures::sink::SinkExt;
use futures::stream::StreamExt;
use futures::Sink;
use futures::Stream;

use tokio::net::TcpStream;

use crate::codec::*;
use crate::messages::Message;
use primcast_core::types::*;

pub struct Conn {
    to: (Gid, Pid),
    rx: BincodeReader<Message, TcpStream>,
    tx: BincodeWriter<Message, TcpStream>,
}

impl Conn {
    pub async fn incoming(self_id: (Gid, Pid), sock: TcpStream) -> Result<Self, std::io::Error> {
        sock.set_nodelay(true).unwrap();
        let (mut rx, mut tx) = bincode_split::<Message, Message, _>(sock);
        tx.send(Message::Handshake {
            gid: self_id.0,
            pid: self_id.1,
        })
        .await?;
        if let Message::Handshake { gid, pid } = rx
            .next()
            .await
            .unwrap_or(Err(std::io::ErrorKind::ConnectionReset.into()))?
        {
            Ok(Conn { to: (gid, pid), rx, tx })
        } else {
            Err(std::io::ErrorKind::InvalidData.into())
        }
    }

    pub async fn request(
        from: (Gid, Pid),
        to: (Gid, Pid),
        addr: SocketAddr,
        req: Message,
    ) -> Result<Self, std::io::Error> {
        let sock = TcpStream::connect(addr).await?;
        sock.set_nodelay(true).unwrap();
        let (mut rx, mut tx) = bincode_split::<Message, Message, _>(sock);
        tx.feed(Message::Handshake {
            gid: from.0,
            pid: from.1,
        })
        .await?;
        tx.send(req).await?;
        if let Message::Handshake { gid, pid } = rx
            .next()
            .await
            .unwrap_or(Err(std::io::ErrorKind::ConnectionReset.into()))?
        {
            if (gid, pid) == to {
                return Ok(Conn { to, rx, tx });
            } else {
                eprintln!("handshake error: {:?} != {:?}", (gid, pid), to);
            }
        }
        Err(std::io::ErrorKind::InvalidData.into())
    }

    pub fn gid(&self) -> Gid {
        self.to.0
    }

    pub fn pid(&self) -> Pid {
        self.to.1
    }

    pub async fn recv(&mut self) -> Result<Message, std::io::Error> {
        self.rx
            .next()
            .await
            .unwrap_or(Err(std::io::ErrorKind::ConnectionReset.into()))
    }

    pub fn split(self) -> (ConnWrite, ConnRead) {
        let w = ConnWrite {
            to: self.to,
            tx: self.tx,
        };
        let r = ConnRead {
            to: self.to,
            rx: self.rx,
        };
        (w, r)
    }
}

impl Stream for Conn {
    type Item = Result<Message, std::io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_next_unpin(cx)
    }
}

impl Sink<Message> for Conn {
    type Error = std::io::Error;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.tx.poll_ready_unpin(cx)
    }

    fn start_send(mut self: std::pin::Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        self.tx.start_send_unpin(item)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.tx.poll_flush_unpin(cx)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.tx.poll_close_unpin(cx)
    }
}

pub struct ConnRead {
    to: (Gid, Pid),
    rx: BincodeReader<Message, TcpStream>,
}

impl ConnRead {
    pub fn gid(&self) -> Gid {
        self.to.0
    }

    pub fn pid(&self) -> Pid {
        self.to.1
    }

    pub async fn recv(&mut self) -> Result<Message, std::io::Error> {
        self.rx
            .next()
            .await
            .unwrap_or(Err(std::io::ErrorKind::ConnectionReset.into()))
    }

    pub fn unsplit(self, write_half: ConnWrite) -> Conn {
        assert!(self.rx.get_ref().is_pair_of(write_half.tx.get_ref()));
        Conn {
            to: self.to,
            rx: self.rx,
            tx: write_half.tx,
        }
    }
}

impl Stream for ConnRead {
    type Item = Result<Message, std::io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_next_unpin(cx)
    }
}

pub struct ConnWrite {
    to: (Gid, Pid),
    tx: BincodeWriter<Message, TcpStream>,
}

impl ConnWrite {
    pub fn gid(&self) -> Gid {
        self.to.0
    }

    pub fn pid(&self) -> Pid {
        self.to.1
    }

    pub fn unsplit(self, read_half: ConnRead) -> Conn {
        assert!(self.tx.get_ref().is_pair_of(read_half.rx.get_ref()));
        Conn {
            to: self.to,
            rx: read_half.rx,
            tx: self.tx,
        }
    }
}

impl Sink<Message> for ConnWrite {
    type Error = std::io::Error;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.tx.poll_ready_unpin(cx)
    }

    fn start_send(mut self: std::pin::Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        self.tx.start_send_unpin(item)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.tx.poll_flush_unpin(cx)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.tx.poll_close_unpin(cx)
    }
}
