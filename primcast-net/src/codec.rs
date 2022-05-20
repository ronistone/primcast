use bincode;
use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;
use tokio_util::codec;

const HDR_SIZE: usize = 4;


pub type BincodeReader<M, IO> = codec::FramedRead<ReadHalf<IO>, BincodeCodec<M>>;
pub type BincodeWriter<M, IO> = codec::FramedWrite<WriteHalf<IO>, BincodeCodec<M>>;

pub struct BincodeCodec<M> {
    phantom: PhantomData<M>,
    header: Option<usize>,
}

impl<M: serde::Serialize + serde::de::DeserializeOwned> BincodeCodec<M> {
    pub fn new() -> Self {
        Self {
            phantom: PhantomData,
            header: None,
        }
    }
}

impl<M: DeserializeOwned> codec::Decoder for BincodeCodec<M> {
    type Item = M;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if self.header.is_none() {
            // try read header
            if src.len() < 4 {
                return Ok(None);
            }
            self.header = Some(src.get_u32() as usize);
        }

        if let Some(len) = self.header {
            // try read msg
            if src.len() < len {
                // reserve enough space (just an optimization)
                src.reserve(len - src.len());
                return Ok(None);
            }

            self.header = None;
            let msg = bincode::deserialize(&src[..len]).map_err(|_| std::io::ErrorKind::InvalidData)?;
            src.advance(len);
            return Ok(Some(msg));
        }

        Ok(None)
    }
}

impl<M: Serialize> codec::Encoder<M> for BincodeCodec<M> {
    type Error = std::io::Error;
    fn encode(&mut self, m: M, buf: &mut BytesMut) -> Result<(), std::io::Error> {
        let size = bincode::serialized_size(&m).unwrap() as usize;
        buf.reserve(HDR_SIZE + size);
        buf.put_u32(size as u32);
        let mut w = buf.writer();
        Ok(bincode::serialize_into(&mut w, &m).expect("serialization error"))
    }
}

/// Split an io source (e.g., a socket) into framed read and write halfs.
pub fn bincode_split<RM, WM, IO>(io: IO) -> (BincodeReader<RM, IO>, BincodeWriter<WM, IO>)
where
    WM: Serialize + DeserializeOwned,
    RM: Serialize + DeserializeOwned,
    IO: AsyncRead + AsyncWrite,
{
    let (r, w) = tokio::io::split(io);
    let r = codec::FramedRead::new(r, BincodeCodec::new());
    let w = codec::FramedWrite::new(w, BincodeCodec::new());
    (r, w)
}

#[cfg(allow_unused)]
pub fn bincode_unsplit<RM, WM, IO>(rx: BincodeReader<RM, IO>, tx: BincodeWriter<WM, IO>) -> Option<IO>
where
    WM: Serialize + DeserializeOwned,
    RM: Serialize + DeserializeOwned,
    IO: AsyncRead + AsyncWrite,
{
    let rx = rx.into_inner();
    let tx = tx.into_inner();
    if rx.is_pair_of(&tx) {
        Some(rx.unsplit(tx))
    } else {
        None
    }
}
