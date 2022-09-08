use serde::Deserialize;
use serde::Serialize;

use bytes::Bytes;

use primcast_core::types::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub msg: Bytes,
    pub dest: GidSet,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Reply {
    pub ts: Clock,
    pub msg: Bytes,
    pub dest: GidSet,
}
