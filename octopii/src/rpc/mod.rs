mod handler;
mod message;

pub use handler::RpcHandler;
pub use message::{
    MessageId, OneWayMessage, RequestPayload, ResponsePayload, RpcMessage, RpcRequest, RpcResponse,
};

use crate::error::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Serialize an RPC message to bytes
pub fn serialize<T: Serialize>(msg: &T) -> Result<Bytes> {
    let data = bincode::serialize(msg)?;
    Ok(Bytes::from(data))
}

/// Deserialize an RPC message from bytes
pub fn deserialize<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<T> {
    let msg = bincode::deserialize(data)?;
    Ok(msg)
}
