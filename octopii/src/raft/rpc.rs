use crate::rpc::RequestPayload;
use bytes::Bytes;
use protobuf::Message as PbMessage;
use raft::prelude::*;

/// Convert a Raft Message to RPC RequestPayload
pub fn raft_message_to_rpc(msg: &Message) -> Option<RequestPayload> {
    match msg.write_to_bytes() {
        Ok(bytes) => Some(RequestPayload::RaftMessage {
            message: Bytes::from(bytes),
        }),
        Err(e) => {
            tracing::error!(
                "Failed to serialize Raft message {:?}: {}",
                msg.get_msg_type(),
                e
            );
            None
        }
    }
}

/// Convert RPC RequestPayload to Raft Message
pub fn rpc_to_raft_message(_from: u64, to: u64, payload: &RequestPayload) -> Option<Message> {
    match payload {
        RequestPayload::RaftMessage { message } => match Message::parse_from_bytes(message) {
            Ok(mut msg) => {
                msg.to = to;
                Some(msg)
            }
            Err(e) => {
                tracing::error!("Failed to deserialize Raft message: {}", e);
                None
            }
        },
        _ => None,
    }
}

// ResponsePayload conversions remain for application-level RPCs.
