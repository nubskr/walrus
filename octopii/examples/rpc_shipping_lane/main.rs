use bytes::Bytes;
use futures::FutureExt;
use octopii::rpc::{self, RequestPayload, ResponsePayload, RpcHandler};
use octopii::transport::QuicTransport;
use octopii::ShippingLane;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    let payload = run_negotiated_transfer().await?;
    println!("Negotiated transfer payload: {:?}", payload);
    Ok(())
}

pub async fn run_negotiated_transfer() -> Result<Bytes, Box<dyn Error>> {
    let rpc_server_transport = Arc::new(QuicTransport::new("127.0.0.1:0".parse()?).await?);
    let rpc_server_addr = rpc_server_transport.local_addr()?;
    let data_server_transport = Arc::new(QuicTransport::new("127.0.0.1:0".parse()?).await?);
    let shipping_lane = Arc::new(ShippingLane::new(Arc::clone(&data_server_transport)));
    let rpc_server = Arc::new(RpcHandler::new(Arc::clone(&rpc_server_transport)));

    let snapshot = Bytes::from_static(b"SNAPSHOT_PAYLOAD");

    rpc_server
        .set_request_handler({
            let shipping_lane = Arc::clone(&shipping_lane);
            let snapshot = snapshot.clone();
            move |req| match req.payload {
                RequestPayload::Custom { operation, data } if operation == "SNAPSHOT_REQUEST" => {
                    let addr_str = match String::from_utf8(data.to_vec()) {
                        Ok(s) => s,
                        Err(e) => {
                            return async move {
                                ResponsePayload::Error {
                                    message: format!("invalid utf-8 address: {}", e),
                                }
                            }
                            .boxed()
                        }
                    };

                    let addr: SocketAddr = match addr_str.parse() {
                        Ok(a) => a,
                        Err(e) => {
                            return async move {
                                ResponsePayload::Error {
                                    message: format!("invalid address: {}", e),
                                }
                            }
                            .boxed()
                        }
                    };

                    let bytes = snapshot.clone();
                    let lane = Arc::clone(&shipping_lane);
                    let chunk = bytes.clone();
                    tokio::spawn(async move {
                        if let Err(err) = lane.send_memory(addr, chunk).await {
                            tracing::warn!("shipping lane send failed: {}", err);
                        }
                    });

                    async move {
                        ResponsePayload::CustomResponse {
                            success: true,
                            data: Bytes::from(format!("READY {}", bytes.len())),
                        }
                    }
                    .boxed()
                }
                _ => async {
                    ResponsePayload::Error {
                        message: "unsupported request".into(),
                    }
                }
                .boxed(),
            }
        })
        .await;

    let rpc_server_task = tokio::spawn(run_rpc_server(
        Arc::clone(&rpc_server_transport),
        Arc::clone(&rpc_server),
    ));

    let rpc_client_transport = Arc::new(QuicTransport::new("127.0.0.1:0".parse()?).await?);
    let rpc_client = Arc::new(RpcHandler::new(Arc::clone(&rpc_client_transport)));

    let data_client_transport = Arc::new(QuicTransport::new("127.0.0.1:0".parse()?).await?);
    let data_client_addr = data_client_transport.local_addr()?;
    let data_client_accept = Arc::clone(&data_client_transport);

    let receive_task = tokio::spawn(async move {
        let (_addr, peer) = data_client_accept.accept().await?;
        let bytes = peer
            .recv_chunk_verified()
            .await?
            .ok_or_else(|| octopii::error::OctopiiError::Transport("no data received".into()))?;
        Ok::<_, octopii::error::OctopiiError>(bytes)
    });

    let payload = RequestPayload::Custom {
        operation: "SNAPSHOT_REQUEST".into(),
        data: Bytes::from(data_client_addr.to_string()),
    };

    let response = rpc_client
        .request(rpc_server_addr, payload, Duration::from_secs(2))
        .await?;

    let data = match response.payload {
        ResponsePayload::CustomResponse { success: true, .. } => receive_task.await??,
        ResponsePayload::Error { message } => return Err(message.into()),
        other => return Err(format!("unexpected response: {:?}", other).into()),
    };

    rpc_server_transport.close();
    rpc_client_transport.close();
    data_server_transport.close();
    data_client_transport.close();

    rpc_server_task.abort();

    Ok(data)
}

async fn run_rpc_server(transport: Arc<QuicTransport>, rpc: Arc<RpcHandler>) {
    while let Ok((addr, peer)) = transport.accept().await {
        let rpc = Arc::clone(&rpc);
        tokio::spawn(async move {
            loop {
                match peer.recv().await {
                    Ok(Some(bytes)) => match rpc::deserialize(&bytes) {
                        Ok(message) => {
                            rpc.notify_message(addr, message, Some(Arc::clone(&peer)))
                                .await;
                        }
                        Err(err) => tracing::warn!("rpc deserialize error: {}", err),
                    },
                    Ok(None) => break,
                    Err(err) => {
                        tracing::warn!("rpc recv error: {}", err);
                        break;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::run_negotiated_transfer;
    use bytes::Bytes;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rpc_negotiated_transfer_succeeds() {
        let received = run_negotiated_transfer()
            .await
            .expect("negotiated transfer should succeed");
        assert_eq!(received, Bytes::from_static(b"SNAPSHOT_PAYLOAD"));
    }
}
