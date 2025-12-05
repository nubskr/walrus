use bytes::Bytes;
use futures::FutureExt;
use octopii::rpc::{self, RequestPayload, ResponsePayload, RpcHandler};
use octopii::transport::QuicTransport;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error>> {
    let response = run_rpc_example().await?;
    println!("RPC example response: {}", response);
    Ok(())
}

pub async fn run_rpc_example() -> Result<String, Box<dyn Error>> {
    let server_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server_transport = Arc::new(QuicTransport::new(server_addr).await?);
    let server_bind = server_transport.local_addr()?;
    let server_rpc = Arc::new(RpcHandler::new(Arc::clone(&server_transport)));

    server_rpc
        .set_request_handler(|req| match req.payload {
            RequestPayload::Custom { data, .. } => {
                let upper = String::from_utf8_lossy(&data).to_uppercase();
                async {
                    ResponsePayload::CustomResponse {
                        success: true,
                        data: Bytes::from(upper),
                    }
                }
                .boxed()
            }
            _ => async {
                ResponsePayload::Error {
                    message: "unsupported payload".into(),
                }
            }
            .boxed(),
        })
        .await;

    let server_loop = tokio::spawn(run_server(
        Arc::clone(&server_transport),
        Arc::clone(&server_rpc),
    ));

    let client_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let client_transport = Arc::new(QuicTransport::new(client_addr).await?);
    let client_rpc = Arc::new(RpcHandler::new(Arc::clone(&client_transport)));

    let response = client_rpc
        .request(
            server_bind,
            RequestPayload::Custom {
                operation: "upper".into(),
                data: Bytes::from("test"),
            },
            Duration::from_secs(1),
        )
        .await?;

    server_transport.close();
    client_transport.close();
    server_loop.abort();

    match response.payload {
        ResponsePayload::CustomResponse { data, .. } => {
            Ok(String::from_utf8(data.to_vec()).expect("response is utf-8"))
        }
        ResponsePayload::Error { message } => Err(message.into()),
        other => Err(format!("unexpected payload: {:?}", other).into()),
    }
}

async fn run_server(transport: Arc<QuicTransport>, rpc: Arc<RpcHandler>) {
    while let Ok((addr, peer)) = transport.accept().await {
        let rpc = Arc::clone(&rpc);
        tokio::spawn(async move {
            loop {
                match peer.recv().await {
                    Ok(Some(bytes)) => match rpc::deserialize(&bytes) {
                        Ok(msg) => {
                            rpc.notify_message(addr, msg, Some(Arc::clone(&peer))).await;
                        }
                        Err(err) => {
                            tracing::warn!("rpc deserialize error: {}", err);
                        }
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
    use super::run_rpc_example;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rpc_round_trip() {
        let result = run_rpc_example().await.expect("rpc example should succeed");
        assert_eq!(result, "TEST");
    }
}
