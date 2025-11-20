use bytes::Bytes;
use octopii::transport::QuicTransport;
use octopii::{ChunkSource, ShippingLane};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error>> {
    let payload = run_shipping_lane_example().await?;
    println!("Shipping Lane transferred payload: {:?}", payload);
    Ok(())
}

pub async fn run_shipping_lane_example() -> Result<Bytes, Box<dyn Error>> {
    let receiver_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let receiver_transport = Arc::new(QuicTransport::new(receiver_addr).await?);
    let receiver_bind = receiver_transport.local_addr()?;

    let sender_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let sender_transport = Arc::new(QuicTransport::new(sender_addr).await?);
    let shipping_lane = ShippingLane::new(sender_transport.clone());

    let recv_task = tokio::spawn({
        let receiver_transport = Arc::clone(&receiver_transport);
        async move {
            let (_addr, peer) = receiver_transport.accept().await?;
            let data = peer
                .recv_chunk_verified()
                .await?
                .ok_or_else(|| octopii::error::OctopiiError::Transport("missing data".into()))?;
            Ok::<_, octopii::error::OctopiiError>(data)
        }
    });

    let payload = Bytes::from_static(b"hello-shipping-lane");
    let result = shipping_lane
        .send_memory(receiver_bind, payload.clone())
        .await?;
    assert!(result.success);

    let received = recv_task.await??;
    Ok(received)
}

#[cfg(test)]
mod tests {
    use super::run_shipping_lane_example;
    use bytes::Bytes;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shipping_lane_transfers_payload() {
        let received = run_shipping_lane_example()
            .await
            .expect("shipping lane example");
        assert_eq!(received, Bytes::from_static(b"hello-shipping-lane"));
    }
}
