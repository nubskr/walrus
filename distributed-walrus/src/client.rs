use crate::controller::NodeController;
use anyhow::{anyhow, Result};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

const MAX_FRAME_LEN: usize = 64 * 1024;

pub async fn start_client_listener(
    controller: Arc<NodeController>,
    bind_addr: String,
) -> Result<()> {
    let listener = TcpListener::bind(&bind_addr).await?;
    info!("Client listener bound on {}", bind_addr);

    loop {
        let (socket, addr) = listener.accept().await?;
        let controller_clone = controller.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, controller_clone).await {
                warn!("Client connection {} closed with error: {}", addr, e);
            }
        });
    }
}

async fn handle_connection(mut socket: TcpStream, controller: Arc<NodeController>) -> Result<()> {
    loop {
        let mut len_buf = [0u8; 4];
        if let Err(e) = socket.read_exact(&mut len_buf).await {
            // Graceful EOF ends the loop; bubble up real errors.
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(());
            }
            return Err(e.into());
        }

        let frame_len = u32::from_le_bytes(len_buf) as usize;
        if frame_len == 0 || frame_len > MAX_FRAME_LEN {
            send_response(&mut socket, "ERR invalid frame length").await?;
            continue;
        }

        let mut buf = vec![0u8; frame_len];
        socket.read_exact(&mut buf).await?;
        let text = match String::from_utf8(buf) {
            Ok(s) => s,
            Err(_) => {
                send_response(&mut socket, "ERR invalid utf-8").await?;
                continue;
            }
        };

        let response = match handle_command(text.trim_end(), controller.clone()).await {
            Ok(msg) => msg,
            Err(e) => format!("ERR {}", e),
        };

        send_response(&mut socket, &response).await?;
    }
}

async fn handle_command(line: &str, controller: Arc<NodeController>) -> Result<String> {
    let mut parts = line.splitn(3, ' ');
    let Some(op) = parts.next() else {
        return Err(anyhow!("empty command"));
    };
    tracing::info!("client command received: {}", line);

    match op {
        "REGISTER" => {
            let topic = parts
                .next()
                .ok_or_else(|| anyhow!("REGISTER requires a topic"))?;
            controller.ensure_topic(topic).await?;
            Ok("OK".into())
        }
        "PUT" => {
            let topic = parts
                .next()
                .ok_or_else(|| anyhow!("PUT requires a topic"))?;
            let payload = parts
                .next()
                .ok_or_else(|| anyhow!("PUT requires a payload"))?;
            controller
                .append_for_topic(topic, payload.as_bytes().to_vec())
                .await?;
            Ok("OK".into())
        }
        "GET" => {
            let topic = parts
                .next()
                .ok_or_else(|| anyhow!("GET requires a topic"))?;
            match controller.read_one_for_topic_shared(topic).await? {
                Some(bytes) => Ok(format!("OK {}", String::from_utf8_lossy(&bytes))),
                None => Ok("EMPTY".into()),
            }
        }
        "STATE" => {
            let topic = parts
                .next()
                .ok_or_else(|| anyhow!("STATE requires a topic"))?;
            Ok(controller.topic_snapshot(topic)?)
        }
        "METRICS" => Ok(controller.get_metrics()?),
        _ => Err(anyhow!("unknown command")),
    }
}

async fn send_response(socket: &mut TcpStream, message: &str) -> Result<()> {
    let bytes = message.as_bytes();
    let len = bytes.len() as u32;
    socket.write_all(&len.to_le_bytes()).await?;
    socket.write_all(bytes).await?;
    Ok(())
}
