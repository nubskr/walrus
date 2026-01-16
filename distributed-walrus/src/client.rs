use crate::controller::NodeController;
use crate::token;
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
    let mut authenticated = false;
    let mut authenticated_user: Option<crate::auth::User> = None;

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

        let response = match handle_command(
            text.trim_end(),
            controller.clone(),
            &mut authenticated,
            &mut authenticated_user,
        )
        .await
        {
            Ok(msg) => msg,
            Err(e) => format!("ERR {}", e),
        };

        send_response(&mut socket, &response).await?;
    }
}

async fn handle_command(
    line: &str,
    controller: Arc<NodeController>,
    authenticated: &mut bool,
    authenticated_user: &mut Option<crate::auth::User>,
) -> Result<String> {
    let mut parts = line.splitn(3, ' ');
    let Some(op) = parts.next() else {
        return Err(anyhow!("empty command"));
    };
    // Mask API key in logs for security
    let log_line = if op == "APIKEY" {
        "client command received: APIKEY <redacted>".to_string()
    } else {
        format!("client command received: {}", line)
    };
    tracing::info!("{}", log_line);

    // APIKEY command - authenticate with permanent API key (no expiration)
    // Can also use LOGIN with username/password to get API key
    if op == "APIKEY" {
        let api_key = parts
            .next()
            .ok_or_else(|| anyhow!("APIKEY requires api_key"))?;

        if let Some(user) = controller.metadata.authenticate_with_api_key(api_key) {
            *authenticated = true;
            *authenticated_user = Some(user.clone());
            info!("User {} authenticated via API key (role: {:?})", user.username, user.role);
            return Ok(format!("OK authenticated as {} (role: {:?})", user.username, user.role));
        } else {
            return Err(anyhow!("invalid API key"));
        }
    }

    // LOGIN command - authenticate with username/password and returns API key
    if op == "LOGIN" {
        let username = parts
            .next()
            .ok_or_else(|| anyhow!("LOGIN requires username"))?;
        let password = parts
            .next()
            .ok_or_else(|| anyhow!("LOGIN requires password"))?;

        if let Some(user) = controller.metadata.authenticate(username, password) {
            *authenticated = true;
            *authenticated_user = Some(user.clone());

            info!("User {} authenticated successfully via LOGIN", username);
            return Ok(format!("OK authenticated\nAPI_KEY: {}\nROLE: {:?}\n\nSave this API key and use 'APIKEY <key>' for future connections",
                user.api_key, user.role));
        } else {
            return Err(anyhow!("invalid username or password"));
        }
    }

    // Check if authentication is required
    // Allow unauthenticated access only if no users exist (for initial setup)
    let requires_auth = controller.metadata.has_users();
    if requires_auth && !*authenticated {
        return Err(anyhow!("authentication required - use APIKEY <key> or LOGIN <username> <password>"));
    }

    match op {
        "REGISTER" => {
            // Check write permission
            if let Some(user) = authenticated_user {
                if !user.can_write() {
                    return Err(anyhow!("permission denied - this operation requires Producer or Admin role"));
                }
            }
            let topic = parts
                .next()
                .ok_or_else(|| anyhow!("REGISTER requires a topic"))?;
            controller.ensure_topic(topic).await?;
            Ok("OK topic registered".into())
        }
        "PUT" => {
            // Check write permission
            if let Some(user) = authenticated_user {
                if !user.can_write() {
                    return Err(anyhow!("permission denied - this operation requires Producer or Admin role"));
                }
            }
            let topic = parts
                .next()
                .ok_or_else(|| anyhow!("PUT requires a topic"))?;
            let payload = parts
                .next()
                .ok_or_else(|| anyhow!("PUT requires a payload"))?;
            controller
                .append_for_topic(topic, payload.as_bytes().to_vec())
                .await?;
            Ok("OK message published".into())
        }
        "GET" => {
            // Check read permission
            if let Some(user) = authenticated_user {
                if !user.can_read() {
                    return Err(anyhow!("permission denied - this operation requires Consumer or Admin role"));
                }
            }
            let topic = parts
                .next()
                .ok_or_else(|| anyhow!("GET requires a topic"))?;
            match controller.read_one_for_topic_shared(topic).await? {
                Some(bytes) => Ok(format!("OK {}", String::from_utf8_lossy(&bytes))),
                None => Ok("EMPTY".into()),
            }
        }
        "STATE" => {
            // Check read permission
            if let Some(user) = authenticated_user {
                if !user.can_read() {
                    return Err(anyhow!("permission denied - this operation requires Consumer or Admin role"));
                }
            }
            let topic = parts
                .next()
                .ok_or_else(|| anyhow!("STATE requires a topic"))?;
            Ok(controller.topic_snapshot(topic)?)
        }
        "METRICS" => {
            // Check read permission
            if let Some(user) = authenticated_user {
                if !user.can_read() {
                    return Err(anyhow!("permission denied - this operation requires Consumer or Admin role"));
                }
            }
            Ok(controller.get_metrics()?)
        }
        "ADDUSER" => {
            // Check admin permission
            if let Some(user) = authenticated_user {
                if !user.is_admin() {
                    return Err(anyhow!("permission denied - this operation requires Admin role"));
                }
            } else {
                return Err(anyhow!("authentication required"));
            }
            let username = parts
                .next()
                .ok_or_else(|| anyhow!("ADDUSER requires <username> <password> <role>"))?;
            let password = parts
                .next()
                .ok_or_else(|| anyhow!("ADDUSER requires <username> <password> <role>"))?;

            // For now, default to Producer role for backward compatibility
            // Can extend to parse role from command later
            let api_key = controller.add_producer(username, password).await?;
            Ok(format!("OK user added as Producer\nAPI_KEY: {}", api_key))
        }
        "ADDPRODUCER" => {
            // Check admin permission
            if let Some(user) = authenticated_user {
                if !user.is_admin() {
                    return Err(anyhow!("permission denied - this operation requires Admin role"));
                }
            } else {
                return Err(anyhow!("authentication required"));
            }
            let username = parts
                .next()
                .ok_or_else(|| anyhow!("ADDPRODUCER requires <username> <password>"))?;
            let password = parts
                .next()
                .ok_or_else(|| anyhow!("ADDPRODUCER requires <username> <password>"))?;

            let api_key = controller.add_producer(username, password).await?;
            Ok(format!("OK Producer added\nAPI_KEY: {}", api_key))
        }
        "ADDCONSUMER" => {
            // Check admin permission
            if let Some(user) = authenticated_user {
                if !user.is_admin() {
                    return Err(anyhow!("permission denied - this operation requires Admin role"));
                }
            } else {
                return Err(anyhow!("authentication required"));
            }
            let username = parts
                .next()
                .ok_or_else(|| anyhow!("ADDCONSUMER requires <username> <password>"))?;
            let password = parts
                .next()
                .ok_or_else(|| anyhow!("ADDCONSUMER requires <username> <password>"))?;

            let api_key = controller.add_consumer(username, password).await?;
            Ok(format!("OK Consumer added\nAPI_KEY: {}", api_key))
        }
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
