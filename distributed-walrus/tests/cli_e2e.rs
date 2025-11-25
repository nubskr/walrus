use std::net::{SocketAddr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use distributed_walrus::cli_client::CliClient;
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::time::sleep;

struct RunningNode {
    _dir: TempDir,
    child: Child,
    client_addr: String,
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_end_to_end_roundtrip_and_errors() -> anyhow::Result<()> {
    let node = start_single_node().await?;
    wait_for_port(&node.client_addr).await?;
    let client = CliClient::new(&node.client_addr);

    // Unknown topic should error.
    let err = client.put("missing", "payload").await.unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("unknown topic"),
        "unexpected error: {err}"
    );

    client.register("logs").await?;
    // Idempotent register should not fail.
    client.register("logs").await?;

    let messages = vec!["one", "two", "three", "four", "five"];
    for msg in &messages {
        client.put("logs", msg).await?;
    }

    for expected in &messages {
        let got = client.get("logs").await?;
        assert_eq!(got.as_deref(), Some(*expected));
    }

    // Queue drained -> EMPTY.
    assert_eq!(client.get("logs").await?, None);

    // State returns JSON payload.
    let state = client.state("logs").await?;
    assert!(
        state.contains("leader_node"),
        "state did not look like JSON: {state}"
    );

    // Metrics endpoint responds with JSON.
    let metrics = client.metrics().await?;
    assert!(
        metrics.contains("state"),
        "metrics did not look like JSON: {metrics}"
    );

    // Garbage command should surface server error text.
    let garbage = client.send_raw("GARBAGE").await?;
    assert!(
        garbage.to_uppercase().starts_with("ERR"),
        "expected ERR for garbage command, got {garbage}"
    );

    Ok(())
}

async fn start_single_node() -> anyhow::Result<RunningNode> {
    let data_dir = tempfile::tempdir()?;
    let raft_port = next_free_port()?;
    let client_port = next_free_port()?;
    let client_addr = format!("127.0.0.1:{client_port}");

    let child = Command::new(env!("CARGO_BIN_EXE_distributed-walrus"))
        .arg("--node-id")
        .arg("1")
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--raft-port")
        .arg(raft_port.to_string())
        .arg("--client-port")
        .arg(client_port.to_string())
        .arg("--raft-host")
        .arg("127.0.0.1")
        .arg("--client-host")
        .arg("127.0.0.1")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    Ok(RunningNode {
        _dir: data_dir,
        child,
        client_addr,
    })
}

fn next_free_port() -> anyhow::Result<u16> {
    let sock = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let port = sock.local_addr()?.port();
    drop(sock);
    Ok(port)
}

async fn wait_for_port(addr: &str) -> anyhow::Result<()> {
    for _ in 0..50 {
        if TcpStream::connect(addr).await.is_ok() {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("port {addr} did not become ready");
}
