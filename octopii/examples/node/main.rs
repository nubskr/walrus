use octopii::{Config, OctopiiNode, OctopiiRuntime};
use std::error::Error;
use std::time::Duration;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error>> {
    let value = run_node_example().await?;
    println!("Replicated value: {}", value);
    Ok(())
}

pub async fn run_node_example() -> Result<String, Box<dyn Error>> {
    let data_dir = tempfile::tempdir()?;
    let wal_dir = data_dir.path().join("node");
    std::fs::create_dir_all(&wal_dir)?;

    let mut config = Config::default();
    config.bind_addr = "127.0.0.1:0".parse()?;
    config.peers = Vec::new();
    config.wal_dir = wal_dir;
    config.is_initial_leader = true;
    config.worker_threads = 2;

    let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());
    let node = OctopiiNode::new(config, runtime).await?;

    node.start().await?;
    node.campaign().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    node.propose(b"SET sample value".to_vec()).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let response = node.query(b"GET sample").await?;

    node.shutdown();

    Ok(String::from_utf8(response.to_vec())?)
}

#[cfg(test)]
mod tests {
    use super::run_node_example;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_node_proposes_and_reads() {
        let value = run_node_example().await.expect("node example should work");
        assert_eq!(value, "value");
    }
}
