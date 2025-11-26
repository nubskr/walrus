use anyhow::{anyhow, Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct CliClient {
    addr: String,
}

impl CliClient {
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }

    pub async fn register(&self, topic: &str) -> Result<()> {
        self.simple_ok(&format!("REGISTER {}", topic)).await
    }

    pub async fn put(&self, topic: &str, data: &str) -> Result<()> {
        self.simple_ok(&format!("PUT {} {}", topic, data)).await
    }

    /// Returns Ok(None) if the topic is empty.
    pub async fn get(&self, topic: &str) -> Result<Option<String>> {
        let resp = self.send_raw(&format!("GET {}", topic)).await?;
        if resp == "EMPTY" {
            return Ok(None);
        }
        if let Some(rest) = resp.strip_prefix("OK ") {
            return Ok(Some(rest.to_string()));
        }
        Err(anyhow!("unexpected GET response: {}", resp))
    }

    pub async fn state(&self, topic: &str) -> Result<String> {
        self.send_payload(&format!("STATE {}", topic)).await
    }

    pub async fn metrics(&self) -> Result<String> {
        self.send_payload("METRICS").await
    }

    pub async fn send_raw(&self, line: &str) -> Result<String> {
        let mut stream = TcpStream::connect(&self.addr)
            .await
            .with_context(|| format!("connect to {}", self.addr))?;
        let bytes = line.as_bytes();
        let len = bytes.len() as u32;
        stream
            .write_all(&len.to_le_bytes())
            .await
            .context("write length")?;
        stream.write_all(bytes).await.context("write payload")?;

        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .context("read length")?;
        let resp_len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; resp_len];
        stream.read_exact(&mut buf).await.context("read payload")?;
        let text = String::from_utf8(buf).context("utf-8 decode")?;
        Ok(text)
    }

    async fn simple_ok(&self, line: &str) -> Result<()> {
        let resp = self.send_raw(line).await?;
        if resp == "OK" {
            return Ok(());
        }
        Err(anyhow!(resp))
    }

    async fn send_payload(&self, line: &str) -> Result<String> {
        let resp = self.send_raw(line).await?;
        if let Some(rest) = resp.strip_prefix("OK ") {
            return Ok(rest.to_string());
        }
        if resp.starts_with("ERR") {
            return Err(anyhow!(resp));
        }
        Ok(resp)
    }
}
