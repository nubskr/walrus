use crate::wal::WriteAheadLog;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

/// Trait for application state machines.
pub trait StateMachineTrait: Send + Sync {
    fn apply(&self, command: &[u8]) -> std::result::Result<Bytes, String>;
    fn snapshot(&self) -> Vec<u8>;
    fn restore(&self, data: &[u8]) -> std::result::Result<(), String>;
    fn compact(&self) -> std::result::Result<(), String> {
        Ok(())
    }
}

/// Shared state machine handle type.
pub type StateMachine = Arc<dyn StateMachineTrait>;

/// A minimal in-memory KV state machine used for tests and OpenRaft bootstrapping.
pub struct KvStateMachine {
    map: StdMutex<HashMap<Vec<u8>, Vec<u8>>>,
}

impl KvStateMachine {
    pub fn in_memory() -> Self {
        Self {
            map: StdMutex::new(HashMap::new()),
        }
    }
}

impl StateMachineTrait for KvStateMachine {
    fn apply(&self, command: &[u8]) -> std::result::Result<Bytes, String> {
        // Protocol: "SET key value" | "GET key" | "DELETE key"
        let s = std::str::from_utf8(command).map_err(|e| e.to_string())?;
        let mut tokens = s.split_whitespace();
        let op = tokens.next().unwrap_or("");
        match op {
            "SET" => {
                let key = tokens.next().ok_or_else(|| "SET missing key".to_string())?;
                let val = tokens
                    .next()
                    .ok_or_else(|| "SET missing value".to_string())?;
                self.map
                    .lock()
                    .unwrap()
                    .insert(key.as_bytes().to_vec(), val.as_bytes().to_vec());
                Ok(Bytes::from("OK"))
            }
            "GET" => {
                let key = tokens.next().ok_or_else(|| "GET missing key".to_string())?;
                let val_opt = self.map.lock().unwrap().get(key.as_bytes()).cloned();
                match val_opt {
                    Some(v) => Ok(Bytes::from(v)),
                    None => Ok(Bytes::from("NOT_FOUND")),
                }
            }
            "DELETE" => {
                let key = tokens
                    .next()
                    .ok_or_else(|| "DELETE missing key".to_string())?;
                self.map.lock().unwrap().remove(key.as_bytes());
                Ok(Bytes::from("OK"))
            }
            _ => Err("unknown op".into()),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        // naive bincode for tests
        bincode::serialize(&*self.map.lock().unwrap()).unwrap_or_default()
    }

    fn restore(&self, data: &[u8]) -> std::result::Result<(), String> {
        let map: HashMap<Vec<u8>, Vec<u8>> =
            bincode::deserialize(data).map_err(|e| e.to_string())?;
        *self.map.lock().unwrap() = map;
        Ok(())
    }
}

/// WAL-backed wrapper that replays commands on startup and durably appends writes.
pub struct WalBackedStateMachine {
    inner: StateMachine,
    wal: Arc<WriteAheadLog>,
}

impl WalBackedStateMachine {
    pub fn with_inner(inner: StateMachine, wal: Arc<WriteAheadLog>) -> Arc<Self> {
        let sm = Arc::new(Self { inner, wal });
        Self::replay_wal(&sm);
        sm
    }

    fn replay_wal(this: &Arc<Self>) {
        let wal = Arc::clone(&this.wal);
        let inner = Arc::clone(&this.inner);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                if let Ok(entries) = wal.read_all().await {
                    for entry in entries {
                        let _ = inner.apply(&entry);
                    }
                }
            })
        });
    }

    fn append_entry(&self, command: &[u8]) -> std::result::Result<(), String> {
        let data = Bytes::copy_from_slice(command);
        let wal = Arc::clone(&self.wal);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                wal.append(data)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            })
        })
    }
}

impl StateMachineTrait for WalBackedStateMachine {
    fn apply(&self, command: &[u8]) -> std::result::Result<Bytes, String> {
        self.append_entry(command)?;
        self.inner.apply(command)
    }

    fn snapshot(&self) -> Vec<u8> {
        self.inner.snapshot()
    }

    fn restore(&self, data: &[u8]) -> std::result::Result<(), String> {
        self.inner.restore(data)
    }

    fn compact(&self) -> std::result::Result<(), String> {
        self.inner.compact()
    }
}
