//! Shared controller types for translating between logical topics/partitions and Walrus keys.
//! Keeps the wal_key format in one place so callers do not reimplement string munging.

/// Logical identifier for a topic + partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TopicPartition<'a> {
    pub topic: &'a str,
    pub partition: u32,
}

impl<'a> TopicPartition<'a> {
    pub fn new(topic: &'a str, partition: u32) -> Self {
        Self { topic, partition }
    }

    pub fn wal_key(self, generation: u64) -> String {
        wal_key(self.topic, self.partition, generation)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalKeyParts {
    pub topic: String,
    pub partition: u32,
    pub generation: u64,
}

pub fn wal_key(topic: &str, partition: u32, generation: u64) -> String {
    format!("t_{}_p_{}_g_{}", topic, partition, generation)
}

pub fn parse_wal_key(key: &str) -> Option<WalKeyParts> {
    if !key.starts_with("t_") {
        return None;
    }
    let rest = &key[2..];
    let p_idx = rest.rfind("_p_")?;
    let g_idx = rest.rfind("_g_")?;
    if p_idx >= g_idx {
        return None;
    }
    let topic = &rest[..p_idx];
    let partition = rest[p_idx + 3..g_idx].parse().ok()?;
    let generation = rest[g_idx + 3..].parse().ok()?;
    Some(WalKeyParts {
        topic: topic.to_string(),
        partition,
        generation,
    })
}
