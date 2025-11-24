//! Helpers for translating between logical topics/partitions and Walrus keys.

pub fn wal_key(topic: &str, partition: u32, generation: u64) -> String {
    format!("t_{}_p_{}_g_{}", topic, partition, generation)
}
