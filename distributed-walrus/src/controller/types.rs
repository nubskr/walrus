//! Helpers for translating between logical topics and Walrus keys.

pub fn wal_key(topic: &str, segment: u64) -> String {
    format!("t_{}_s_{}", topic, segment)
}
