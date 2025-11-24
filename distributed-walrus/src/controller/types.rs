//! Helpers for translating between logical topics and Walrus keys.

pub fn wal_key(topic: &str, segment: u64) -> String {
    format!("t_{}_s_{}", topic, segment)
}

/// Extract (topic, segment) from a wal_key such as `t_topic_s_3`.
pub fn parse_wal_key(wal_key: &str) -> Option<(String, u64)> {
    let suffix = wal_key.rsplitn(2, "_s_").collect::<Vec<_>>();
    if suffix.len() != 2 {
        return None;
    }
    let topic_part = suffix[1].strip_prefix("t_")?;
    let segment = suffix[0].parse::<u64>().ok()?;
    Some((topic_part.to_string(), segment))
}

/// Per-connection read cursor for a topic.
#[derive(Default, Clone, Debug)]
pub struct ReadCursor {
    pub segment: u64,
    pub delivered_in_segment: u64,
}
