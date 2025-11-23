//! Kafka protocol helpers shared across server handlers.

use std::collections::HashSet;

use bytes::{BufMut, BytesMut};

use crate::kafka::codec::KafkaPrimitive;
use crate::metadata::{ClusterState, PartitionState};

pub fn encode_api_versions_response(res: &mut BytesMut) {
    res.put_i16(0);
    let supported = [
        (0i16, 0i16),
        (1, 0i16),
        (3, 0),
        (18, 0),
        (19, 0),
        (50, 0),
        (51, 0),
        (60, 0),
        (61, 0),
        (62, 0),
        (63, 0),
    ];
    res.put_i32(supported.len() as i32);
    for (api, version) in supported {
        res.put_i16(api);
        res.put_i16(version);
        res.put_i16(version);
    }
}

pub fn encode_topics<F>(
    res: &mut BytesMut,
    state: ClusterState,
    topics_filter: Vec<String>,
    mut encode_fn: F,
) where
    F: FnMut(&mut BytesMut, String, Vec<(u32, PartitionState)>),
{
    let requested: HashSet<String> = topics_filter.into_iter().collect();
    let use_filter = !requested.is_empty();
    let mut topics: Vec<_> = state.topics.into_iter().collect();
    topics.sort_by(|a, b| a.0.cmp(&b.0));
    res.put_i32(
        topics
            .iter()
            .filter(|(name, _)| !use_filter || requested.contains(name))
            .count() as i32,
    );
    for (name, info) in topics.into_iter() {
        if use_filter && !requested.contains(&name) {
            continue;
        }
        let parts: Vec<_> = info.partition_states.into_iter().collect();
        encode_fn(res, name, parts);
    }
}
