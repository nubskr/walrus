use bytes::Bytes;
use octopii::StateMachineTrait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub type NodeId = u64;
pub type TopicName = String;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterState {
    pub topics: HashMap<TopicName, TopicInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicInfo {
    pub partitions: u32,
    pub partition_states: HashMap<u32, PartitionState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionState {
    pub current_generation: u64,
    pub leader_node: NodeId,
    #[serde(default)]
    pub history: Vec<SegmentRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentRecord {
    pub generation: u64,
    pub stored_on_node: NodeId,
    pub start_offset: u64,
    pub end_offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MetadataCmd {
    CreateTopic {
        name: String,
        partitions: u32,
        initial_leader: NodeId,
    },
    RolloverPartition {
        name: String,
        partition: u32,
        new_leader: NodeId,
        sealed_segment_size_bytes: u64,
    },
    TrimHistory {
        name: String,
        partition: u32,
        up_to_generation: u64,
    },
}

#[derive(Clone)]
pub struct MetadataStateMachine {
    state: Arc<RwLock<ClusterState>>,
}

impl MetadataStateMachine {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(ClusterState::default())),
        }
    }

    pub fn snapshot_state(&self) -> ClusterState {
        self.state
            .read()
            .map(|s| s.clone())
            .unwrap_or_else(|_| ClusterState::default())
    }

    pub fn get_partition_leader(&self, topic: &str, partition: u32) -> Option<(NodeId, u64)> {
        let guard = self.state.read().ok()?;
        let topic_info = guard.topics.get(topic)?;
        let part = topic_info.partition_states.get(&partition)?;
        Some((part.leader_node, part.current_generation))
    }

    pub fn get_partition_state(&self, topic: &str, partition: u32) -> Option<PartitionState> {
        let guard = self.state.read().ok()?;
        let topic_info = guard.topics.get(topic)?;
        topic_info.partition_states.get(&partition).cloned()
    }

    pub fn assignments_for_node(&self, node_id: NodeId) -> Vec<(String, u32, u64)> {
        let guard = match self.state.read() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };
        let mut out = Vec::new();
        for (topic, info) in guard.topics.iter() {
            for (part_id, state) in info.partition_states.iter() {
                if state.leader_node == node_id {
                    out.push((topic.clone(), *part_id, state.current_generation));
                }
            }
        }
        out
    }

    /// If a node is removed from membership, reassign any leaderships it held to the provided replacement.
    pub fn reassign_leader(&self, removed: NodeId, replacement: NodeId) {
        if let Ok(mut guard) = self.state.write() {
            for (_topic, info) in guard.topics.iter_mut() {
                for (_id, part) in info.partition_states.iter_mut() {
                    if part.leader_node == removed {
                        part.leader_node = replacement;
                    }
                }
            }
        }
    }
}

impl StateMachineTrait for MetadataStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let cmd: MetadataCmd =
            bincode::deserialize(command).map_err(|e| format!("decode cmd: {e}"))?;
        let mut state = self
            .state
            .write()
            .map_err(|_| "state poisoned".to_string())?;

        match cmd {
            MetadataCmd::CreateTopic {
                name,
                partitions,
                initial_leader,
            } => {
                if state.topics.contains_key(&name) {
                    return Ok(Bytes::from_static(b"EXISTS"));
                }

                let mut partition_states = HashMap::new();
                for id in 0..partitions {
                    partition_states.insert(
                        id,
                        PartitionState {
                            current_generation: 1,
                            leader_node: initial_leader,
                            history: Vec::new(),
                        },
                    );
                }

                state.topics.insert(
                    name,
                    TopicInfo {
                        partitions,
                        partition_states,
                    },
                );
                Ok(Bytes::from_static(b"CREATED"))
            }
            MetadataCmd::RolloverPartition {
                name,
                partition,
                new_leader,
                sealed_segment_size_bytes,
            } => {
                if let Some(topic) = state.topics.get_mut(&name) {
                    if let Some(part) = topic.partition_states.get_mut(&partition) {
                        let prev_head_start = part
                            .history
                            .last()
                            .map(|s| s.end_offset)
                            .unwrap_or(0);
                        let sealed_end = prev_head_start + sealed_segment_size_bytes;
                        let record = SegmentRecord {
                            generation: part.current_generation,
                            stored_on_node: part.leader_node,
                            start_offset: prev_head_start,
                            end_offset: sealed_end,
                        };
                        part.history.push(record);
                        part.current_generation += 1;
                        part.leader_node = new_leader;
                        return Ok(Bytes::from_static(b"ROLLED"));
                    }
                }
                Err("Topic or partition not found".into())
            }
            MetadataCmd::TrimHistory {
                name,
                partition,
                up_to_generation,
            } => {
                if let Some(topic) = state.topics.get_mut(&name) {
                    if let Some(part) = topic.partition_states.get_mut(&partition) {
                        part.history
                            .retain(|seg| seg.generation > up_to_generation);
                        return Ok(Bytes::from_static(b"TRIMMED"));
                    }
                }
                Err("Topic or partition not found".into())
            }
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let state = self.state.read().ok();
        bincode::serialize(state.as_deref().unwrap_or(&ClusterState::default())).unwrap_or_default()
    }

    fn restore(&self, data: &[u8]) -> Result<(), String> {
        let recovered: ClusterState =
            bincode::deserialize(data).map_err(|e| format!("snapshot decode: {e}"))?;
        let mut guard = self
            .state
            .write()
            .map_err(|_| "state poisoned".to_string())?;
        *guard = recovered;
        Ok(())
    }
}
