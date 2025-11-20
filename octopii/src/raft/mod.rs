mod rpc;
mod state_machine;
mod storage;

pub use rpc::*;
pub use state_machine::{KvStateMachine, StateMachine, StateMachineTrait};
pub use storage::WalStorage;

use crate::error::Result;
use raft::{prelude::*, Config as RaftConfig, RawNode};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

/// Raft node wrapper
pub struct RaftNode {
    raw_node: Arc<Mutex<RawNode<WalStorage>>>,
    node_id: u64,
    ready_notify: Arc<Notify>,
}

impl RaftNode {
    /// Create a new Raft node
    ///
    /// For a fresh cluster (following TiKV five_mem_node pattern):
    /// - Leader node (is_leader=true): Initializes with a snapshot containing ONLY itself as voter
    /// - Follower nodes (is_leader=false): Start with empty storage, will initialize from leader messages
    /// - If peers list is non-empty: Bootstrap ALL nodes with full peer list (TiKV run() pattern)
    pub async fn new(
        node_id: u64,
        peers: Vec<u64>,
        storage: WalStorage,
        is_leader: bool,
    ) -> Result<Self> {
        // Check if storage is already initialized (e.g., on restart)
        let is_initialized = storage.initial_state()?.initialized();

        if is_initialized {
            tracing::info!(
                "Raft node {} restarting with existing state (storage already initialized)",
                node_id
            );
        } else {
            // Fresh node - bootstrap with initial snapshot
            // If peers list provided, bootstrap ALL nodes with all peers (like TiKV run())
            // Otherwise use ConfChange pattern (leader=self, followers=empty)
            if !peers.is_empty() {
                // TiKV run() pattern: ALL nodes start with ALL peers as voters
                let mut all_voters = peers.clone();
                all_voters.push(node_id);
                all_voters.sort_unstable();
                all_voters.dedup();

                let mut snapshot = Snapshot::default();
                snapshot.mut_metadata().index = 1;
                snapshot.mut_metadata().term = 1;
                snapshot.mut_metadata().mut_conf_state().voters = all_voters.clone();

                storage.apply_snapshot(snapshot)?;

                tracing::info!(
                    "Bootstrapped Raft node {} with voters {:?} (multi-node bootstrap)",
                    node_id,
                    all_voters
                );
            } else if is_leader {
                // ConfChange pattern: Leader initializes with ONLY itself as voter
                let mut snapshot = Snapshot::default();
                snapshot.mut_metadata().index = 1;
                snapshot.mut_metadata().term = 1;
                snapshot.mut_metadata().mut_conf_state().voters = vec![node_id];

                storage.apply_snapshot(snapshot)?;

                tracing::info!(
                    "Bootstrapped Raft LEADER node {} (will add peers via ConfChange)",
                    node_id
                );
            } else {
                // Followers start with minimal empty state for ConfChange pattern
                tracing::info!(
                    "Created Raft FOLLOWER node {} (will initialize from leader)",
                    node_id
                );
            }
        }

        let config = RaftConfig {
            id: node_id,
            election_tick: 10,
            heartbeat_tick: 3,
            pre_vote: true, // Enable pre-vote to prevent election storms
            ..Default::default()
        };

        config.validate()?;

        let raw_node = RawNode::new(
            &config,
            storage,
            &slog::Logger::root(slog::Discard, slog::o!()),
        )?;

        let raft_node = Arc::new(Mutex::new(raw_node));
        let notify = Arc::new(Notify::new());
        // Ensure ready handler processes initial Ready (leaders have pending entries immediately)
        notify.notify_one();

        Ok(Self {
            raw_node: raft_node,
            node_id,
            ready_notify: notify,
        })
    }

    /// Get the node ID
    pub fn id(&self) -> u64 {
        self.node_id
    }

    /// Propose a change to the state machine
    pub async fn propose(&self, data: Vec<u8>) -> Result<()> {
        let mut node = self.raw_node.lock().await;
        let last_index_before = node.raft.raft_log.last_index();
        node.propose(vec![], data)?;
        tracing::debug!(
            "Proposed entry, last_index was {} (role: {:?})",
            last_index_before,
            node.raft.state
        );
        if node.has_ready() {
            self.ready_notify.notify_one();
        }
        Ok(())
    }

    /// Tick the Raft node (should be called periodically)
    pub async fn tick(&self) {
        let mut node = self.raw_node.lock().await;
        node.tick();
        if node.has_ready() {
            self.ready_notify.notify_one();
        }
    }

    /// Process a Raft message
    pub async fn step(&self, msg: Message) -> Result<()> {
        let mut node = self.raw_node.lock().await;
        let msg_type = msg.get_msg_type();
        node.step(msg)?;
        let has_ready = node.has_ready();
        tracing::info!(
            "Node {} stepped {:?}, has_ready={}",
            self.node_id,
            msg_type,
            has_ready
        );
        if has_ready {
            tracing::info!("Node {} notifying ready after {:?}", self.node_id, msg_type);
            self.ready_notify.notify_one();
        }
        Ok(())
    }

    /// Check if there are ready messages to process
    pub async fn ready(&self) -> Option<Ready> {
        let mut node = self.raw_node.lock().await;
        if node.has_ready() {
            tracing::info!("Node {} ready() available", self.node_id);
            Some(node.ready())
        } else {
            None
        }
    }

    /// Advance the Raft state machine after processing ready
    /// Returns LightReady which contains committed entries
    pub async fn advance(&self, rd: Ready) -> raft::LightReady {
        let mut node = self.raw_node.lock().await;
        let light_rd = node.advance(rd);
        light_rd
    }

    /// Finish applying committed entries
    pub async fn advance_apply(&self) {
        let mut node = self.raw_node.lock().await;
        node.advance_apply();
    }

    /// Persist entries from Ready to storage
    pub async fn persist_entries(&self, entries: &[Entry]) {
        // Clone entries to avoid holding lock during WAL write
        let entries_vec: Vec<Entry> = entries.to_vec();

        let store = {
            let node = self.raw_node.lock().await;
            node.store().clone()
        }; // Release lock before async WAL operations

        // Actually persist to WAL (not just in-memory!)
        if let Err(e) = store.append_entries(&entries_vec).await {
            tracing::error!(
                "Failed to persist {} entries to WAL: {}",
                entries_vec.len(),
                e
            );
        }
    }

    /// Persist hard state from Ready to storage
    pub async fn persist_hard_state(&self, hs: HardState) {
        let node = self.raw_node.lock().await;
        node.store().set_hard_state(hs);
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        let node = self.raw_node.lock().await;
        node.raft.state == raft::StateRole::Leader
    }

    /// Check if the cluster has a leader
    pub async fn has_leader(&self) -> bool {
        let node = self.raw_node.lock().await;
        node.raft.leader_id != raft::INVALID_ID
    }

    /// Campaign to become leader (triggers election)
    pub async fn campaign(&self) -> Result<()> {
        let mut node = self.raw_node.lock().await;
        node.campaign()?;
        tracing::info!("Node {} starting election campaign", self.node_id);
        if node.has_ready() {
            self.ready_notify.notify_one();
        }
        Ok(())
    }

    /// Transfer leadership to another node
    ///
    /// This requests the current leader to transfer leadership to the specified node.
    /// Used for planned maintenance and load balancing.
    ///
    /// # Arguments
    /// * `target_id` - The ID of the node to transfer leadership to
    pub async fn transfer_leader(&self, target_id: u64) -> Result<()> {
        let mut node = self.raw_node.lock().await;
        node.transfer_leader(target_id);
        tracing::info!(
            "Node {} requested leadership transfer to node {}",
            self.node_id,
            target_id
        );
        if node.has_ready() {
            self.ready_notify.notify_one();
        }
        Ok(())
    }

    /// Request a read index for linearizable reads
    ///
    /// The leader confirms it's still the leader by communicating with a quorum
    /// and returns a read index. Reads at or before this index are linearizable.
    ///
    /// # Arguments
    /// * `request_ctx` - Context to identify this read request
    pub async fn read_index(&self, request_ctx: Vec<u8>) -> Result<()> {
        let mut node = self.raw_node.lock().await;
        node.read_index(request_ctx);
        tracing::info!("Node {} requested read index", self.node_id);
        if node.has_ready() {
            self.ready_notify.notify_one();
        }
        Ok(())
    }

    /// Propose a configuration change (add/remove node)
    pub async fn propose_conf_change(&self, context: Vec<u8>, cc: ConfChange) -> Result<()> {
        let change_type = cc.get_change_type();
        let node_id = cc.node_id;
        let mut node = self.raw_node.lock().await;
        node.propose_conf_change(context, cc)?;
        tracing::info!(
            "Proposed ConfChange: {:?} for node {}",
            change_type,
            node_id
        );
        if node.has_ready() {
            self.ready_notify.notify_one();
        }
        Ok(())
    }

    /// Apply a committed configuration change
    pub async fn apply_conf_change(&self, cc: &ConfChange) -> Result<ConfState> {
        let mut node = self.raw_node.lock().await;
        let conf_state = node.apply_conf_change(cc)?;
        tracing::info!(
            "Applied ConfChange: {:?} for node {}, new voters: {:?}",
            cc.get_change_type(),
            cc.node_id,
            conf_state.voters
        );

        // CRITICAL: After applying ConfChange, explicitly broadcast to ensure newly added
        // peers get initial replication, even if they're slow to start or haven't responded yet.
        // This prevents Progress from getting stuck in paused state.
        if node.raft.state == raft::StateRole::Leader {
            node.raft.bcast_append();
        }

        if node.has_ready() {
            self.ready_notify.notify_one();
        }
        Ok(conf_state)
    }

    /// Get current hard state
    pub async fn hard_state(&self) -> HardState {
        let node = self.raw_node.lock().await;
        node.raft.hard_state().clone()
    }

    /// Try to compact logs (threshold-based)
    pub async fn try_compact_logs(
        &self,
        applied_index: u64,
        state_machine_data: Vec<u8>,
    ) -> Result<()> {
        let node = self.raw_node.lock().await;
        node.store()
            .compact_logs(applied_index, state_machine_data)?;
        Ok(())
    }

    /// Get progress information for a peer (used for learner promotion)
    pub async fn peer_progress(&self, peer_id: u64) -> Option<(u64, u64)> {
        let node = self.raw_node.lock().await;
        if let Some(progress) = node.raft.prs().get(peer_id) {
            let leader_last_index = node.raft.raft_log.last_index();
            Some((progress.matched, leader_last_index))
        } else {
            None
        }
    }

    /// Proactively trigger snapshot catch-up for lagging peers
    /// If a peer lags the leader by more than `lag_threshold` log entries, transition its
    /// Progress state to Snapshot at the leader's committed index to initiate snapshot transfer.
    pub async fn check_and_trigger_snapshot(&self, lag_threshold: u64) {
        if lag_threshold == 0 {
            return;
        }
        let mut node = self.raw_node.lock().await;

        // Only the leader should evaluate and trigger snapshot catch-up
        if node.raft.state != raft::StateRole::Leader {
            return;
        }

        let leader_last_index = node.raft.raft_log.last_index();
        let commit_index = node.raft.hard_state().commit;

        // Gather peer IDs from current configuration (voters + learners, including joint)
        let mut peer_ids: Vec<u64> = Vec::new();
        let status = node.status();
        if let Some(progress) = status.progress {
            let cs = progress.conf().to_conf_state();
            peer_ids.extend(cs.voters);
            peer_ids.extend(cs.voters_outgoing);
            peer_ids.extend(cs.learners);
            peer_ids.extend(cs.learners_next);
            peer_ids.sort_unstable();
            peer_ids.dedup();
        }

        // Detect lagging peers
        let mut lagging_peers: Vec<u64> = Vec::new();
        let mut lag_samples: Vec<(u64, u64)> = Vec::new();
        for peer_id in peer_ids {
            if peer_id == self.node_id {
                continue;
            }
            if let Some(pr) = node.raft.prs().get(peer_id) {
                let lag = leader_last_index.saturating_sub(pr.matched);
                if lag > lag_threshold {
                    lagging_peers.push(peer_id);
                }
                lag_samples.push((peer_id, lag));
            }
        }

        if lagging_peers.is_empty() {
            return;
        }

        // Log a concise summary of peer lags
        lag_samples.sort_by_key(|&(_, lag)| std::cmp::Reverse(lag));
        let top_samples: Vec<String> = lag_samples
            .into_iter()
            .take(3)
            .map(|(pid, lag)| format!("{}:{}e", pid, lag))
            .collect();
        tracing::info!(
            "[Node {}] {} lagging peers over threshold {} (leader_last={}): top={:?}",
            self.node_id,
            lagging_peers.len(),
            lag_threshold,
            leader_last_index,
            top_samples
        );
        println!(
            "[snapshot-trigger] leader={} lagging_peers={:?} threshold={} leader_last={}",
            self.node_id, lagging_peers, lag_threshold, leader_last_index
        );

        // Transition lagging peers to Snapshot state at commit_index
        let mut any_changed = false;
        for pid in lagging_peers {
            if let Some(pr) = node.raft.mut_prs().get_mut(pid) {
                pr.become_snapshot(commit_index);
                tracing::info!(
                    "[Node {}] Triggering snapshot catch-up for peer {} at commit {} (leader last={})",
                    self.node_id,
                    pid,
                    commit_index,
                    leader_last_index
                );
                println!(
                    "[snapshot-trigger] become_snapshot peer={} at_commit={} (leader_last={})",
                    pid, commit_index, leader_last_index
                );
                any_changed = true;
            }
        }

        // Force immediate send so followers receive snapshot/appends without waiting for next tick
        if any_changed {
            node.raft.bcast_append();
            println!("[snapshot-trigger] bcast_append after become_snapshot");
        }
        // Ensure the ready loop is notified so snapshot/append messages are emitted promptly
        self.ready_notify.notify_one();
    }

    /// Expose raw_node for advanced operations
    pub fn raw_node(&self) -> &Arc<Mutex<RawNode<WalStorage>>> {
        &self.raw_node
    }

    pub fn ready_notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.ready_notify)
    }

    /// Prepare a fresh snapshot in storage at the current commit index so the leader
    /// can serve it to lagging peers. This does NOT compact logs; it only updates
    /// the storage snapshot image so `Storage::snapshot()` returns a valid snapshot.
    ///
    /// The provided `snapshot_data` should be the serialized state machine image.
    pub async fn prepare_serving_snapshot(&self, snapshot_data: Vec<u8>) -> Result<()> {
        let mut node = self.raw_node.lock().await;

        // Only meaningful on leader
        if node.raft.state != raft::StateRole::Leader {
            tracing::debug!(
                "[Node {}] Skipping prepare_serving_snapshot (not leader)",
                self.node_id
            );
            return Ok(());
        }

        // Determine target index/term for snapshot
        let commit_index = node.raft.hard_state().commit;
        if commit_index == 0 {
            // Nothing committed yet; no snapshot to prepare
            tracing::debug!(
                "[Node {}] Skipping prepare_serving_snapshot (commit_index=0)",
                self.node_id
            );
            return Ok(());
        }

        // If current storage snapshot is already at/after commit_index, nothing to do
        let current_snap_index = node.store().snapshot(0, 0).map(|s| s.get_metadata().index).unwrap_or(0);
        if current_snap_index >= commit_index {
            tracing::debug!(
                "[Node {}] Storage snapshot already up-to-date (have {}, need {})",
                self.node_id,
                current_snap_index,
                commit_index
            );
            return Ok(());
        }

        // Determine term at commit index (fall back to hard_state.term if unavailable)
        let term_at_commit = match node.raft.raft_log.term(commit_index) {
            Ok(t) => t,
            Err(_) => node.raft.hard_state().term,
        };

        tracing::info!(
            "[Node {}] Preparing serving snapshot at index {} (term {}, prev_snap={})",
            self.node_id,
            commit_index,
            term_at_commit,
            current_snap_index
        );

        // Build snapshot metadata using current conf state
        let mut snapshot = Snapshot::default();
        snapshot.data = bytes::Bytes::from(snapshot_data);
        {
            let meta = snapshot.mut_metadata();
            meta.index = commit_index;
            meta.term = term_at_commit;
            *meta.mut_conf_state() = node.raft.prs().conf().to_conf_state();
        }

        // Apply snapshot to storage (does not trim logs)
        node.store().apply_snapshot(snapshot)?;

        // Wake ready loop (followers may immediately request the snapshot)
        self.ready_notify.notify_one();
        tracing::info!(
            "[Node {}] Prepared serving snapshot at index {}",
            self.node_id,
            commit_index
        );
        Ok(())
    }

    /// Report snapshot sending status back to raft-rs (TiKV pattern).
    /// Must be called after finishing or failing snapshot transfer so raft
    /// can move the peer out of Snapshot state and continue replication.
    pub async fn report_snapshot(&self, to_peer_id: u64, status: raft::SnapshotStatus) {
        let mut node = self.raw_node.lock().await;
        node.report_snapshot(to_peer_id, status);
        if node.has_ready() {
            self.ready_notify.notify_one();
        }
    }

    /// Report a peer as unreachable (TiKV pattern).
    /// This informs raft-rs to adjust its sending behavior (e.g., pause, probe).
    pub async fn report_unreachable(&self, to_peer_id: u64) {
        let mut node = self.raw_node.lock().await;
        node.report_unreachable(to_peer_id);
        if node.has_ready() {
            self.ready_notify.notify_one();
        }
    }
}
