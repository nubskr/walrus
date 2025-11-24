use crate::controller::parse_wal_key;
use crate::controller::NodeController;
use crate::rpc::InternalResp;

impl NodeController {
    pub(crate) async fn forward_append(&self, wal_key: String, data: Vec<u8>) -> InternalResp {
        self.update_leases().await;
        match self.append_with_retry(&wal_key, data).await {
            Ok(_) => {
                tracing::info!("handle_rpc: append success for {}", wal_key);
                self.record_append(&wal_key, 1).await; // 1 entry appended
                if let Some((topic, segment)) = parse_wal_key(&wal_key) {
                    if let Err(e) = self.maybe_rollover(&topic, segment).await {
                        tracing::warn!(
                            "handle_rpc: rollover check failed for {} segment {}: {}",
                            topic,
                            segment,
                            e
                        );
                    }
                }
                InternalResp::Ok
            }
            Err(e) => {
                tracing::error!("handle_rpc: append failed for {}: {}", wal_key, e);
                InternalResp::Error(e.to_string())
            }
        }
    }
}
