pub mod runtime;

pub mod chunk;
pub mod config;
pub mod error;
#[cfg(feature = "openraft")]
pub mod openraft;
pub mod rpc;
pub mod shipping_lane;
pub mod state_machine;
pub mod transport;
pub mod wal;

// Re-export main types
pub use chunk::{ChunkSource, TransferResult};
pub use config::Config;
pub use error::{OctopiiError, Result};
#[cfg(feature = "openraft")]
pub use openraft::node::OpenRaftNode as OctopiiNode;
pub use runtime::OctopiiRuntime;
pub use shipping_lane::ShippingLane;
pub use state_machine::{KvStateMachine, StateMachine, StateMachineTrait, WalBackedStateMachine};

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_runtime_from_handle() {
        // Test using handle from current runtime (no nested runtime creation)
        let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());
        let result = runtime.spawn(async { 42 }).await.unwrap();
        assert_eq!(result, 42);
    }
}
