<div align="center">
  <img src="./assets/octopii.png"
       alt="octopii"
       width="20%">
  <div>Octopii: A batteries-included framework for building distributed systems</div>
  <br>

[![CI](https://github.com/octopii-rs/octopii/actions/workflows/ci.yml/badge.svg)](https://github.com/octopii-rs/octopii/actions)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

</div>

---

## What's Included

Octopii provides everything you need to build replicated, fault-tolerant systems:

- **Raft Consensus** - Leader election, log replication, and membership changes via OpenRaft
- **QUIC Transport** - Fast, encrypted, multiplexed networking with Quinn
- **Durable Persistence** - Crash-resistant Write-Ahead Log (Walrus) with checksums
- **Pluggable State Machines** - Bring your own replication logic (KV stores, counters, registries, etc.)
- **Large File Transfers** - P2P streaming with automatic checksum verification (Shipping Lane)
- **RPC Framework** - Request/response messaging with correlation and timeouts
- **Runtime Management** - Flexible async execution with Tokio

## Architecture

<div>
  <img src="./assets/architecture.png"
       alt="Octopii Architecture"
       width="70%">
</div>

## Quick Start

Add Octopii to your `Cargo.toml`:

```toml
[dependencies]
octopii = { version = "0.1.0", features = ["openraft"] }
tokio = { version = "1", features = ["full"] }
```

Create a simple replicated key-value store:

```rust
use octopii::{Config, OctopiiNode, OctopiiRuntime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = OctopiiRuntime::new(4);

    let config = Config {
        node_id: 1,
        bind_addr: "127.0.0.1:5001".parse()?,
        peers: vec!["127.0.0.1:5002".parse()?],
        wal_dir: "./data/node1".into(),
        is_initial_leader: true,
        ..Default::default()
    };

    let node = OctopiiNode::new(config, runtime).await?;
    node.start().await?;

    // Write with consensus
    node.propose(b"SET key value".to_vec()).await?;

    // Read from state machine
    let value = node.query(b"GET key").await?;
    println!("Value: {:?}", String::from_utf8(value.to_vec()));

    Ok(())
}
```

## Building Your Distributed System

Octopii makes it easy to replicate custom application logic:

1. **Implement your state machine** - Define how commands are applied to your application state
2. **Configure your cluster** - Specify node IDs, addresses, and persistence settings
3. **Start your nodes** - Octopii handles leader election, replication, and failover
4. **Use the API** - Propose writes, query state, manage membership

```rust
use octopii::StateMachineTrait;
use bytes::Bytes;

struct MyStateMachine {
    // Your application state
}

impl StateMachineTrait for MyStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        // Execute command deterministically
        // This will be replicated across all nodes
    }

    fn snapshot(&self) -> Vec<u8> {
        // Serialize your state for fast catch-up
    }

    fn restore(&self, data: &[u8]) -> Result<(), String> {
        // Restore from snapshot
    }
}
```

## Documentation

Comprehensive guides are available in the `docs/` directory:

- **[API.md](docs/API.md)** - Complete API reference with examples
- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** - System design, data flow, and internals
- **[CUSTOM_STATE_MACHINES.md](docs/CUSTOM_STATE_MACHINES.md)** - Guide to implementing custom replication logic
- **[SHIPPING_LANE.md](docs/SHIPPING_LANE.md)** - P2P file transfer and bulk data streaming

## Examples

Working examples are available in the `examples/` directory:

- `examples/node/` - Three-node cluster setup
- `examples/custom_state_machine/` - Custom state machine implementation
- `examples/shipping_lane/` - Large file transfers
- `examples/rpc/` - Custom RPC messaging
- `examples/wal/` - Direct WAL usage

Run an example:

```bash
cargo run --example node --features openraft
```

## Why Octopii?

Most distributed systems require assembling multiple components: a consensus library, network transport, persistence layer, RPC framework, and data transfer mechanisms. Octopii provides all of these integrated and tested together, so you can focus on your application logic instead of infrastructure plumbing.

## Project Status

Octopii is currently in early development (v0.1.0). The API may change as the project evolves.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
