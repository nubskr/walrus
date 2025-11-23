# Testing Distributed Walrus

Cluster tests run against the three-node Docker Compose setup in this repo. Some tests interfere when run together, so prefer isolated invocations.

## Test commands
- Install deps: `make deps`
- Cluster suite one-by-one (recommended): `make all-tests-one-by-one`
- Cluster suite in one pytest run (may interfere): `make all-tests`
- Individual cluster test: `make <test_name>` (e.g., `make test_env_overrides_apply_to_monitor_rollover_and_retention`)
- Soak tests (short): `make soak-tests`
- Long mixed-load soak (~20m default): `make soak-test-20m` (or `SOAK_LONG_DURATION_SEC=900 make soak-test-long`; tune clients via `SOAK_LONG_CLIENTS`)
- Rust unit tests: `cargo test` (or target by name, e.g., `cargo test partition_ids_gate_leases_and_io`)

## Cluster lifecycle helpers
- Start cluster: `make cluster-up` (or `make cluster-bootstrap` to wait for ports)
- Stop cluster and clear data: `make cluster-down`
- Restart: `make cluster-restart`

All commands assume the `distributed-walrus/` directory as the cwd. Avoid `make all-tests` if you need isolation; use `all-tests-one-by-one` instead.
