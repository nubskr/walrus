# Distributed Walrus: Usage Guide

This guide covers how to bring up the reference 3‑node cluster, how to talk to it with any Kafka client, and what to expect today.

## Prerequisites
- Docker and Docker Compose
- Python 3 (only if you want to run the sample client)
- `make` for the provided helpers

## Start and stop the demo cluster
```
cd distributed-walrus
make cluster-up        # builds the image and starts node1–node3
make cluster-logs      # follow all node logs
make cluster-down      # stop and remove containers + data volumes
```

Ports exposed on the host:
- Kafka protocol: 9091 (node1), 9092 (node2), 9093 (node3)
- Raft ports stay internal to the Compose network (6001–6003)

Data directories live under `distributed-walrus/test_data/node*/`. `make cluster-down` removes them; omit `-v` in the Makefile command if you want to keep data.

## Talk to the cluster with a Kafka client
Distributed Walrus speaks the Kafka protocol, so you can reuse existing Kafka libraries. An end‑to‑end example using `kafka-python` is in `distributed-walrus/examples/python_kafka_client.py`.

Run it after the cluster is up. Prefer `uv` (same as the test Makefile) and fall back to `python -m venv`:
```
cd distributed-walrus
if command -v uv >/dev/null 2>&1; then
  uv venv --if-exists reuse .venv
  uv pip install --python .venv/bin/python -r examples/requirements.txt
else
  python3 -m venv .venv && . .venv/bin/activate
  pip install -r examples/requirements.txt
fi
. .venv/bin/activate
python examples/python_kafka_client.py \
  --bootstrap localhost:9091,localhost:9092,localhost:9093 \
  --topic example-logs \
  --count 10
```

What the sample does:
- Creates the topic (3 partitions, replication factor 3) if it does not already exist
- Produces a burst of messages with `acks=all`
- Consumes from the beginning and verifies it can read back the writes

Any Kafka client (Java, Go, Rust, Node, etc.) can be pointed at the same bootstrap list.

## Feature highlights
- Kafka protocol endpoints implemented and exercised by the integration suite: ApiVersions, Metadata, CreateTopics, Produce, Fetch
- Raft‑backed replication with leadership election and follower catch‑up
- Log retention with generation rollover and pruning (see `docker-compose.yml` for demo settings)
- Cluster membership changes (hot‑joining a node and removing a node) covered by tests

## Current limitations and gotchas
- No authentication, TLS, or ACLs
- Consumer groups, transactions, idempotent producers, and Kafka Streams APIs are not implemented
- Batch append endpoint expects serialized batches; regular appends can be parallelized across topics/partitions
- Demo Compose file uses small segment sizes and retention for faster tests; adjust for production‑like workloads
- Node hostnames are `node1`, `node2`, `node3` inside the Compose network; use `localhost` ports from your host

## Testing and soak workloads
- Run the full cluster test suite: `make all-tests`
- Run short soak scenarios: `make soak-tests`
- Run the long‑running mixed‑load soak (configurable via `SOAK_LONG_DURATION_SEC`): `make soak-test-long`

These tests exercise failure cases, leadership changes, GC, membership changes, follower reads, and both batch and non‑batch append paths in a 3‑node cluster.
