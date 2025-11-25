#!/usr/bin/env python3
"""
Local rollover/read smoke test against an already running 3-node cluster (no Docker).

Expected client ports:
  node1: 10081, node2: 10082, node3: 10083
"""

import json
import socket
import struct
import time
from contextlib import closing

PORTS = {1: 10081, 2: 10082, 3: 10083}


def send_cmd(port: int, cmd: str) -> str:
    data = cmd.encode()
    with closing(socket.create_connection(("127.0.0.1", port), timeout=5)) as sock:
        sock.sendall(struct.pack("<I", len(data)) + data)
        raw_len = sock.recv(4)
        if len(raw_len) < 4:
            raise RuntimeError("short read on len")
        msg_len = struct.unpack("<I", raw_len)[0]
        chunks = []
        remaining = msg_len
        while remaining > 0:
            chunk = sock.recv(remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks).decode()


def parse_metrics(port: int) -> dict:
    try:
        raw = send_cmd(port, "METRICS")
        payload = raw[3:] if raw.startswith("OK ") else raw
        return json.loads(payload)
    except Exception:
        return {}


def _can_connect(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except Exception:
        return False


def wait_ports(deadline: float = 60.0) -> None:
    start = time.time()
    while time.time() - start < deadline:
        if all(_can_connect("127.0.0.1", p) for p in PORTS.values()):
            return
        time.sleep(0.5)
    raise SystemExit("client ports did not open in time")


def wait_leader(deadline: float = 60.0) -> int:
    start = time.time()
    while time.time() - start < deadline:
        for p in PORTS.values():
            m = parse_metrics(p)
            lid = m.get("current_leader")
            if lid in PORTS:
                return PORTS[lid]
        time.sleep(0.5)
    raise SystemExit("no leader elected in time")


def wait_membership(expected: set[int], deadline: float = 90.0) -> None:
    start = time.time()
    while time.time() - start < deadline:
        m = parse_metrics(PORTS[1])
        membership = m.get("membership_config", {}).get("membership", {})
        configs = membership.get("configs", []) or []
        node_set = {int(k) for k in (membership.get("nodes", {}) or {}).keys()}
        if expected.issubset(node_set) and any(
            expected.issubset(set(cfg)) for cfg in configs if isinstance(cfg, list)
        ):
            return
        time.sleep(0.5)
    raise SystemExit(f"cluster membership incomplete: configs={configs} nodes={node_set}")


def wait_replication(expected_nodes: set[int], deadline: float = 90.0) -> None:
    start = time.time()
    expected = {str(n) for n in expected_nodes}
    while time.time() - start < deadline:
        m = parse_metrics(PORTS[1])
        last_log = m.get("last_log_index", 0)
        repl = m.get("replication", {}) or {}
        if last_log == 0:
            time.sleep(0.5)
            continue
        ready = True
        for n in expected:
            r = repl.get(n)
            if not r or r.get("index", 0) < last_log:
                ready = False
                break
        if ready:
            return
        time.sleep(0.5)
    raise SystemExit(f"replication did not catch up, last_log={last_log} repl={repl}")


def current_leader_port() -> int:
    for p in PORTS.values():
        m = parse_metrics(p)
        lid = m.get("current_leader")
        if lid in PORTS:
            return PORTS[lid]
    return PORTS[1]


def main():
    wait_ports()
    wait_leader()
    wait_membership(set(PORTS.keys()))
    wait_replication(set(PORTS.keys()))

    topic = f"rollover_local_{int(time.time())}"
    leader_port = current_leader_port()
    resp = send_cmd(leader_port, f"REGISTER {topic}")
    if not resp.startswith("OK"):
        raise SystemExit(f"register failed: {resp}")

    messages = [f"msg-{i}" for i in range(60)]
    for idx, msg in enumerate(messages, 1):
        leader_port = current_leader_port()
        for attempt in range(8):
            resp = send_cmd(leader_port, f"PUT {topic} {msg}")
            if resp.startswith("OK"):
                break
            time.sleep(0.5 * (attempt + 1))
            leader_port = current_leader_port()
        if not resp.startswith("OK"):
            raise SystemExit(f"put failed at {idx}: {resp}")
        time.sleep(0.05)
        if idx % 10 == 0:
            time.sleep(0.6)

    time.sleep(1.5)
    state_raw = send_cmd(PORTS[1], f"STATE {topic}")
    state = json.loads(state_raw)
    sealed = state.get("sealed_segments", {})
    leaders = state.get("segment_leaders", {})
    if len(sealed) < 6:
        raise SystemExit(f"expected >=6 sealed segments, got {sealed}")
    if not all(int(v) >= 10 for v in list(sealed.values())[:6]):
        raise SystemExit(f"sealed counts too low: {sealed}")
    leader_counts = {}
    for node in leaders.values():
        leader_counts[node] = leader_counts.get(node, 0) + 1
    if len(leader_counts) < 3:
        raise SystemExit(f"leaders not spread across 3 nodes: {leader_counts}")

    read_back = []
    for _ in messages:
        resp = send_cmd(PORTS[2], f"GET {topic}")
        if resp.startswith("OK "):
            read_back.append(resp[3:])
        else:
            time.sleep(0.2)
            resp = send_cmd(PORTS[2], f"GET {topic}")
            if resp.startswith("OK "):
                read_back.append(resp[3:])
    if read_back != messages:
        raise SystemExit(f"read mismatch: {read_back[:5]} ... vs {messages[:5]}")

    print("local rollover/read test passed")


if __name__ == "__main__":
    main()
