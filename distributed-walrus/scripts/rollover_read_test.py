#!/usr/bin/env python3
import json
import socket
import struct
import subprocess
import time
from contextlib import closing


def send_cmd(host, port, cmd):
    with closing(socket.create_connection((host, port), timeout=10)) as sock:
        data = cmd.encode()
        sock.sendall(struct.pack("<I", len(data)))
        sock.sendall(data)
        # read len
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


def parse_metrics_response(resp):
    if not resp:
        return None
    raw = resp[3:] if resp.startswith("OK ") else resp
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        print(f"Failed to parse metrics: {resp}")
        return None


def wait_ports(addrs, deadline=90):
    start = time.time()
    while time.time() - start < deadline:
        ok = True
        for host, port in addrs:
            try:
                with socket.create_connection((host, port), timeout=5):
                    pass
            except Exception:
                ok = False
                break
        if ok:
            return
        time.sleep(1)
    raise RuntimeError("Ports did not come up in time")


def wait_for_leader(host, port, node_id, deadline=90):
    start = time.time()
    while time.time() - start < deadline:
        try:
            resp = send_cmd(host, port, "METRICS")
            print(f"DEBUG: METRICS response: {resp}")
            metrics = parse_metrics_response(resp)
            if metrics:
                current_leader = metrics.get("current_leader")
                if current_leader == node_id:
                    print(f"Node {node_id} is leader.")
                    return
                print(f"Node {node_id} not yet leader. Current: {current_leader}")
        except Exception as e:
            print(f"Error checking metrics: {e}")
        time.sleep(1)
    raise RuntimeError(f"Node {node_id} did not become leader in time")


def wait_for_membership(host, port, expected_nodes, deadline=90):
    expected_set = set(expected_nodes)
    start = time.time()
    while time.time() - start < deadline:
        try:
            resp = send_cmd(host, port, "METRICS")
            print(f"DEBUG: METRICS response: {resp}")
            metrics = parse_metrics_response(resp)
            if metrics:
                membership = metrics.get("membership_config", {}).get("membership", {})
                configs = membership.get("configs", []) or []
                nodes = membership.get("nodes", {}) or {}
                config_sets = [set(cfg) for cfg in configs if isinstance(cfg, list)]
                node_set = set(int(k) for k in nodes.keys())
                if expected_set.issubset(node_set) and any(expected_set.issubset(cfg) for cfg in config_sets):
                    print(f"Cluster membership ready: {config_sets} with nodes {node_set}")
                    return
                print(f"Membership not ready yet. Configs: {config_sets}, nodes: {node_set}")
        except Exception as e:
            print(f"Error checking membership: {e}")
        time.sleep(1)
    raise RuntimeError(f"Cluster did not reach membership {expected_set} in time")


def wait_for_replication(host, port, expected_nodes, deadline=90):
    start = time.time()
    expected = {str(n) for n in expected_nodes}
    while time.time() - start < deadline:
        try:
            resp = send_cmd(host, port, "METRICS")
            print(f"DEBUG: METRICS response: {resp}")
            metrics = parse_metrics_response(resp)
            if metrics:
                last_log = metrics.get("last_log_index", 0)
                replication = metrics.get("replication", {}) or {}
                ready = True
                for node in expected:
                    rep = replication.get(node)
                    if not rep or rep.get("index", 0) < last_log:
                        ready = False
                        break
                if ready:
                    print(f"Replication ready at index {last_log} for nodes {expected}")
                    return
                print(f"Replication not ready. last_log={last_log}, replication={replication}")
        except Exception as e:
            print(f"Error checking replication: {e}")
        time.sleep(1)
    raise RuntimeError("Replication did not catch up in time")


def main():
    port_map = {1: 10091, 2: 10092, 3: 10093}

    def current_leader_port():
        for port in port_map.values():
            try:
                metrics = parse_metrics_response(send_cmd("localhost", port, "METRICS"))
                if metrics:
                    leader_id = metrics.get("current_leader")
                    if leader_id and leader_id in port_map:
                        return port_map[leader_id]
            except Exception as e:
                print(f"Error probing leader on port {port}: {e}")
        return port_map[1]

    compose = [
        "docker",
        "compose",
        "-p",
        "walrus-test",
        "-f",
        "docker-compose.yml",
        "-f",
        "docker-compose.test.yml",
    ]

    subprocess.check_call(compose + ["down", "-v"])
    # Also force-remove containers in case of previous failed runs (e.g., if project name changed)
    subprocess.call(["docker", "rm", "-f", "walrus-1", "walrus-2", "walrus-3"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.check_call(compose + ["up", "--build", "-d"])
    try:
        wait_ports([("localhost", 10091), ("localhost", 10092), ("localhost", 10093)], 120)

        wait_for_leader("localhost", 10091, 1) # Wait for node 1 to become leader
        wait_for_membership("localhost", 10091, {1, 2, 3})
        wait_for_replication("localhost", 10091, {1, 2, 3})
        time.sleep(2)  # allow the cluster to settle before issuing writes

        topic = "rollover_demo"
        leader_port = current_leader_port()
        # Register
        resp = send_cmd("localhost", leader_port, f"REGISTER {topic}")
        assert resp.startswith("OK"), f"register failed: {resp}"

        # Write 60 messages, pausing every 10 to let monitor rollover
        messages = [f"msg-{i}" for i in range(60)]
        for idx, msg in enumerate(messages, 1):
            leader_port = current_leader_port()
            for attempt in range(10):
                resp = send_cmd("localhost", leader_port, f"PUT {topic} {msg}")
                if resp.startswith("OK"):
                    break
                print(f"PUT attempt {attempt + 1} for {msg} failed: {resp}")
                leader_port = current_leader_port()
                time.sleep(0.8 * (attempt + 1))
            assert resp.startswith("OK"), f"put failed at {idx}: {resp}"
            time.sleep(0.1)
            if idx % 10 == 0:
                time.sleep(0.6)

        time.sleep(1.5)

        # Check metadata snapshot
        state_raw = send_cmd("localhost", 10091, f"STATE {topic}")
        state = json.loads(state_raw)
        sealed = state.get("sealed_segments", {})
        leaders = state.get("segment_leaders", {})
        assert len(sealed) == 6, f"expected ==6 sealed segments, got {sealed}"
        counts = [int(v) for v in sealed.values()]
        assert all(c == 10 for c in counts[:6]), f"sealed counts too low: {sealed}"
        leader_counts = {}
        for node in leaders.values():
            leader_counts[node] = leader_counts.get(node, 0) + 1
        assert len(leader_counts) == 3, f"leaders not spread across 3 nodes: {leader_counts}"

        # Read all entries from a non-leader port to exercise forwarding
        read_back = []
        for _ in range(len(messages)):
            resp = send_cmd("localhost", 10092, f"GET {topic}")
            if resp.startswith("OK "):
                read_back.append(resp[3:])
            else:
                time.sleep(0.2)
                resp = send_cmd("localhost", 10092, f"GET {topic}")
                if resp.startswith("OK "):
                    read_back.append(resp[3:])
        assert read_back == messages, f"read mismatch: {read_back[:5]} ... vs {messages[:5]}"

        print("rollover/read test passed")
    finally:
        subprocess.check_call(compose + ["down", "-v"])


if __name__ == "__main__":
    main()
