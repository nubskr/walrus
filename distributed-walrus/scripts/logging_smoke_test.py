#!/usr/bin/env python3
import json
import socket
import struct
import subprocess
import sys
import time
from contextlib import closing


def send_cmd(host, port, cmd):
    with closing(socket.create_connection((host, port), timeout=10)) as sock:
        data = cmd.encode()
        sock.sendall(struct.pack("<I", len(data)))
        sock.sendall(data)
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


def wait_for_cluster_ready(port_map, deadline=90):
    start = time.time()
    expected_nodes = set(port_map.keys())
    while time.time() - start < deadline:
        try:
            metrics = parse_metrics_response(send_cmd("localhost", port_map[1], "METRICS"))
            if not metrics:
                time.sleep(1)
                continue
            leader = metrics.get("current_leader")
            membership = metrics.get("membership_config", {}).get("membership", {})
            configs = membership.get("configs", []) or []
            node_set = {int(k) for k in (membership.get("nodes", {}) or {}).keys()}
            if leader in expected_nodes and expected_nodes.issubset(node_set) and any(expected_nodes.issubset(set(cfg)) for cfg in configs if isinstance(cfg, list)):
                print(f"Leader is {leader} and membership looks healthy: {configs}")
                return
            print(f"Waiting for cluster: leader={leader}, configs={configs}, nodes={node_set}")
        except Exception as exc:
            print(f"Error while checking readiness: {exc}")
        time.sleep(1)
    raise RuntimeError("Cluster did not become ready in time")


def current_leader_port(port_map):
    for node_id, port in port_map.items():
        try:
            metrics = parse_metrics_response(send_cmd("localhost", port, "METRICS"))
            if metrics:
                leader_id = metrics.get("current_leader")
                if leader_id and leader_id in port_map:
                    return port_map[leader_id]
        except Exception as exc:
            print(f"Error probing leader on port {port}: {exc}")
    return port_map[1]


def main():
    port_map = {1: 10091, 2: 10092, 3: 10093}
    compose = [
        "docker",
        "compose",
        "-p",
        "walrus-log-test",
        "-f",
        "docker-compose.yml",
        "-f",
        "docker-compose.test.yml",
    ]

    subprocess.check_call(compose + ["down", "-v"])
    subprocess.call(["docker", "rm", "-f", "walrus-1", "walrus-2", "walrus-3"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.check_call(compose + ["up", "--build", "-d"])

    log_proc = subprocess.Popen(compose + ["logs", "-f"], stdout=sys.stdout, stderr=sys.stderr)
    try:
        wait_ports([("localhost", port) for port in port_map.values()], 120)
        wait_for_cluster_ready(port_map)

        topic = f"log-test-{int(time.time())}"
        leader_port = current_leader_port(port_map)
        resp = send_cmd("localhost", leader_port, f"REGISTER {topic}")
        assert resp.startswith("OK"), f"register failed: {resp}"
        print(f"Registered topic {topic} via {leader_port}")

        messages = [f"hello-{i}" for i in range(20)]
        for idx, msg in enumerate(messages, 1):
            leader_port = current_leader_port(port_map)
            for attempt in range(5):
                resp = send_cmd("localhost", leader_port, f"PUT {topic} {msg}")
                if resp.startswith("OK"):
                    print(f"Wrote {msg} on attempt {attempt + 1} via {leader_port}")
                    break
                print(f"PUT attempt {attempt + 1} for {msg} failed: {resp}")
                leader_port = current_leader_port(port_map)
                time.sleep(0.5)
            assert resp.startswith("OK"), f"put failed for {msg}: {resp}"
            time.sleep(0.1)

        print("Finished writes, reading back from follower...")
        read_back = []
        for _ in range(len(messages)):
            resp = send_cmd("localhost", port_map[2], f"GET {topic}")
            if resp.startswith("OK "):
                read_back.append(resp[3:])
            else:
                time.sleep(0.2)
                resp = send_cmd("localhost", port_map[2], f"GET {topic}")
                if resp.startswith("OK "):
                    read_back.append(resp[3:])
        assert read_back == messages, f"read mismatch: {read_back}"
        print("Simple logging smoke test succeeded")
    finally:
        if log_proc:
            log_proc.terminate()
            try:
                log_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log_proc.kill()
        subprocess.check_call(compose + ["down", "-v"])


if __name__ == "__main__":
    main()
