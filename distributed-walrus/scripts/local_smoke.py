#!/usr/bin/env python3
"""Simple no-Docker smoke test against locally running Walrus nodes."""

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


def metrics(port: int) -> dict:
    try:
        raw = send_cmd(port, "METRICS")
        payload = raw[3:] if raw.startswith("OK ") else raw
        return json.loads(payload)
    except Exception:
        return {}


def current_leader_port() -> int:
    for p in PORTS.values():
        m = metrics(p)
        lid = m.get("current_leader")
        if lid in PORTS:
            return PORTS[lid]
    return PORTS[1]


def main():
    topic = f"local-demo-{int(time.time())}"
    leader = current_leader_port()
    resp = send_cmd(leader, f"REGISTER {topic}")
    if not resp.startswith("OK"):
        raise SystemExit(f"REGISTER failed: {resp}")

    messages = [f"msg-{i}" for i in range(30)]
    for msg in messages:
        leader = current_leader_port()
        resp = send_cmd(leader, f"PUT {topic} {msg}")
        if not resp.startswith("OK"):
            raise SystemExit(f"PUT failed for {msg}: {resp}")

    reads = []
    for _ in messages:
        r = send_cmd(PORTS[2], f"GET {topic}")
        if r.startswith("OK "):
            reads.append(r[3:])
    if reads != messages:
        raise SystemExit(f"read mismatch: {reads}")

    print("local cluster smoke test passed")


if __name__ == "__main__":
    main()