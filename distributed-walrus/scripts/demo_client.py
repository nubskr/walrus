#!/usr/bin/env python3
"""
Simple length-prefixed client to talk to the distributed-walrus TCP facade.
It exercises routing by sending PUTs to different nodes for the same topic.
"""
import argparse
import socket
import struct
from contextlib import closing


def send_command(host: str, port: int, command: str) -> str:
    data = command.encode("utf-8")
    frame = struct.pack("<I", len(data)) + data
    with closing(socket.create_connection((host, port), timeout=5)) as sock:
        sock.sendall(frame)
        # Read response length
        len_buf = recv_exact(sock, 4)
        if not len_buf:
            raise RuntimeError("no response length")
        resp_len = struct.unpack("<I", len_buf)[0]
        resp = recv_exact(sock, resp_len)
        return resp.decode("utf-8")


def recv_exact(sock: socket.socket, n: int) -> bytes:
    chunks = []
    remaining = n
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="demo", help="topic name to use")
    parser.add_argument(
        "--nodes",
        nargs="+",
        default=["127.0.0.1:9091", "127.0.0.1:9092", "127.0.0.1:9093"],
        help="list of host:port client listeners to hit",
    )
    args = parser.parse_args()

    nodes = []
    for entry in args.nodes:
        host, port = entry.split(":")
        nodes.append((host, int(port)))

    # Register on the first node (leader)
    host0, port0 = nodes[0]
    print(f"REGISTER {args.topic} via {host0}:{port0}")
    print(" ->", send_command(host0, port0, f"REGISTER {args.topic}"))

    # Issue PUTs through each node to observe forwarding to the leader.
    for idx, (host, port) in enumerate(nodes, start=1):
        payload = f"msg-from-node{idx}"
        print(f"PUT via {host}:{port}: {payload}")
        print(" ->", send_command(host, port, f"PUT {args.topic} {payload}"))


if __name__ == "__main__":
    main()
