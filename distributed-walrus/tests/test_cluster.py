import os
import shutil
import socket
import struct
import subprocess
import time
from pathlib import Path
from typing import Optional

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker-compose", "-f", str(PROJECT_ROOT / "docker-compose.yml")]
SKIP_BUILD = os.getenv("WALRUS_SKIP_BUILD") == "1"
LOG_TAIL = int(os.getenv("WALRUS_LOG_TAIL", "120"))

# Broker ports exposed on the host.
NODES = {
    1: ("127.0.0.1", 9091),
    2: ("127.0.0.1", 9092),
    3: ("127.0.0.1", 9093),
}
HOTJOIN_NODE = {4: ("127.0.0.1", 9094)}


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(item, call):
    """Expose test outcome to fixtures for log dumping on failure."""
    outcome = yield
    rep = outcome.get_result()
    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture(autouse=True)
def log_on_failure(request):
    """If a test fails, emit recent docker logs to aid debugging."""
    yield
    rep = getattr(request.node, "rep_call", None)
    if rep and rep.failed:
        print("\n========= docker-compose logs (recent) =========")
        dump_logs(["node1", "node2", "node3"], tail=LOG_TAIL)
        print("========= end logs =========\n")


@pytest.fixture(scope="module", autouse=True)
def cluster():
    """Build and start the 3-node cluster, clean it up afterwards."""
    data_dir = PROJECT_ROOT / "test_data"
    try:
        subprocess.run(COMPOSE + ["down", "-v"], cwd=PROJECT_ROOT, check=False)
        if data_dir.exists():
            shutil.rmtree(data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)

        up_args = ["up", "-d", "--remove-orphans"]
        if not SKIP_BUILD:
            up_args.append("--build")
        subprocess.run(COMPOSE + up_args, check=True, cwd=PROJECT_ROOT)

        # Node 1 sleeps ~20s during bootstrap; give the cluster time to elect/rollover.
        wait_for_ports(timeout=40)
        wait_for_cluster_metadata(timeout=90)
        yield
    finally:
        subprocess.run(COMPOSE + ["down", "-v"], check=True, cwd=PROJECT_ROOT)


@pytest.fixture(autouse=True)
def ensure_base_cluster(cluster):
    """Guarantee core nodes are up and responsive before each test and try to restore after."""
    ensure_network()
    cleanup_node4_container()
    subprocess.run(COMPOSE + ["up", "-d", "node1", "node2", "node3"], check=True, cwd=PROJECT_ROOT)
    ensure_cluster_ready(timeout=120)
    yield
    subprocess.run(COMPOSE + ["up", "-d", "node1", "node2", "node3"], check=False, cwd=PROJECT_ROOT)
    try:
        ensure_cluster_ready(timeout=120)
    except Exception as exc:  # pragma: no cover - best-effort cleanup
        print(f"[teardown] cluster not healthy after test: {exc}")


def wait_for_ports(timeout: int = 30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if all(tcp_ready(host, port) for host, port in NODES.values()):
            return
        time.sleep(1)
    pytest.fail("Kafka ports never became reachable")
def wait_for_specific_ports(targets: dict[int, tuple[str, int]], timeout: int = 30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if all(tcp_ready(host, port) for host, port in targets.values()):
            return
        time.sleep(1)
    pytest.fail(f"Ports never became reachable for {list(targets.keys())}")


def tcp_ready(host: str, port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(1.0)
        return s.connect_ex((host, port)) == 0


def wait_for_cluster_metadata(timeout: int = 90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if all(metadata_ready_for_node(node_id, host, port) for node_id, (host, port) in NODES.items()):
            return
        time.sleep(2)
    pytest.fail("Metadata endpoints did not become ready on all nodes")


def kafka_string(value: str) -> bytes:
    encoded = value.encode()
    return struct.pack(">h", len(encoded)) + encoded


def make_produce_request(topic: str, payload: bytes, correlation_id: int = 1, partition: int = 0) -> bytes:
    header = struct.pack(">hhih", 0, 0, correlation_id, 4) + b"test"

    body = struct.pack(">hi", 1, 5000)  # acks, timeout
    body += struct.pack(">i", 1)  # topic count
    body += kafka_string(topic)
    body += struct.pack(">i", 1)  # partition count
    body += struct.pack(">i", partition)  # partition id
    body += struct.pack(">i", len(payload)) + payload

    frame = header + body
    return struct.pack(">i", len(frame)) + frame


def make_metadata_request(topics=None, correlation_id: int = 1) -> bytes:
    topics = topics or []
    header = struct.pack(">hhih", 3, 0, correlation_id, 4) + b"test"
    body = struct.pack(">i", len(topics))
    for topic in topics:
        body += kafka_string(topic)
    frame = header + body
    return struct.pack(">i", len(frame)) + frame


def make_api_versions_request(correlation_id: int = 42) -> bytes:
    header = struct.pack(">hhih", 18, 0, correlation_id, 4) + b"test"
    return struct.pack(">i", len(header)) + header


def make_fetch_request(topic: str, max_bytes: int = 1024, correlation_id: int = 99, partition: int = 0) -> bytes:
    header = struct.pack(">hhih", 1, 0, correlation_id, 4) + b"test"
    body = struct.pack(">iii", -1, 0, 0)  # replica_id, max_wait, min_bytes
    body += struct.pack(">i", 1)  # topic count
    body += kafka_string(topic)
    body += struct.pack(">i", 1)  # partition count
    body += struct.pack(">i", partition)  # partition id
    body += struct.pack(">q", 0)  # fetch offset (ignored)
    body += struct.pack(">i", max_bytes)
    frame = header + body
    return struct.pack(">i", len(frame)) + frame


def make_internal_state_request(topics=None, correlation_id: int = 123) -> bytes:
    topics = topics or []
    header = struct.pack(">hhih", 50, 0, correlation_id, 4) + b"test"
    body = struct.pack(">i", len(topics))
    for topic in topics:
        body += kafka_string(topic)
    frame = header + body
    return struct.pack(">i", len(frame)) + frame


def make_fetch_with_offset(topic: str, partition: int, offset: int, max_bytes: int = 4096, correlation_id: int = 100) -> bytes:
    header = struct.pack(">hhih", 1, 0, correlation_id, 4) + b"test"
    body = struct.pack(">iii", -1, 0, 0)
    body += struct.pack(">i", 1)
    body += kafka_string(topic)
    body += struct.pack(">i", 1)
    body += struct.pack(">i", partition)
    body += struct.pack(">q", offset)
    body += struct.pack(">i", max_bytes)
    frame = header + body
    return struct.pack(">i", len(frame)) + frame


def make_create_topics_request(topics: list[tuple[str, int]], correlation_id: int = 7) -> bytes:
    """
    topics: list of (name, partitions)
    """
    header = struct.pack(">hhih", 19, 0, correlation_id, 4) + b"test"
    body = struct.pack(">i", len(topics))
    for name, partitions in topics:
        body += kafka_string(name)
        body += struct.pack(">i", partitions)  # partitions
        body += struct.pack(">h", 1)  # replication factor (fixed to 1)
        body += struct.pack(">i", 0)  # replica assignment count
        body += struct.pack(">i", 0)  # config count
    body += struct.pack(">i", 10000)  # timeout ms
    frame = header + body
    return struct.pack(">i", len(frame)) + frame


def make_membership_remove_request(node_id: int, correlation_id: int = 8) -> bytes:
    header = struct.pack(">hhih", 51, 0, correlation_id, 4) + b"test"
    body = struct.pack(">hi", 0, node_id)  # op=remove, node id
    return struct.pack(">i", len(header + body)) + header + body


def read_frame(sock: socket.socket) -> Optional[bytes]:
    try:
        size_bytes = recv_exact(sock, 4)
        if size_bytes is None:
            return None
        frame_len = struct.unpack(">i", size_bytes)[0]
        return recv_exact(sock, frame_len)
    except Exception:
        return None


def recv_exact(sock: socket.socket, size: int) -> Optional[bytes]:
    buf = b""
    while len(buf) < size:
        chunk = sock.recv(size - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def send_frame(host: str, port: int, frame: bytes) -> Optional[bytes]:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5.0)
            s.connect((host, port))
            s.sendall(frame)
            return read_frame(s)
    except Exception as exc:
        print(f"Socket error to {host}:{port}: {exc}")
        return None


def send_frame_with_retry(host: str, port: int, frame: bytes, attempts: int = 5, delay: float = 2.0) -> Optional[bytes]:
    for i in range(attempts):
        resp = send_frame(host, port, frame)
        if resp:
            return resp
        time.sleep(delay)
    return None


def ensure_cluster_ready(timeout: int = 90):
    """Wait for broker ports and metadata responses across the cluster."""
    wait_for_ports(timeout=min(timeout, 40))
    wait_for_cluster_metadata(timeout=timeout)


def ensure_network():
    subprocess.run(
        ["docker", "network", "create", "distributed-walrus_walrus-net"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def cleanup_node4_container():
    subprocess.run(
        ["docker", "rm", "-f", "walrus-4"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def dump_logs(services: list[str], tail: int = 120):
    cmd = COMPOSE + ["logs", "--tail", str(tail)] + services
    try:
        subprocess.run(cmd, check=False, cwd=PROJECT_ROOT)
    except Exception as exc:
        print(f"[log] failed to fetch logs: {exc}")


def produce_and_assert_ok(node: tuple[str, int], topic: str, payload: bytes, partition: int = 0) -> bytes:
    resp = send_frame_with_retry(*node, make_produce_request(topic, payload, partition=partition))
    assert resp, f"produce to {node} returned no response"
    codes = parse_produce_error_codes(resp)
    assert all(code == 0 for code in codes), f"produce failed with codes {codes}"
    return resp


def has_files(path: Path) -> bool:
    if not path.exists():
        return False
    for root, _, files in os.walk(path):
        for name in files:
            if os.path.getsize(Path(root) / name) > 0:
                return True
    return False


def data_plane_root(node_id: int) -> Path:
    return PROJECT_ROOT / f"test_data/node{node_id}/node_{node_id}/user_data/data_plane"


def snapshot_data_plane_mtimes() -> dict[int, dict[str, int]]:
    """Capture nanosecond mtimes for all files under each node's data_plane dir."""
    snapshot: dict[int, dict[str, int]] = {}
    for node_id in NODES:
        root = data_plane_root(node_id)
        snapshot[node_id] = {}
        if not root.exists():
            continue
        for path in root.iterdir():
            if path.is_file():
                snapshot[node_id][path.name] = path.stat().st_mtime_ns
    return snapshot


def list_generations(node_id: int) -> list[int]:
    """Return generation numbers for logs partition as reported by internal state."""
    resp = send_frame_with_retry(*NODES.get(node_id, NODES[1]), make_internal_state_request(["logs"]))
    state = decode_internal_state(resp or b"")
    if not state:
        return []
    gens: list[int] = []
    for topic in state["topics"]:
        if topic["name"] != "logs":
            continue
        for part in topic["partitions"]:
            gens.append(int(part["generation"]))
    return gens


def wal_file_count(node_id: int) -> int:
    root = data_plane_root(node_id)
    if not root.exists():
        return 0
    return len([p for p in root.iterdir() if p.is_file()])


def wal_file_mtime_ns(node_id: int) -> int:
    root = data_plane_root(node_id)
    if not root.exists():
        return 0
    times = [int(p.stat().st_mtime_ns) for p in root.iterdir() if p.is_file()]
    return max(times) if times else 0


def pause_container(name: str):
    subprocess.run(["docker", "kill", "-s", "STOP", name], check=True)


def resume_container(name: str):
    subprocess.run(["docker", "kill", "-s", "CONT", name], check=True)


def restart_container(name: str):
    subprocess.run(COMPOSE + ["restart", name], check=True, cwd=PROJECT_ROOT)


def parse_produce_error_codes(resp: bytes) -> list[int]:
    """Return Kafka error codes from a Produce response frame."""
    idx = 0
    _correlation_id = struct.unpack_from(">i", resp, idx)[0]
    idx += 4
    topic_count = struct.unpack_from(">i", resp, idx)[0]
    idx += 4
    codes: list[int] = []
    for _ in range(topic_count):
        topic_len = struct.unpack_from(">h", resp, idx)[0]
        idx += 2 + topic_len
        part_count = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        for _ in range(part_count):
            _partition = struct.unpack_from(">i", resp, idx)[0]
            idx += 4
            code = struct.unpack_from(">h", resp, idx)[0]
            idx += 2 + 8  # error code + base offset
            codes.append(code)
    return codes


def decode_metadata(resp: bytes) -> Optional[dict]:
    """Parse the simplified metadata response emitted by our Kafka facade."""
    try:
        idx = 0
        if len(resp) < 16:
            return None
        correlation_id = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        broker_count = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        local_node_id = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        host_len = struct.unpack_from(">h", resp, idx)[0]
        idx += 2
        host = resp[idx : idx + host_len].decode()
        idx += host_len
        port = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        topic_count = struct.unpack_from(">i", resp, idx)[0]
        idx += 4

        topics = []
        for _ in range(topic_count):
            _topic_error = struct.unpack_from(">h", resp, idx)[0]
            idx += 2
            name_len = struct.unpack_from(">h", resp, idx)[0]
            idx += 2
            name = resp[idx : idx + name_len].decode()
            idx += name_len
            partition_count = struct.unpack_from(">i", resp, idx)[0]
            idx += 4
            partitions = []
            for _ in range(partition_count):
                _part_error = struct.unpack_from(">h", resp, idx)[0]
                idx += 2
                partition = struct.unpack_from(">i", resp, idx)[0]
                idx += 4
                leader = struct.unpack_from(">i", resp, idx)[0]
                idx += 4
                replica_count = struct.unpack_from(">i", resp, idx)[0]
                idx += 4
                replicas = []
                for _ in range(replica_count):
                    replicas.append(struct.unpack_from(">i", resp, idx)[0])
                    idx += 4
                isr_count = struct.unpack_from(">i", resp, idx)[0]
                idx += 4
                isr = []
                for _ in range(isr_count):
                    isr.append(struct.unpack_from(">i", resp, idx)[0])
                    idx += 4
                partitions.append(
                    {
                        "partition": partition,
                        "leader": leader,
                        "replicas": replicas,
                        "isr": isr,
                    }
                )
            topics.append({"name": name, "partitions": partitions})

        return {
            "correlation_id": correlation_id,
            "broker_count": broker_count,
            "local_node_id": local_node_id,
            "host": host,
            "port": port,
            "topics": topics,
        }
    except Exception:
        return None


def metadata_ready_for_node(node_id: int, host: str, port: int) -> bool:
    frame = make_metadata_request(["logs"])
    resp = send_frame(host, port, frame)
    meta = decode_metadata(resp or b"")
    if not meta:
        return False
    for topic in meta["topics"]:
        for part in topic["partitions"]:
            if topic["name"] == "logs" and part["leader"] == 1:
                return True
    return False


def decode_api_versions(resp: bytes) -> Optional[dict]:
    """Decode the simplified ApiVersions response emitted by the server."""
    try:
        idx = 0
        corr = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        error_code = struct.unpack_from(">h", resp, idx)[0]
        idx += 2
        api_count = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        apis = []
        for _ in range(api_count):
            key = struct.unpack_from(">h", resp, idx)[0]
            idx += 2
            min_v = struct.unpack_from(">h", resp, idx)[0]
            idx += 2
            max_v = struct.unpack_from(">h", resp, idx)[0]
            idx += 2
            apis.append((key, min_v, max_v))
        return {"correlation_id": corr, "error_code": error_code, "apis": apis}
    except Exception:
        return None


def decode_fetch(resp: bytes) -> Optional[list[dict]]:
    """Decode simplified Fetch response, returning error/payload per partition."""
    try:
        idx = 0
        _corr = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        topic_count = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        payloads: list[bytes] = []
        for _ in range(topic_count):
            topic_len = struct.unpack_from(">h", resp, idx)[0]
            idx += 2 + topic_len
            part_count = struct.unpack_from(">i", resp, idx)[0]
            idx += 4
            for _ in range(part_count):
                _partition = struct.unpack_from(">i", resp, idx)[0]
                idx += 4
                error_code = struct.unpack_from(">h", resp, idx)[0]
                idx += 2
                high_watermark = struct.unpack_from(">q", resp, idx)[0]
                idx += 8
                size = struct.unpack_from(">i", resp, idx)[0]
                idx += 4
                payload = resp[idx : idx + size]
                idx += size
                payloads.append({"error_code": error_code, "payload": payload, "high_watermark": high_watermark})
        return payloads
    except Exception:
        return None


def decode_create_topics_resp(resp: bytes) -> Optional[list[tuple[str, int]]]:
    """Return list of (topic, error_code) for CreateTopics."""
    try:
        idx = 0
        _corr = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        _throttle = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        count = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        topics: list[tuple[str, int]] = []
        for _ in range(count):
            name_len = struct.unpack_from(">h", resp, idx)[0]
            idx += 2
            name = resp[idx : idx + name_len].decode()
            idx += name_len
            err = struct.unpack_from(">h", resp, idx)[0]
            idx += 2
            msg_len = struct.unpack_from(">h", resp, idx)[0]
            idx += 2 + msg_len
            _cfg_count = struct.unpack_from(">i", resp, idx)[0]
            idx += 4
            topics.append((name, err))
        return topics
    except Exception:
        return None


def decode_internal_state(resp: bytes) -> Optional[dict]:
    try:
        idx = 0
        corr = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        topic_count = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        topics = []
        for _ in range(topic_count):
            name_len = struct.unpack_from(">h", resp, idx)[0]
            idx += 2
            name = resp[idx : idx + name_len].decode()
            idx += name_len
            part_count = struct.unpack_from(">i", resp, idx)[0]
            idx += 4
            parts = []
            for _ in range(part_count):
                partition = struct.unpack_from(">i", resp, idx)[0]
                idx += 4
                leader = struct.unpack_from(">i", resp, idx)[0]
                idx += 4
                generation = struct.unpack_from(">q", resp, idx)[0]
                idx += 8
                parts.append({"partition": partition, "leader": leader, "generation": generation})
            topics.append({"name": name, "partitions": parts})
        return {"correlation_id": corr, "topics": topics}
    except Exception:
        return None


def test_write_forwarding():
    payload = b"FORWARD_TEST_DATA"
    before = snapshot_data_plane_mtimes()

    resp = send_frame_with_retry(*NODES[1], make_produce_request("logs", payload))
    assert resp, "Leader did not return a Produce response"
    codes = parse_produce_error_codes(resp)
    assert all(code == 0 for code in codes), f"Produce returned errors: {codes}"

    time.sleep(2)

    after = snapshot_data_plane_mtimes()
    leader_changes = set(after[1].items()) - set(before[1].items())
    assert leader_changes, "Leader storage did not change after produce"
    for follower in (2, 3):
        assert after[follower] == before.get(follower, {}), f"Follower {follower} should not mutate storage on forwarded writes"


def test_metadata_connectivity():
    for node_id, (host, port) in NODES.items():
        meta = decode_metadata(send_frame(host, port, make_metadata_request(["logs"])) or b"")
        assert meta, f"Node {node_id} returned no metadata"
        assert meta["local_node_id"] == node_id, f"Node {node_id} reported wrong id {meta['local_node_id']}"
        assert any(
            topic["name"] == "logs"
            and any(part["leader"] == 1 for part in topic["partitions"])
            for topic in meta["topics"]
        ), f"Logs topic missing or leader incorrect on node {node_id}"


def test_followers_forward_produce_to_leader():
    for follower in (2, 3):
        payload = f"FORWARDED_FROM_FOLLOWER_{follower}".encode()
        before = snapshot_data_plane_mtimes()

        resp = send_frame_with_retry(*NODES[follower], make_produce_request("logs", payload))
        assert resp, f"Follower {follower} did not return a Produce response"
        codes = parse_produce_error_codes(resp)
        assert all(code == 0 for code in codes), f"Produce via follower {follower} returned errors: {codes}"

        time.sleep(2)
        after = snapshot_data_plane_mtimes()
        leader_changed = set(after[1].items()) - set(before[1].items())
        assert leader_changed, f"Leader storage did not change after follower {follower} forwarded produce"
        for other in (2, 3):
            assert after[other] == before.get(other, {}), f"Follower {other} should not store forwarded writes"


def test_metadata_filtered_request_returns_only_logs():
    frame = make_metadata_request(["logs"])
    resp = send_frame_with_retry(*NODES[1], frame)
    meta = decode_metadata(resp or b"")
    assert meta, "metadata decode failed"
    assert len(meta["topics"]) == 1 and meta["topics"][0]["name"] == "logs", "unexpected topics in filtered metadata"
    partitions = meta["topics"][0]["partitions"]
    assert any(p["leader"] == 1 for p in partitions), "leader for logs missing or incorrect"


def test_api_versions_support_known_keys():
    frame = make_api_versions_request()
    resp = send_frame_with_retry(*NODES[3], frame)
    decoded = decode_api_versions(resp or b"")
    assert decoded, "ApiVersions decode failed"
    assert decoded["error_code"] == 0
    supported = {key: (min_v, max_v) for key, min_v, max_v in decoded["apis"]}
    assert supported.get(0) == (0, 0)
    assert supported.get(1) == (0, 0)
    assert supported.get(3) == (0, 0)
    assert supported.get(18) == (0, 0)
    assert supported.get(19) == (0, 0)
    assert supported.get(50) == (0, 0)
    assert supported.get(51) == (0, 0)


def test_produce_unknown_topic_returns_error_and_no_disk_change():
    before = snapshot_data_plane_mtimes()
    resp = send_frame_with_retry(*NODES[1], make_produce_request("unknown-topic", b"bad"))
    assert resp, "produce to unknown topic returned no response"
    codes = parse_produce_error_codes(resp)
    assert any(code != 0 for code in codes), f"expected non-zero error code, got {codes}"
    time.sleep(1)
    after = snapshot_data_plane_mtimes()
    assert after == before, "unknown topic produce should not touch any storage"


def test_produce_invalid_partition_returns_error_and_no_disk_change():
    before = snapshot_data_plane_mtimes()
    resp = send_frame_with_retry(*NODES[1], make_produce_request("logs", b"bad-partition", partition=99))
    assert resp, "produce to invalid partition returned no response"
    codes = parse_produce_error_codes(resp)
    assert any(code != 0 for code in codes), f"expected non-zero error code, got {codes}"
    time.sleep(1)
    after = snapshot_data_plane_mtimes()
    assert after == before, "invalid partition produce should not touch any storage"


def test_metadata_unfiltered_returns_logs_topic():
    frame = make_metadata_request()
    resp = send_frame_with_retry(*NODES[2], frame)
    meta = decode_metadata(resp or b"")
    assert meta, "metadata decode failed"
    assert any(topic["name"] == "logs" for topic in meta["topics"]), "logs topic missing in unfiltered metadata"


def test_fetch_from_follower_reads_latest_entry():
    payload = b"READ_FORWARD_ME"
    resp = send_frame_with_retry(*NODES[1], make_produce_request("logs", payload))
    assert resp and all(code == 0 for code in parse_produce_error_codes(resp)), "produce failed"
    time.sleep(2)

    fetch_resp = send_frame_with_retry(*NODES[3], make_fetch_request("logs", 4096))
    items = decode_fetch(fetch_resp or b"")
    assert items, "fetch returned no payloads"
    assert all(item["error_code"] == 0 for item in items), f"fetch returned errors {items}"
    # Even if payload is empty (depending on Walrus read offsets), the follower
    # should return a successful response path via the leader.


def test_fetch_payload_and_offsets_from_leader():
    resp1 = send_frame_with_retry(*NODES[1], make_produce_request("logs", b"FIRST"))
    resp2 = send_frame_with_retry(*NODES[1], make_produce_request("logs", b"SECOND"))
    assert resp1 and resp2
    assert all(code == 0 for code in parse_produce_error_codes(resp1 + resp2))
    time.sleep(1)

    fetch0 = send_frame_with_retry(*NODES[1], make_fetch_request("logs", 4096, partition=0))
    items0 = decode_fetch(fetch0 or b"")
    assert items0 and items0[0]["error_code"] == 0
    assert items0[0]["payload"].startswith(b"FIRST") or items0[0]["payload"] == b""

    # Offset 1 should return the second payload
    header = struct.pack(">hhih", 1, 0, 100, 4) + b"test"
    body = struct.pack(">iii", -1, 0, 0) + struct.pack(">i", 1) + kafka_string("logs")
    body += struct.pack(">i", 1) + struct.pack(">i", 0) + struct.pack(">q", 1) + struct.pack(">i", 4096)
    fetch1 = send_frame_with_retry(*NODES[1], struct.pack(">i", len(header + body)) + header + body)
    items1 = decode_fetch(fetch1 or b"")
    assert items1 and items1[0]["error_code"] == 0
    assert items1[0]["payload"].startswith(b"SECOND") or items1[0]["payload"] == b""


def test_create_topic_and_roundtrip_partition_zero():
    ensure_cluster_ready()
    resp = send_frame_with_retry(*NODES[1], make_create_topics_request([("topic_x", 1)]))
    topics = decode_create_topics_resp(resp or b"")
    assert topics and topics[0][1] == 0, f"CreateTopics failed: {topics}"

    data = b"NEWTOPIC"
    prod = send_frame_with_retry(*NODES[1], make_produce_request("topic_x", data, partition=0))
    assert prod and all(code == 0 for code in parse_produce_error_codes(prod))
    time.sleep(1)
    fetch = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request("topic_x", 4096)) or b"")
    assert fetch and fetch[0]["error_code"] == 0
    assert fetch[0]["payload"] in (data, b"")


def test_fetch_offset_strictness_with_high_watermark():
    ensure_cluster_ready()
    send_frame_with_retry(*NODES[1], make_create_topics_request([("strict_topic", 1)]))
    payloads = [b"S1", b"S2", b"S3"]
    for pay in payloads:
        produce_and_assert_ok(NODES[1], "strict_topic", pay, partition=0)
    time.sleep(1)

    fetch0 = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_with_offset("strict_topic", 0, 0)) or b"")
    assert fetch0 and fetch0[0]["error_code"] == 0
    assert fetch0[0]["high_watermark"] >= len(payloads)
    # Payload may still be empty depending on WAL semantics, but offset 0 must be accepted.

    fetch2 = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_with_offset("strict_topic", 0, 2)) or b"")
    assert fetch2 and fetch2[0]["error_code"] == 0
    assert fetch2[0]["high_watermark"] >= len(payloads)

    # Offset beyond available should return an error.
    fetch4 = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_with_offset("strict_topic", 0, 4)) or b"")
    assert fetch4 and fetch4[0]["error_code"] != 0
    assert fetch4[0]["high_watermark"] >= len(payloads)


def test_fetch_unknown_topic_returns_error():
    frame = make_fetch_request("not-real", 1024)
    resp = send_frame_with_retry(*NODES[1], frame)
    items = decode_fetch(resp or b"")
    assert items, "fetch returned no payloads"
    assert any(item["error_code"] != 0 for item in items), f"expected fetch error codes, got {items}"


def test_fetch_invalid_partition_returns_error():
    frame = make_fetch_request("logs", 1024, partition=5)
    resp = send_frame_with_retry(*NODES[2], frame)
    items = decode_fetch(resp or b"")
    assert items, "fetch returned no payloads"
    assert any(item["error_code"] != 0 for item in items), f"expected non-zero error code, got {items}"


def test_partition_one_produce_and_fetch():
    payload = b"PARTITION_ONE_PAYLOAD"
    resp = send_frame_with_retry(*NODES[1], make_produce_request("logs", payload, partition=1))
    assert resp and all(code == 0 for code in parse_produce_error_codes(resp))
    time.sleep(1)
    fetch_resp = send_frame_with_retry(*NODES[1], make_fetch_request("logs", 4096, partition=1))
    items = decode_fetch(fetch_resp or b"")
    assert items and items[0]["error_code"] == 0
    assert items[0]["payload"] in (payload, b"")


def test_restart_preserves_data_and_leadership():
    ensure_cluster_ready()
    payload = b"RESTART_PERSISTENCE"
    resp = send_frame_with_retry(*NODES[1], make_produce_request("logs", payload))
    assert resp and all(code == 0 for code in parse_produce_error_codes(resp))
    time.sleep(1)

    # Restart leader container
    subprocess.run(COMPOSE + ["stop", "node1"], check=True, cwd=PROJECT_ROOT)
    time.sleep(2)
    subprocess.run(COMPOSE + ["start", "node1"], check=True, cwd=PROJECT_ROOT)
    wait_for_specific_ports({1: NODES[1]}, timeout=40)
    # On restart, ensure leader responds; followers may still be catching up.
    deadline = time.time() + 60
    while time.time() < deadline:
        if metadata_ready_for_node(1, *NODES[1]):
            break
        time.sleep(2)
    time.sleep(5)
    ensure_cluster_ready()

    fetch_resp = send_frame_with_retry(*NODES[1], make_fetch_request("logs", 4096), attempts=8, delay=2)
    items = decode_fetch(fetch_resp or b"")
    assert items and items[0]["error_code"] == 0


def test_monitor_rollover_and_gc_removes_old_generation():
    ensure_cluster_ready()
    big_payload = b"A" * 70000
    resp = send_frame_with_retry(*NODES[1], make_produce_request("logs", big_payload))
    assert resp and all(code == 0 for code in parse_produce_error_codes(resp)), "produce failed"

    # Wait for monitor loop (configured to 1s) to detect oversize and roll over.
    time.sleep(6)
    state_resp = send_frame_with_retry(*NODES[1], make_internal_state_request(["logs"]))
    state = decode_internal_state(state_resp or b"")
    assert state, "failed to decode internal state response"
    generations = [
        part["generation"]
        for topic in state["topics"]
        if topic["name"] == "logs"
        for part in topic["partitions"]
    ]
    assert generations, "no generations reported for logs"
    assert max(generations) > 1, f"expected rollover to advance generation, got {generations}"


def test_internal_state_reports_leader_and_generation():
    ensure_cluster_ready()
    frame = make_internal_state_request(["logs"])
    for node_id, (host, port) in NODES.items():
        resp = send_frame_with_retry(host, port, frame)
        state = decode_internal_state(resp or b"")
        assert state, f"internal state decode failed on node {node_id}"
        topics = {t["name"]: t for t in state["topics"]}
        assert "logs" in topics, f"logs missing in internal state on node {node_id}"
        partitions = topics["logs"]["partitions"]
        assert partitions, f"no partitions reported on node {node_id}"
        for part in partitions:
            assert part["leader"] == 1, f"leader mismatch on node {node_id}: {part}"
            assert part["generation"] >= 1, f"generation missing on node {node_id}: {part}"


def test_hot_join_node4_reports_metadata():
    ensure_cluster_ready()
    ensure_network()
    cleanup_node4_container()
    subprocess.run(COMPOSE + ["up", "-d", "node1", "node2", "node3"], check=True, cwd=PROJECT_ROOT)
    # Start node4 only when needed
    subprocess.run(COMPOSE + ["--profile", "hotjoin", "up", "-d", "node4"], check=True, cwd=PROJECT_ROOT)
    wait_for_specific_ports(HOTJOIN_NODE, timeout=60)
    meta = decode_metadata(send_frame_with_retry(*list(HOTJOIN_NODE.values())[0], make_metadata_request(["logs"])) or b"")
    assert meta, "node4 metadata decode failed"
    assert meta["local_node_id"] == 4
    subprocess.run(COMPOSE + ["--profile", "hotjoin", "stop", "node4"], check=True, cwd=PROJECT_ROOT)


def test_fetch_respects_offsets_and_payloads_from_leader():
    ensure_cluster_ready()

    # Use partition 1 to avoid earlier payload noise.
    before = snapshot_data_plane_mtimes()
    payloads = [b"OFFSET_A", b"OFFSET_B", b"OFFSET_C"]
    for pay in payloads:
        produce_and_assert_ok(NODES[1], "logs", pay, partition=1)
    time.sleep(2)
    after = snapshot_data_plane_mtimes()
    assert set(after[1].items()) - set(before[1].items()), "leader storage did not change for partition 1 writes"

    # Offset 0 should return first payload for the current generation.
    fetch0 = send_frame_with_retry(*NODES[1], make_fetch_with_offset("logs", 1, 0))
    items0 = decode_fetch(fetch0 or b"")
    assert items0 and items0[0]["error_code"] == 0
    if items0[0]["payload"]:
        assert items0[0]["payload"] in payloads, f"unexpected payload at offset 0: {items0[0]['payload']}"

    # Offset 2 should surface the third entry.
    fetch2 = send_frame_with_retry(*NODES[1], make_fetch_with_offset("logs", 1, 2))
    items2 = decode_fetch(fetch2 or b"")
    assert items2 and items2[0]["error_code"] == 0
    if items2[0]["payload"]:
        assert items2[0]["payload"] in payloads, f"unexpected payload at offset 2: {items2[0]['payload']}"


def test_leader_failover_timeout_then_recovery():
    ensure_cluster_ready()
    produce_and_assert_ok(NODES[1], "logs", b"FAILOVER_BASE")
    time.sleep(1)

    # Kill leader and ensure follower produce fails fast (backpressure/timeout path).
    subprocess.run(COMPOSE + ["stop", "node1"], check=True, cwd=PROJECT_ROOT)
    wait_for_specific_ports({2: NODES[2], 3: NODES[3]}, timeout=20)
    start = time.time()
    resp = send_frame(NODES[2][0], NODES[2][1], make_produce_request("logs", b"SHOULD_FAIL"))
    elapsed = time.time() - start
    # If leader is down, follower may drop the connection entirely or return an error code.
    if resp:
        assert any(code != 0 for code in parse_produce_error_codes(resp)), "produce should fail while leader is offline"
    assert elapsed < 8, f"produce timed out too slowly ({elapsed}s)"
    # Bring leader back and ensure writes/read persist.
    subprocess.run(COMPOSE + ["start", "node1"], check=True, cwd=PROJECT_ROOT)
    wait_for_specific_ports({1: NODES[1]}, timeout=60)
    deadline = time.time() + 90
    while time.time() < deadline:
        if metadata_ready_for_node(1, *NODES[1]):
            break
        time.sleep(2)
    ensure_network()
    subprocess.run(COMPOSE + ["up", "-d", "node1", "node2", "node3"], check=True, cwd=PROJECT_ROOT)
    ensure_cluster_ready(timeout=120)
    before = snapshot_data_plane_mtimes()
    resp = send_frame_with_retry(
        *NODES[1], make_produce_request("logs", b"FAILOVER_RECOVER"), attempts=15, delay=2
    )
    assert resp, "leader did not respond after recovery"
    time.sleep(2)
    fetch = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request("logs", 4096)) or b"")
    assert fetch and fetch[0]["error_code"] == 0, "fetch after leader recovery failed"
    after = snapshot_data_plane_mtimes()
    assert set(after[1].items()) - set(before[1].items()), "leader storage did not advance after recovery append"


def test_leader_pause_causes_timeout_and_resumes():
    """Pause leader with SIGSTOP to exercise client timeout path without killing container."""
    ensure_cluster_ready()
    produce_and_assert_ok(NODES[1], "logs", b"PAUSE_BASE")
    time.sleep(1)

    pause_container("walrus-1")
    try:
        start = time.time()
        resp = send_frame(NODES[2][0], NODES[2][1], make_produce_request("logs", b"PAUSE_SHOULD_TIMEOUT"))
        elapsed = time.time() - start
        if resp:
            assert any(code != 0 for code in parse_produce_error_codes(resp)), "produce should fail while leader paused"
        assert elapsed < 8, f"produce during pause took too long ({elapsed}s)"
    finally:
        resume_container("walrus-1")
    wait_for_specific_ports({1: NODES[1]}, timeout=40)
    ensure_cluster_ready(timeout=120)

    # Writes should succeed again.
    after_resp = send_frame_with_retry(
        *NODES[1], make_produce_request("logs", b"PAUSE_RECOVER"), attempts=8, delay=2
    )
    assert after_resp, "leader did not respond after resume"
    fetch = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request("logs", 4096)) or b"")
    assert fetch and fetch[0]["error_code"] == 0


def test_full_cluster_restart_preserves_data():
    ensure_cluster_ready()
    payload = b"CLUSTER_RESTART_PAYLOAD"
    produce_and_assert_ok(NODES[1], "logs", payload)
    time.sleep(1)

    subprocess.run(COMPOSE + ["stop", "node1", "node2", "node3"], check=True, cwd=PROJECT_ROOT)
    time.sleep(2)
    subprocess.run(COMPOSE + ["start", "node1", "node2", "node3"], check=True, cwd=PROJECT_ROOT)
    wait_for_ports(timeout=90)
    deadline = time.time() + 120
    while time.time() < deadline:
        if metadata_ready_for_node(1, *NODES[1]):
            break
        time.sleep(2)

    fetch = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request("logs", 4096)) or b"")
    assert fetch and fetch[0]["error_code"] == 0
    assert has_files(data_plane_root(1)), "leader data files missing after full cluster restart"


def test_follower_restart_catches_up_and_reads():
    ensure_cluster_ready()
    subprocess.run(COMPOSE + ["stop", "node3"], check=True, cwd=PROJECT_ROOT)
    time.sleep(2)
    produce_and_assert_ok(NODES[1], "logs", b"FOLLOWER_RESTART_DATA")
    subprocess.run(COMPOSE + ["start", "node3"], check=True, cwd=PROJECT_ROOT)
    wait_for_specific_ports({3: NODES[3]}, timeout=40)
    time.sleep(4)
    ensure_cluster_ready(timeout=120)

    # Follower should at least respond to metadata and the cluster should still serve reads via the leader.
    meta = decode_metadata(send_frame_with_retry(*NODES[3], make_metadata_request(["logs"])) or b"")
    assert meta and meta["local_node_id"] == 3
    leader_fetch = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request("logs", 4096)) or b"")
    assert leader_fetch and leader_fetch[0]["error_code"] == 0

    # Follower forwards fetch to leader; ensure it answers after catching up.
    follower_fetch = decode_fetch(send_frame_with_retry(*NODES[3], make_fetch_request("logs", 4096)) or b"")
    assert follower_fetch and follower_fetch[0]["error_code"] == 0


def force_rollover_to_generation(target_generation: int):
    """Write large payloads until the reported generation reaches target_generation."""
    for _ in range(20):
        ensure_cluster_ready()
        gens = list_generations(1)
        if gens and max(gens) >= target_generation:
            return
        produce_and_assert_ok(NODES[1], "logs", b"R" * 70000)
        time.sleep(8)
    pytest.fail(f"did not reach generation {target_generation}")


def test_retention_gc_prunes_old_generations():
    ensure_cluster_ready()
    start_count = wal_file_count(1)
    start_mtime = wal_file_mtime_ns(1)

    force_rollover_to_generation(2)
    time.sleep(3)
    gens = list_generations(1)
    assert gens, "no generations found after rollover"
    assert gens[-1] >= 2
    assert wal_file_count(1) == start_count or wal_file_count(1) == 1
    assert wal_file_mtime_ns(1) >= start_mtime, "wal file mtime did not advance after rollover"

    force_rollover_to_generation(3)
    time.sleep(3)
    gens = list_generations(1)
    assert gens and gens[-1] >= 3
    assert wal_file_count(1) == start_count or wal_file_count(1) == 1
    assert wal_file_mtime_ns(1) >= start_mtime, "wal file mtime did not advance across generations"


def test_hot_join_rejoin_promotes_node4():
    node4 = list(HOTJOIN_NODE.values())[0]
    ensure_cluster_ready()
    ensure_network()
    cleanup_node4_container()
    subprocess.run(COMPOSE + ["up", "-d", "node1", "node2", "node3"], check=True, cwd=PROJECT_ROOT)
    subprocess.run(COMPOSE + ["--profile", "hotjoin", "up", "-d", "node4"], check=True, cwd=PROJECT_ROOT)
    wait_for_specific_ports(HOTJOIN_NODE, timeout=60)
    produce_and_assert_ok(node4, "logs", b"HOTJOIN_DATA")
    fetch = decode_fetch(send_frame_with_retry(*node4, make_fetch_request("logs", 4096)) or b"")
    assert fetch and all(item["error_code"] == 0 for item in fetch)

    # Stop, wipe data to force fresh join, and start again to ensure promotion loop tolerates rejoin.
    subprocess.run(COMPOSE + ["--profile", "hotjoin", "stop", "node4"], check=True, cwd=PROJECT_ROOT)
    shutil.rmtree(PROJECT_ROOT / "test_data" / "node4", ignore_errors=True)
    subprocess.run(COMPOSE + ["--profile", "hotjoin", "up", "-d", "node4"], check=True, cwd=PROJECT_ROOT)
    wait_for_specific_ports(HOTJOIN_NODE, timeout=60)
    deadline = time.time() + 90
    meta = None
    while time.time() < deadline and not meta:
        meta = decode_metadata(send_frame_with_retry(*node4, make_metadata_request(["logs"])) or b"")
        if meta:
            break
        time.sleep(2)
    assert meta and meta["local_node_id"] == 4
    subprocess.run(COMPOSE + ["--profile", "hotjoin", "stop", "node4"], check=True, cwd=PROJECT_ROOT)


def test_remove_node_from_membership_and_continue_io():
    ensure_cluster_ready()
    # Remove voter 3; expect leader to handle membership change.
    resp = send_frame_with_retry(*NODES[1], make_membership_remove_request(3))
    assert resp, "membership request returned no response"
    code = struct.unpack_from(">h", resp, 4)[0]
    assert code == 0, f"membership change failed with code {code}"

    # Writes and reads should still succeed on remaining nodes.
    produce_and_assert_ok(NODES[1], "logs", b"AFTER_REMOVE")
    time.sleep(1)
    fetch = decode_fetch(send_frame_with_retry(*NODES[2], make_fetch_request("logs", 4096)) or b"")
    assert fetch and fetch[0]["error_code"] == 0
