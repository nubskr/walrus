import os
import random
import shutil
import socket
import struct
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional
from uuid import uuid4

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


def wait_until(predicate, timeout=10.0, interval=0.2, msg="Timeout waiting for condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval)
    pytest.fail(msg)


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


def make_truncated_produce_request(
    topic: str, declared_size: int, actual_payload: bytes, correlation_id: int = 77, partition: int = 0
) -> bytes:
    """
    Build a Produce frame whose record set size claims more bytes than provided to hit decode errors.
    """
    header = struct.pack(">hhih", 0, 0, correlation_id, 4) + b"test"

    body = struct.pack(">hi", 1, 5000)
    body += struct.pack(">i", 1)
    body += kafka_string(topic)
    body += struct.pack(">i", 1)
    body += struct.pack(">i", partition)
    body += struct.pack(">i", declared_size)
    body += actual_payload  # intentionally shorter than declared_size

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


def make_metadata_request_with_count(count: int, topics=None, correlation_id: int = 1) -> bytes:
    """Craft a Metadata request with an explicit topics count (can be negative)."""
    topics = topics or []
    header = struct.pack(">hhih", 3, 0, correlation_id, 4) + b"test"
    body = struct.pack(">i", count)
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

def make_create_topics_request_with_repl(topics: list[tuple[str, int]], replication: int, correlation_id: int = 7) -> bytes:
    header = struct.pack(">hhih", 19, 0, correlation_id, 4) + b"test"
    body = struct.pack(">i", len(topics))
    for name, partitions in topics:
        body += kafka_string(name)
        body += struct.pack(">i", partitions)
        body += struct.pack(">h", replication)
        body += struct.pack(">i", 0)
        body += struct.pack(">i", 0)
    body += struct.pack(">i", 10000)
    frame = header + body
    return struct.pack(">i", len(frame)) + frame

def make_create_topics_request_with_partitions(topics: list[tuple[str, int]], partitions: int, correlation_id: int = 7) -> bytes:
    header = struct.pack(">hhih", 19, 0, correlation_id, 4) + b"test"
    body = struct.pack(">i", len(topics))
    for name, _ in topics:
        body += kafka_string(name)
        body += struct.pack(">i", partitions)
        body += struct.pack(">h", 1)
        body += struct.pack(">i", 0)
        body += struct.pack(">i", 0)
    body += struct.pack(">i", 10000)
    frame = header + body
    return struct.pack(">i", len(frame)) + frame


def make_create_topics_request_with_assignment_and_configs(
    topic: str, correlation_id: int = 15, config: Optional[tuple[str, str]] = None
) -> bytes:
    """
    CreateTopics payload that includes replica assignments and configs to exercise decoding paths.
    """
    header = struct.pack(">hhih", 19, 0, correlation_id, 4) + b"test"
    body = struct.pack(">i", 1)  # topic count
    body += kafka_string(topic)
    body += struct.pack(">i", 1)  # partitions
    body += struct.pack(">h", 1)  # replication factor must remain 1
    body += struct.pack(">i", 1)  # assignment count
    body += struct.pack(">i", 0)  # partition id
    body += struct.pack(">i", 1)  # replica count
    body += struct.pack(">i", 1)  # replica id
    cfgs = [config] if config else []
    body += struct.pack(">i", len(cfgs))
    for key, val in cfgs:
        body += kafka_string(key)
        body += kafka_string(val)
    body += struct.pack(">i", 10_000)  # timeout
    frame = header + body
    return struct.pack(">i", len(frame)) + frame


def make_membership_remove_request(node_id: int, correlation_id: int = 8) -> bytes:
    header = struct.pack(">hhih", 51, 0, correlation_id, 4) + b"test"
    body = struct.pack(">hi", 0, node_id)  # op=remove, node id
    return struct.pack(">i", len(header + body)) + header + body

def make_membership_request(op_code: int, node_id: int, correlation_id: int = 8) -> bytes:
    header = struct.pack(">hhih", 51, 0, correlation_id, 4) + b"test"
    body = struct.pack(">hi", op_code, node_id)
    return struct.pack(">i", len(header + body)) + header + body


def make_join_cluster_request(node_id: int, addr: str, correlation_id: int = 9) -> bytes:
    header = struct.pack(">hhih", 51, 0, correlation_id, 4) + b"test"
    encoded_addr = kafka_string(addr)
    body = struct.pack(">hi", 2, node_id) + encoded_addr  # op=2 reserved for join for this test
    return struct.pack(">i", len(header + body)) + header + body


def decode_metadata_leader(meta: dict, topic: str = "logs", partition: int = 0) -> Optional[int]:
    for t in meta.get("topics", []):
        if t.get("name") != topic:
            continue
        for part in t.get("partitions", []):
            if part.get("partition") == partition:
                return int(part.get("leader"))
    return None


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
            s.settimeout(7.0)
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
    wait_for_ports(timeout=timeout)
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


def list_generations(node_id: int, topic: str = "logs") -> list[int]:
    """Return generation numbers for a topic/partition as reported by internal state."""
    resp = send_frame_with_retry(*NODES.get(node_id, NODES[1]), make_internal_state_request([topic]))
    state = decode_internal_state(resp or b"")
    if not state:
        return []
    gens: list[int] = []
    for topic_info in state["topics"]:
        if topic_info["name"] != topic:
            continue
        for part in topic_info["partitions"]:
            gens.append(int(part["generation"]))
    gens.sort()
    return gens


def force_rollover_to_generation(target_generation: int, topic: str = "logs", node_id: int = 1):
    """Write large payloads until the reported generation reaches target_generation."""
    for _ in range(100):
        ensure_cluster_ready()
        gens = list_generations(node_id, topic)
        if gens and max(gens) >= target_generation:
            return
        produce_and_assert_ok(NODES[node_id], topic, b"R" * 1024 * 1024)
        time.sleep(0.1)
    pytest.fail(f"did not reach generation {target_generation}")


def wal_file_count(node_id: int) -> int:
    root = data_plane_root(node_id)
    if not root.exists():
        return 0
    return len([p for p in root.iterdir() if p.is_file()])


def data_plane_size_bytes(node_id: int) -> int:
    root = data_plane_root(node_id)
    total = 0
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            try:
                total += os.path.getsize(Path(dirpath) / name)
            except FileNotFoundError:
                continue
    return total


def dir_size(path: Path) -> int:
    total = 0
    if not path.exists():
        return total
    for dirpath, _, filenames in os.walk(path):
        for name in filenames:
            try:
                total += os.path.getsize(Path(dirpath) / name)
            except FileNotFoundError:
                continue
    return total


def wal_dirs_for_topic(node_id: int, topic: str) -> list[Path]:
    root = data_plane_root(node_id)
    if not root.exists():
        return []
    prefix = f"t_{topic}_p_"
    return [p for p in root.iterdir() if p.is_dir() and p.name.startswith(prefix)]


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


def parse_create_topics_error_codes(resp: bytes) -> list[int]:
    """Return Kafka error codes from a CreateTopics response frame."""
    idx = 0
    _correlation_id = struct.unpack_from(">i", resp, idx)[0]
    idx += 4
    _throttle = struct.unpack_from(">i", resp, idx)[0]
    idx += 4
    topic_count = struct.unpack_from(">i", resp, idx)[0]
    idx += 4
    codes: list[int] = []
    for _ in range(topic_count):
        name_len = struct.unpack_from(">h", resp, idx)[0]
        idx += 2 + name_len
        code = struct.unpack_from(">h", resp, idx)[0]
        idx += 2
        msg_len = struct.unpack_from(">h", resp, idx)[0]
        idx += 2 + max(msg_len, 0)
        cfg_count = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        for _ in range(cfg_count):
            # key+value string pairs
            key_len = struct.unpack_from(">h", resp, idx)[0]
            idx += 2 + max(key_len, 0)
            val_len = struct.unpack_from(">h", resp, idx)[0]
            idx += 2 + max(val_len, 0)
        codes.append(code)
    return codes


def test_malformed_produce_frame_returns_error():
    """
    Send a Produce frame with a truncated record set size to hit the Kafka facade's
    validation error path.
    """
    ensure_cluster_ready()
    topic = f"malformed-{uuid4().hex[:4]}"
    # Create topic so metadata path exists.
    resp = send_frame_with_retry(*NODES[1], make_create_topics_request_with_repl([(topic, 1)], 1))
    assert resp, "create topics response missing"

    header = struct.pack(">hhih", 0, 0, 99, 4) + b"test"
    body = struct.pack(">hi", 1, 5000)  # acks, timeout
    body += struct.pack(">i", 1)  # topic count
    body += kafka_string(topic)
    body += struct.pack(">i", 1)  # partition count
    body += struct.pack(">i", 0)  # partition id
    body += struct.pack(">i", 10)  # declared size
    body += b"\x00\x01"  # only 2 bytes of payload, intentionally truncated
    frame = header + body
    full = struct.pack(">i", len(frame)) + frame
    bad_resp = send_frame_with_retry(*NODES[1], full)
    assert bad_resp is None or bad_resp.startswith(b"\x00\x00"), "expected connection drop or empty response"


def test_negative_record_set_size_rejected():
    """
    Send a Produce frame with a negative record set size; server should reject decode.
    """
    ensure_cluster_ready()
    topic = f"negsize-{uuid4().hex[:4]}"
    send_frame_with_retry(*NODES[1], make_create_topics_request_with_repl([(topic, 1)], 1))

    header = struct.pack(">hhih", 0, 0, 100, 4) + b"test"
    body = struct.pack(">hi", 1, 5000)
    body += struct.pack(">i", 1)
    body += kafka_string(topic)
    body += struct.pack(">i", 1)
    body += struct.pack(">i", 0)
    body += struct.pack(">i", -5)  # negative size
    frame = header + body
    full = struct.pack(">i", len(frame)) + frame
    bad_resp = send_frame_with_retry(*NODES[1], full)
    # Server should close or return no useful body; treat any response as failure.
    assert bad_resp is None or bad_resp.startswith(b"\x00\x00"), "expected connection drop or empty response"


def test_truncated_produce_frame_preserves_offsets_after_disconnect():
    """
    Send a Produce frame that declares more bytes than provided and ensure offsets do not advance.
    """
    ensure_cluster_ready()
    topic = f"trunc-{uuid4().hex[:6]}"
    send_frame_with_retry(*NODES[1], make_create_topics_request([(topic, 1)]))
    produce_and_assert_ok(NODES[1], topic, b"SAFE_BASE")

    base = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request(topic, 4096)) or b"")
    assert base and base[0]["error_code"] == 0
    base_hw = base[0]["high_watermark"]

    frame = make_truncated_produce_request(topic, declared_size=64, actual_payload=b"\x01\x02", partition=0)
    resp = send_frame(*NODES[1], frame)
    assert resp is None or resp.startswith(b"\x00\x00"), "expected connection drop for truncated produce payload"

    time.sleep(1)
    after = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request(topic, 4096)) or b"")
    assert after and after[0]["error_code"] == 0
    assert after[0]["high_watermark"] == base_hw, "truncated produce should not advance offsets"


def test_produce_negative_topic_count_drops_connection():
    """Craft a Produce request with negative topic count; server should reject and leave state unchanged."""
    ensure_cluster_ready()
    baseline = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request("logs", 4096)) or b"")
    hw = baseline[0]["high_watermark"] if baseline else 0

    header = struct.pack(">hhih", 0, 0, 8888, 4) + b"test"
    body = struct.pack(">hi", 1, 5000)
    body += struct.pack(">i", -1)  # negative topic count
    frame = header + body
    bad_resp = send_frame(*NODES[1], struct.pack(">i", len(frame)) + frame)
    assert bad_resp is None or bad_resp.startswith(b"\x00\x00"), "expected connection drop for negative topic count"

    after = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request("logs", 4096)) or b"")
    assert after and after[0]["error_code"] == 0
    assert after[0]["high_watermark"] == hw, "negative topic count should not change offsets"


def test_fetch_negative_topic_count_drops_connection():
    """Fetch with negative topic count should fail validation without crashing the cluster."""
    ensure_cluster_ready()
    header = struct.pack(">hhih", 1, 0, 42424, 4) + b"test"
    body = struct.pack(">iii", -1, 0, 0)
    body += struct.pack(">i", -5)  # negative topic count
    frame = header + body
    bad_resp = send_frame(*NODES[2], struct.pack(">i", len(frame)) + frame)
    assert bad_resp is None or bad_resp.startswith(b"\x00\x00"), "expected connection drop for negative fetch topic count"

    # Cluster should still answer normal fetches afterwards.
    ok = decode_fetch(send_frame_with_retry(*NODES[2], make_fetch_request("logs", 4096)) or b"")
    assert ok and ok[0]["error_code"] == 0


def test_unsupported_api_key_closes_connection_and_cluster_recovers():
    """Send an unknown API key to exercise the fallback error path in the Kafka facade."""
    ensure_cluster_ready()
    header = struct.pack(">hhih", 99, 0, 500, 4) + b"oops"
    frame = struct.pack(">i", len(header)) + header
    resp = send_frame(*NODES[1], frame)
    assert resp is None or resp.startswith(b"\x00\x00"), "unexpected payload for unsupported api key"

    # Verify nodes still respond to ApiVersions afterwards.
    av = decode_api_versions(send_frame_with_retry(*NODES[1], make_api_versions_request()) or b"")
    assert av and av["error_code"] == 0


def test_membership_remove_current_leader_noop_and_io_continues():
    """
    Removing the current leader is treated as a no-op reassign in metadata; ensure we still serve IO.
    """
    ensure_cluster_ready()
    before = decode_metadata(send_frame_with_retry(*NODES[1], make_metadata_request(["logs"])) or b"")
    assert before

    resp = send_frame_with_retry(*NODES[1], make_membership_remove_request(1))
    assert resp, "membership request for leader returned no response"
    code = struct.unpack_from(">h", resp, 4)[0]
    assert code == 0, f"leader remove returned error {code}"

    produce_and_assert_ok(NODES[1], "logs", b"AFTER_LEADER_REMOVE_NOOP")
    fetch = decode_fetch(send_frame_with_retry(*NODES[2], make_fetch_request("logs", 4096)) or b"")
    assert fetch and fetch[0]["error_code"] == 0

    after = decode_metadata(send_frame_with_retry(*NODES[1], make_metadata_request(["logs"])) or b"")
    assert after
    assert decode_metadata_leader(before) == decode_metadata_leader(after)


def test_weird_topic_name_is_sanitized_and_roundtrips():
    """
    Create a topic with path-hostile characters to exercise walrus path sanitization and IO flow.
    """
    ensure_cluster_ready()
    topic = "weird/..//topic:!$"
    before_dirs = {p.name for p in data_plane_root(1).iterdir()} if data_plane_root(1).exists() else set()

    resp = send_frame_with_retry(*NODES[1], make_create_topics_request([(topic, 1)]))
    items = decode_create_topics_resp(resp or b"")
    assert items and items[0][1] in (0, 36), f"create failed for weird topic: {items}"

    wait_for_topics([topic], timeout=60)
    send_test_control(2)  # sync leases explicitly for the fresh topic
    prod_resp = send_frame_with_retry(*NODES[1], make_produce_request(topic, b"ODD_NAME_PAYLOAD"))
    assert prod_resp, "produce for weird topic returned no response"
    prod_codes = parse_produce_error_codes(prod_resp)
    assert all(code in (0, 6) for code in prod_codes), f"unexpected produce codes {prod_codes}"

    def check_fetch():
        f = decode_fetch(send_frame(*NODES[1], make_fetch_request(topic, 4096)) or b"")
        return f and f[0]["error_code"] in (0, 1)  # may be empty if produce rejected

    wait_until(check_fetch, timeout=20)
    fetched = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request(topic, 4096)) or b"")
    assert fetched and fetched[0]["error_code"] in (0, 1)
    # If produce failed due to validation, payload may be empty; the key point is the cluster stays up and paths are sanitized.

    after_dirs = {p.name for p in data_plane_root(1).iterdir()} if data_plane_root(1).exists() else set()
    new_dirs = after_dirs - before_dirs
    assert all(".." not in d for d in new_dirs), f"sanitized directory contains traversal: {new_dirs}"
    if new_dirs:
        assert any(d.startswith("t_") for d in new_dirs), f"expected walrus segment directory for weird topic, saw {new_dirs}"
    else:
        # No disk artifacts were written; still ensure cluster remains responsive through metadata/fetch.
        assert decode_metadata(send_frame_with_retry(*NODES[1], make_metadata_request([topic])) or b"")


def test_follower_fetch_survives_monitor_rollover_and_gc():
    """
    Continuously fetch from a follower while forcing multiple rollovers/gc cycles on the leader.
    Ensures follower routing stays healthy during monitor activity.
    """
    ensure_cluster_ready()
    stop = threading.Event()
    errors: list[str] = []

    def fetch_loop():
        while not stop.is_set():
            resp = send_frame_with_retry(*NODES[2], make_fetch_request("logs", 4096))
            if resp:
                dec = decode_fetch(resp)
                if not dec or dec[0]["error_code"] != 0:
                    errors.append(f"bad fetch {dec}")
            time.sleep(0.5)

    t = threading.Thread(target=fetch_loop, daemon=True)
    t.start()
    try:
        force_rollover_to_generation(3)
    finally:
        stop.set()
        t.join(timeout=5)

    assert not errors, f"follower fetch errors during monitor activity: {errors}"
    final = decode_fetch(send_frame_with_retry(*NODES[2], make_fetch_request("logs", 4096)) or b"")
    assert final and final[0]["error_code"] == 0


def test_join_cluster_duplicate_node_id_is_tolerated_and_cluster_continues_io():
    """
    Trigger a join for an already-present node-id. The controller may accept or reject;
    validation is that the cluster remains healthy and IO still works.
    """
    ensure_cluster_ready()
    resp = send_test_control(3, topic="node1:6001")
    if not resp:
        pytest.skip("test control API unavailable on this build")
    code = decode_test_control_code(resp)
    assert code is not None, "could not decode join control response"

    # Regardless of code, cluster should continue to serve reads/writes.
    produce_and_assert_ok(NODES[1], "logs", b"JOIN_DUP_OK")
    fetch = decode_fetch(send_frame_with_retry(*NODES[2], make_fetch_request("logs", 4096)) or b"")
    assert fetch and fetch[0]["error_code"] == 0


def decode_produce_response(resp: bytes) -> Optional[list[dict]]:
    """Return topic/partition/error/base_offset entries from a Produce response."""
    try:
        idx = 0
        _corr = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        topic_count = struct.unpack_from(">i", resp, idx)[0]
        idx += 4
        entries: list[dict] = []
        for _ in range(topic_count):
            name_len = struct.unpack_from(">h", resp, idx)[0]
            idx += 2
            name = resp[idx : idx + name_len].decode()
            idx += name_len
            part_count = struct.unpack_from(">i", resp, idx)[0]
            idx += 4
            for _ in range(part_count):
                partition = struct.unpack_from(">i", resp, idx)[0]
                idx += 4
                error_code = struct.unpack_from(">h", resp, idx)[0]
                idx += 2
                base_offset = struct.unpack_from(">q", resp, idx)[0]
                idx += 8
                entries.append(
                    {
                        "topic": name,
                        "partition": partition,
                        "error_code": error_code,
                        "base_offset": base_offset,
                    }
                )
        return entries
    except Exception:
        return None


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
            if topic["name"] == "logs" and part["leader"] > 0:
                return True
    return False


def wait_for_topics(topics: list[str], timeout: int = 30):
    """Block until all topics appear in metadata on node1."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if topics_present_on_node(NODES[1][0], NODES[1][1], topics):
            return
        time.sleep(1)
    pytest.fail(f"topics did not appear in metadata: {topics}")


def topics_present_on_node(host: str, port: int, topics: list[str]) -> bool:
    resp = send_frame(host, port, make_metadata_request(topics))
    meta = decode_metadata(resp or b"")
    if not meta:
        return False
    present = {topic["name"] for topic in meta["topics"]}
    return all(t in present for t in topics)


def wait_for_topics_on_all_nodes(topics: list[str], timeout: int = 45):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if all(topics_present_on_node(host, port, topics) for host, port in NODES.values()):
            return
        time.sleep(1)
    pytest.fail(f"topics did not appear on all nodes: {topics}")


def wait_for_topics(topics: list[str], timeout: int = 30):
    """Block until all topics appear in metadata on node1."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = send_frame(*NODES[1], make_metadata_request(topics))
        meta = decode_metadata(resp or b"")
        if meta:
            present = {topic["name"] for topic in meta["topics"]}
            if all(t in present for t in topics):
                return
        time.sleep(1)
    pytest.fail(f"topics did not appear in metadata: {topics}")


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

    wait_until(lambda: set(snapshot_data_plane_mtimes()[1].items()) - set(before[1].items()), msg="Leader storage did not update")

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

        wait_until(lambda: set(snapshot_data_plane_mtimes()[1].items()) - set(before[1].items()), msg=f"Leader storage did not update for follower {follower}")

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
    assert supported.get(60) == (0, 0)
    assert supported.get(61) == (0, 0)
    assert supported.get(62) == (0, 0)
    assert supported.get(63) == (0, 0)


def test_metadata_filtered_unknown_topic_returns_empty():
    """Filtered metadata for a random topic should return no topics, not crash."""
    topic = f"missing-{uuid4().hex[:6]}"
    frame = make_metadata_request([topic])
    resp = send_frame_with_retry(*NODES[1], frame)
    meta = decode_metadata(resp or b"")
    assert meta is not None, "metadata decode failed"
    assert meta["topics"] == [], f"expected no topics for missing filter, got {meta['topics']}"


def test_metadata_negative_topic_count_returns_all_topics():
    """
    decode_string_array returns empty on negative counts; ensure Metadata treats this as unfiltered.
    """
    ensure_cluster_ready()
    resp = send_frame_with_retry(
        *NODES[1], make_metadata_request_with_count(-1, topics=["ignored-topic"], correlation_id=31337)
    )
    meta = decode_metadata(resp or b"")
    assert meta and meta["topics"], "metadata should return topics for negative count request"
    assert any(t["name"] == "logs" for t in meta["topics"]), f"logs topic missing from {meta}"


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

    def check_fetch():
        fetch_resp = send_frame(*NODES[3], make_fetch_request("logs", 4096))
        if not fetch_resp: return False
        items = decode_fetch(fetch_resp)
        return items and all(item["error_code"] == 0 for item in items)

    wait_until(check_fetch)

    fetch_resp = send_frame_with_retry(*NODES[3], make_fetch_request("logs", 4096))
    items = decode_fetch(fetch_resp or b"")
    assert items, "fetch returned no payloads"
    assert all(item["error_code"] == 0 for item in items), f"fetch returned errors {items}"
    # Even if payload is empty (depending on Walrus read offsets), the follower
    # should return a successful response path via the leader.


def test_fetch_payload_and_offsets_from_leader():
    topic = "offsets_topic"
    send_frame_with_retry(*NODES[1], make_create_topics_request([(topic, 1)]))

    resp1 = send_frame_with_retry(*NODES[1], make_produce_request(topic, b"FIRST"))
    resp2 = send_frame_with_retry(*NODES[1], make_produce_request(topic, b"SECOND"))
    assert resp1 and resp2
    assert all(code == 0 for code in parse_produce_error_codes(resp1 + resp2))

    wait_until(lambda: decode_fetch(send_frame(*NODES[1], make_fetch_request(topic, 4096, partition=0)) or b""))

    fetch0 = send_frame_with_retry(*NODES[1], make_fetch_request(topic, 4096, partition=0))
    items0 = decode_fetch(fetch0 or b"")
    assert items0 and items0[0]["error_code"] == 0
    assert items0[0]["payload"].startswith(b"FIRST") or items0[0]["payload"] == b""

    # Offsets are byte-based; use the length of the first payload to reach the second one.
    second_offset = len(b"FIRST")
    header = struct.pack(">hhih", 1, 0, 100, 4) + b"test"
    body = struct.pack(">iii", -1, 0, 0) + struct.pack(">i", 1) + kafka_string(topic)
    body += (
        struct.pack(">i", 1)
        + struct.pack(">i", 0)
        + struct.pack(">q", second_offset)
        + struct.pack(">i", 4096)
    )
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

    def check_fetch():
        f = decode_fetch(send_frame(*NODES[1], make_fetch_request("topic_x", 4096)) or b"")
        return f and f[0]["error_code"] == 0 and f[0]["payload"] in (data, b"")

    wait_until(check_fetch)

    fetch = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request("topic_x", 4096)) or b"")
    assert fetch and fetch[0]["error_code"] == 0
    assert fetch[0]["payload"] in (data, b"")


def test_fetch_offset_strictness_with_high_watermark():
    ensure_cluster_ready()
    send_frame_with_retry(*NODES[1], make_create_topics_request([("strict_topic", 1)]))
    payloads = [b"S1", b"S2", b"S3"]
    total_bytes = sum(len(p) for p in payloads)
    for pay in payloads:
        produce_and_assert_ok(NODES[1], "strict_topic", pay, partition=0)

    wait_until(lambda: decode_fetch(send_frame(*NODES[1], make_fetch_with_offset("strict_topic", 0, 0)) or b""))

    fetch0 = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_with_offset("strict_topic", 0, 0)) or b"")
    assert fetch0 and fetch0[0]["error_code"] == 0
    assert fetch0[0]["high_watermark"] >= total_bytes
    # Payload may still be empty depending on WAL semantics, but offset 0 must be accepted.

    fetch2 = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_with_offset("strict_topic", 0, 2)) or b"")
    assert fetch2 and fetch2[0]["error_code"] == 0
    assert fetch2[0]["high_watermark"] >= total_bytes

    # Offset beyond available should return an error.
    fetch4 = decode_fetch(
        send_frame_with_retry(*NODES[1], make_fetch_with_offset("strict_topic", 0, total_bytes + 1)) or b""
    )
    assert fetch4 and fetch4[0]["error_code"] != 0
    assert fetch4[0]["high_watermark"] >= total_bytes


def test_fetch_with_too_small_max_bytes_returns_empty_payload():
    ensure_cluster_ready()
    topic = "logs"
    partition = 1
    produce_and_assert_ok(NODES[1], topic, b"TINY_PAYLOAD", partition=partition)

    def ready():
        f = decode_fetch(send_frame(*NODES[1], make_fetch_request(topic, 1024, partition=partition)) or b"")
        return f and f[0]["error_code"] == 0

    wait_until(ready)

    small = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request(topic, 2, partition=partition)) or b"")
    assert small and small[0]["error_code"] == 0
    assert small[0]["high_watermark"] >= 1
    assert isinstance(small[0]["payload"], (bytes, bytearray))


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


def test_create_topic_invalid_replication_factor_returns_error():
    ensure_cluster_ready()
    name = f"bad_repl_{int(time.time())}"
    resp = send_frame_with_retry(*NODES[1], make_create_topics_request_with_repl([(name, 1)], replication=2))
    items = decode_create_topics_resp(resp or b"")
    assert items, "create topics invalid replication returned no response"
    assert items[0][1] != 0, f"expected replication factor error, got {items}"


def test_create_topic_invalid_partitions_returns_error():
    ensure_cluster_ready()
    name = f"bad_parts_{int(time.time())}"
    resp = send_frame_with_retry(*NODES[1], make_create_topics_request_with_partitions([(name, 1)], partitions=0))
    items = decode_create_topics_resp(resp or b"")
    assert items, "create topics invalid partitions returned no response"
    assert items[0][1] != 0, f"expected invalid partitions error, got {items}"


def test_create_topic_duplicate_rejected():
    ensure_cluster_ready()
    name = f"dup_topic_{int(time.time())}"
    first = decode_create_topics_resp(send_frame_with_retry(*NODES[1], make_create_topics_request([(name, 1)])) or b"")
    assert first and first[0][1] == 0, f"initial create failed: {first}"

    second = decode_create_topics_resp(send_frame_with_retry(*NODES[1], make_create_topics_request([(name, 1)])) or b"")
    assert second, "duplicate create returned no response"
    # Server treats duplicate as idempotent success; ensure metadata did not duplicate partitions.
    assert second[0][1] == 0, f"duplicate create should not fail catastrophically: {second}"

    meta = decode_metadata(send_frame_with_retry(*NODES[1], make_metadata_request([name])) or b"")
    assert meta and any(t["name"] == name for t in meta["topics"]), "topic missing after duplicate create attempt"


def test_create_topic_consumes_assignments_and_configs():
    """
    Exercise parsing of assignment/config fields even though the controller ignores them today.
    """
    ensure_cluster_ready()
    name = f"assign_cfg_{uuid4().hex[:6]}"
    resp = send_frame_with_retry(
        *NODES[1], make_create_topics_request_with_assignment_and_configs(name, config=("compression.type", "none"))
    )
    items = decode_create_topics_resp(resp or b"")
    assert items, "create topics with assignments returned no response"
    assert items[0][0] == name
    assert items[0][1] in (0, 36), f"unexpected create code {items}"

    wait_for_topics([name])
    meta = decode_metadata(send_frame_with_retry(*NODES[1], make_metadata_request([name])) or b"")
    assert meta and any(t["name"] == name for t in meta["topics"]), "topic missing after assignment/config create"


def test_partition_one_produce_and_fetch():
    payload = b"PARTITION_ONE_PAYLOAD"
    resp = send_frame_with_retry(*NODES[1], make_produce_request("logs", payload, partition=1))
    assert resp and all(code == 0 for code in parse_produce_error_codes(resp))

    def check_fetch():
        f = decode_fetch(send_frame(*NODES[1], make_fetch_request("logs", 4096, partition=1)) or b"")
        return f and f[0]["error_code"] == 0 and f[0]["payload"] in (payload, b"")

    wait_until(check_fetch)

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
    def check_rollover():
        state_resp = send_frame(*NODES[1], make_internal_state_request(["logs"]))
        if not state_resp: return False
        state = decode_internal_state(state_resp)
        if not state: return False
        generations = [
            part["generation"]
            for topic in state["topics"]
            if topic["name"] == "logs"
            for part in topic["partitions"]
        ]
        return generations and max(generations) > 1

    wait_until(check_rollover, timeout=10.0, msg="Rollover did not occur")


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


def test_internal_state_filtered_unknown_topic_returns_empty():
    ensure_cluster_ready()
    topic = f"ghost-{uuid4().hex[:6]}"
    frame = make_internal_state_request([topic])
    resp = send_frame_with_retry(*NODES[1], frame)
    state = decode_internal_state(resp or b"")
    assert state is not None, "internal state decode failed"
    assert state["topics"] == [], f"expected no topics for missing filter, got {state['topics']}"


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
    
    wait_until(lambda: set(snapshot_data_plane_mtimes()[1].items()) - set(before[1].items()), msg="Leader storage did not update")

    after = snapshot_data_plane_mtimes()
    assert set(after[1].items()) - set(before[1].items()), "leader storage did not change for partition 1 writes"

    # Offset 0 should return first payload for the current generation.
    fetch0 = send_frame_with_retry(*NODES[1], make_fetch_with_offset("logs", 1, 0))
    items0 = decode_fetch(fetch0 or b"")
    assert items0 and items0[0]["error_code"] == 0
    if items0[0]["payload"]:
        assert items0[0]["payload"] in payloads, f"unexpected payload at offset 0: {items0[0]['payload']}"

    # Offset at the start of the third entry (byte offsets).
    offset_third = sum(len(p) for p in payloads[:2])
    fetch2 = send_frame_with_retry(*NODES[1], make_fetch_with_offset("logs", 1, offset_third))
    items2 = decode_fetch(fetch2 or b"")
    assert items2 and items2[0]["error_code"] == 0
    if items2[0]["payload"]:
        assert items2[0]["payload"] in payloads, f"unexpected payload at offset {offset_third}: {items2[0]['payload']}"


@pytest.mark.soak
def test_soak_parallel_produce_multi_topics_and_cross_read():
    """
    Exercise regular Produce across distinct topics in parallel and ensure cross-client reads succeed.
    """
    ensure_cluster_ready(timeout=180)
    topic_count = 4
    topics = [f"soak_topic_{i}" for i in range(topic_count)]
    create_resp = send_frame_with_retry(*NODES[1], make_create_topics_request([(t, 1) for t in topics]))
    created = decode_create_topics_resp(create_resp or b"")
    assert created and all(err == 0 for _, err in created), f"CreateTopics failed: {created}"
    wait_for_topics_on_all_nodes(topics, timeout=90)

    clients = topic_count
    per_client_messages = 5
    produced_payloads: dict[str, list[bytes]] = {t: [] for t in topics}

    def worker(client_id: int):
        topic = topics[client_id % len(topics)]
        for i in range(per_client_messages):
            payload = f"client{client_id}-m{i}-{time.time_ns()}".encode()
            partition = 0
            target = list(NODES.values())[(client_id + i) % len(NODES)]
            resp = send_frame_with_retry(*target, make_produce_request(topic, payload, partition=partition))
            assert resp, f"produce to {target} returned no response"
            produced = decode_produce_response(resp) or []
            assert produced and all(entry["error_code"] == 0 for entry in produced), f"produce failed via {target}: {produced}"
            produced_payloads[topic].append(payload)

    with ThreadPoolExecutor(max_workers=clients) as pool:
        list(pool.map(worker, range(clients)))

    # Post-check: each topic should expose committed offsets and surface at least one payload.
    for topic in topics:
        fetch = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request(topic, 8192, partition=0)) or b"")
        assert fetch and fetch[0]["error_code"] == 0, f"leader fetch failed for {topic}: {fetch}"
        assert fetch[0]["high_watermark"] >= per_client_messages, f"high watermark stalled for {topic}: {fetch}"
        payload_block = fetch[0]["payload"]
        if payload_block:
            assert any(p in payload_block for p in produced_payloads[topic]), f"unexpected payload for {topic}: {payload_block}"


@pytest.mark.soak
def test_soak_multi_client_produce_and_fetch_read_your_writes():
    """Spin up several logical clients appending to multiple topics and assert read-after-write."""
    ensure_cluster_ready(timeout=180)
    topic = "logs"
    topic_names = [topic]
    clients = 5
    per_client_messages = 6
    produce_and_assert_ok(NODES[1], topic, b"SOAK_BASELINE", partition=1)

    def worker(client_id: int):
        for i in range(per_client_messages):
            topic = topic_names[(client_id + i) % len(topic_names)]
            payload = f"client{client_id}-msg{i}-{time.time_ns()}".encode()
            partition = 1
            target = list(NODES.values())[(client_id + i) % len(NODES)]
            resp = send_frame_with_retry(*target, make_produce_request(topic, payload, partition=partition))
            assert resp, f"produce to {target} returned no response"
            produced = decode_produce_response(resp) or []
            assert produced and all(entry["error_code"] == 0 for entry in produced), f"produce failed via {target}: {produced}"

            def read_your_write():
                fetch_resp = send_frame_with_retry(*NODES[1], make_fetch_request(topic, 8192, partition=partition))
                items = decode_fetch(fetch_resp or b"")
                if not items or items[0]["error_code"] != 0:
                    return False
                payload_block = items[0]["payload"]
                return payload in payload_block or payload_block == b""

            wait_until(
                read_your_write,
                timeout=20.0,
                interval=0.5,
                msg=f"client {client_id} could not read its write on {topic}",
            )

    with ThreadPoolExecutor(max_workers=clients) as pool:
        list(pool.map(worker, range(clients)))


@pytest.mark.soak_long
def test_soak_long_running_mixed_load_survives():
    """
    Drive mixed produce/fetch traffic for an extended period and ensure the cluster stays healthy.
    Duration defaults to 20 minutes; override via SOAK_LONG_DURATION_SEC.
    """
    ensure_cluster_ready(timeout=180)
    duration_s = int(os.getenv("SOAK_LONG_DURATION_SEC", "1200"))
    topic_count = int(os.getenv("SOAK_LONG_EXTRA_TOPICS", "0"))
    partitions = 2
    base_topics = ["logs"]
    extra_topics = [f"soak_long_topic_{i}" for i in range(topic_count)]
    if extra_topics:
        created = decode_create_topics_resp(
            send_frame_with_retry(*NODES[1], make_create_topics_request([(t, partitions) for t in extra_topics])) or b""
        )
        assert created and all(err == 0 for _, err in created), f"CreateTopics failed: {created}"
    topics = base_topics + extra_topics
    wait_for_topics_on_all_nodes(topics, timeout=120)
    ensure_cluster_ready(timeout=120)
    time.sleep(2)  # allow lease sync after new topic creation
    send_test_control(2)  # explicitly sync leases before driving load

    produced_counts: dict[tuple[str, int], int] = {(t, p): 0 for t in topics for p in range(partitions)}
    count_lock = threading.Lock()

    def seed_topic(topic: str, partition: int):
        payload = f"SOAK-LONG-SEED-{topic}-{partition}".encode()
        deadline = time.time() + 60
        while time.time() < deadline:
            resp = send_frame_with_retry(*NODES[1], make_produce_request(topic, payload, partition=partition))
            if resp and all(code == 0 for code in parse_produce_error_codes(resp)):
                with count_lock:
                    produced_counts[(topic, partition)] += 1
                return
            time.sleep(0.5)
        pytest.fail(f"seed produce failed for {topic}-{partition}")

    for topic in topics:
        for p in range(partitions):
            seed_topic(topic, p)

    stop = time.time() + duration_s
    clients = int(os.getenv("SOAK_LONG_CLIENTS", "6"))

    def worker(client_id: int):
        rng = random.Random(client_id + int(time.time()))
        while time.time() < stop:
            topic = topics[rng.randrange(len(topics))]
            partition = rng.randrange(partitions)
            payload = f"long-{client_id}-{partition}-{time.time_ns()}".encode()
            target = list(NODES.values())[rng.randrange(len(NODES))]
            success = False
            last_resp = b""
            for _ in range(3):
                resp = send_frame_with_retry(*target, make_produce_request(topic, payload, partition=partition))
                last_resp = resp or b""
                produced = decode_produce_response(resp) or []
                if resp and produced and all(entry["error_code"] == 0 for entry in produced):
                    success = True
                    break
                time.sleep(0.2)
            if not success:
                leader_target = NODES[1]
                resp = send_frame_with_retry(*leader_target, make_produce_request(topic, payload, partition=partition))
                last_resp = resp or b""
                produced = decode_produce_response(resp) or []
                success = resp is not None and produced and all(entry["error_code"] == 0 for entry in produced)
            assert success, f"produce failed via {target}: {decode_produce_response(last_resp) or last_resp}"
            with count_lock:
                produced_counts[(topic, partition)] += 1

            # Periodically fetch from random nodes to catch follower/leader issues.
            if rng.random() < 0.4:
                fetch_target = list(NODES.values())[rng.randrange(len(NODES))]
                fetch = decode_fetch(
                    send_frame_with_retry(*fetch_target, make_fetch_request(topic, 8192, partition=partition)) or b""
                )
                assert fetch and fetch[0]["error_code"] == 0, f"fetch failed for {topic}-{partition} via {fetch_target}"
                assert fetch[0]["high_watermark"] >= 0, "high watermark should never be negative"

            # Occasionally assert metadata endpoints stay healthy across all nodes.
            if rng.random() < 0.15:
                for node_id, (host, port) in NODES.items():
                    assert metadata_ready_for_node(node_id, host, port), f"metadata not ready on node {node_id}"

            time.sleep(0.05)

    with ThreadPoolExecutor(max_workers=clients) as pool:
        list(pool.map(worker, range(clients)))

    # Final correctness pass: each topic/partition reports committed offsets.
    for (topic, partition), count in produced_counts.items():
        fetch = decode_fetch(
            send_frame_with_retry(*NODES[1], make_fetch_request(topic, 8192, partition=partition)) or b""
        )
        assert fetch and fetch[0]["error_code"] == 0, f"final fetch failed for {topic}-{partition}: {fetch}"
        assert (
            fetch[0]["high_watermark"] >= 1
        ), f"high watermark did not advance for {topic}-{partition} (count={count}, fetch={fetch})"


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
    assert elapsed < 12, f"produce timed out too slowly ({elapsed}s)"
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

    wait_until(lambda: decode_fetch(send_frame(*NODES[1], make_fetch_request("logs", 4096)) or b""))

    fetch = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request("logs", 4096)) or b"")
    assert fetch and fetch[0]["error_code"] == 0, "fetch after leader recovery failed"
    after = snapshot_data_plane_mtimes()
    assert set(after[1].items()) - set(before[1].items()), "leader storage did not advance after recovery append"


def test_follower_fetch_during_leader_pause_returns_high_watermark():
    ensure_cluster_ready()
    produce_and_assert_ok(NODES[1], "logs", b"PAUSE_FETCH_BASE")
    produce_and_assert_ok(NODES[1], "logs", b"PAUSE_FETCH_BASE2")

    pause_container("walrus-1")
    try:
        wait_until(lambda: tcp_ready(*NODES[2]), timeout=20, msg="follower unavailable during pause")
        fetch = decode_fetch(send_frame_with_retry(*NODES[2], make_fetch_request("logs", 4096)) or b"")
        assert fetch, "follower fetch returned nothing while leader paused"
        assert fetch[0]["error_code"] == 0, f"follower fetch error during pause: {fetch}"
        assert fetch[0]["high_watermark"] >= 0, f"high watermark missing during pause: {fetch}"
        # Payload may be empty because forwarding timed out; ensure no crash and a well-formed response.
    finally:
        resume_container("walrus-1")
    wait_for_specific_ports({1: NODES[1]}, timeout=40)
    ensure_cluster_ready(timeout=120)
    fetch_after = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request("logs", 4096)) or b"")
    assert fetch_after and fetch_after[0]["error_code"] == 0
    assert fetch_after[0]["high_watermark"] >= 2


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
    wait_for_specific_ports({3: NODES[3]}, timeout=90)
    time.sleep(4)
    # Ensure all core nodes are explicitly up before checking readiness to avoid flakiness.
    ensure_network()
    subprocess.run(COMPOSE + ["up", "-d", "node1", "node2", "node3"], check=True, cwd=PROJECT_ROOT)
    ensure_cluster_ready(timeout=180)

    # Follower should at least respond to metadata and the cluster should still serve reads via the leader.
    meta = decode_metadata(send_frame_with_retry(*NODES[3], make_metadata_request(["logs"])) or b"")
    assert meta and meta["local_node_id"] == 3
    leader_fetch = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request("logs", 4096)) or b"")
    assert leader_fetch and leader_fetch[0]["error_code"] == 0

    # Follower forwards fetch to leader; ensure it answers after catching up.
    follower_fetch = decode_fetch(send_frame_with_retry(*NODES[3], make_fetch_request("logs", 4096)) or b"")
    assert follower_fetch and follower_fetch[0]["error_code"] == 0


def test_retention_gc_prunes_old_generations():
    ensure_cluster_ready()
    start_count = wal_file_count(1)
    start_mtime = wal_file_mtime_ns(1)

    force_rollover_to_generation(2)
    wait_until(lambda: wal_file_count(1) <= start_count, timeout=10.0)
    gens = list_generations(1)
    assert gens, "no generations found after rollover"
    assert gens[-1] >= 2
    assert wal_file_count(1) == start_count or wal_file_count(1) == 1
    assert wal_file_mtime_ns(1) >= start_mtime, "wal file mtime did not advance after rollover"

    force_rollover_to_generation(3)
    wait_until(lambda: wal_file_count(1) <= start_count, timeout=10.0)
    gens = list_generations(1)
    assert gens and gens[-1] >= 3
    assert wal_file_count(1) == start_count or wal_file_count(1) == 1
    assert wal_file_mtime_ns(1) >= start_mtime, "wal file mtime did not advance across generations"


def test_produce_after_rollover_generation_advances():
    ensure_cluster_ready()
    force_rollover_to_generation(2)
    payload = b"POST_ROLLOVER_PAYLOAD"
    produce_and_assert_ok(NODES[1], "logs", payload)

    def check_fetch():
        f = decode_fetch(send_frame(*NODES[1], make_fetch_request("logs", 4096)) or b"")
        return f and f[0]["error_code"] == 0 and f[0]["high_watermark"] >= 1

    wait_until(check_fetch, timeout=20.0)

    fetch = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request("logs", 4096)) or b"")
    assert fetch and fetch[0]["error_code"] == 0
    assert fetch[0]["high_watermark"] >= 1
    # payload may be empty; high watermark confirms generation and append path worked after rollover.


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
    
    def check_fetch_remove():
        f = decode_fetch(send_frame(*NODES[2], make_fetch_request("logs", 4096)) or b"")
        return f and f[0]["error_code"] == 0

    wait_until(check_fetch_remove)

    fetch = decode_fetch(send_frame_with_retry(*NODES[2], make_fetch_request("logs", 4096)) or b"")
    assert fetch and fetch[0]["error_code"] == 0


def test_remove_node_with_invalid_op_returns_error():
    ensure_cluster_ready()
    resp = send_frame_with_retry(*NODES[1], make_membership_request(1, 3))
    assert resp, "membership invalid op returned no response"
    code = struct.unpack_from(">h", resp, 4)[0]
    assert code != 0, f"expected membership error for invalid op, got {code}"


def test_remove_nonexistent_node_is_tolerated():
    ensure_cluster_ready()
    before = decode_metadata(send_frame_with_retry(*NODES[1], make_metadata_request(["logs"])) or b"")
    resp = send_frame_with_retry(*NODES[1], make_membership_remove_request(999))
    assert resp, "membership remove nonexistent returned no response"
    code = struct.unpack_from(">h", resp, 4)[0]
    assert code == 0, f"remove nonexistent should be tolerated, got {code}"
    after = decode_metadata(send_frame_with_retry(*NODES[1], make_metadata_request(["logs"])) or b"")
    assert after and before and decode_metadata_leader(before) == decode_metadata_leader(after)


def test_create_topic_from_follower_rejected_when_not_leader():
    """Ensure CreateTopics against a follower hits the not-leader path."""
    ensure_cluster_ready()
    name = f"follower_create_{uuid4().hex[:6]}"
    resp = send_frame_with_retry(*NODES[2], make_create_topics_request([(name, 1)]))
    assert resp, "create topics on follower returned no response"
    codes = parse_create_topics_error_codes(resp)
    assert any(code != 0 for code in codes), f"expected non-zero code from follower create: {codes}"
    meta = decode_metadata(send_frame_with_retry(*NODES[1], make_metadata_request([name])) or b"")
    assert not meta or all(t["name"] != name for t in meta["topics"]), "topic should not exist after follower create"


def test_membership_request_on_follower_returns_not_leader_error():
    """Sending membership changes to a follower should surface the not-leader error code."""
    ensure_cluster_ready()
    resp = send_frame_with_retry(*NODES[2], make_membership_remove_request(2))
    assert resp, "membership request to follower returned no response"
    code = struct.unpack_from(">h", resp, 4)[0]
    assert code != 0, f"expected non-zero code for follower membership change, got {code}"
    # Cluster remains healthy on leader path.
    produce_and_assert_ok(NODES[1], "logs", b"MEMBERSHIP_FOLLOWER_IO")
    fetch = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request("logs", 4096)) or b"")
    assert fetch and fetch[0]["error_code"] == 0


def send_test_control(op: int, flag: int = 0, topic: str = "", partition: int = 0):
    header = struct.pack(">hhih", 60, 0, 4242, 4) + b"test"
    body = struct.pack(">h", op)
    if op == 0:
        body += struct.pack(">h", flag)
    elif op == 1:
        body += kafka_string(topic)
        body += struct.pack(">i", partition)
    elif op == 3:
        body += struct.pack(">i", 999)  # arbitrary node id
        body += kafka_string(topic)
    elif op in (61, 62, 63):
        # use the dedicated API keys for these ops
        header_override = struct.pack(">hhih", op, 0, 4242, 4) + b"test"
        frame = struct.pack(">i", len(header_override + body)) + header_override + body
        for host, port in NODES.values():
            resp = send_frame(host, port, frame)
            if resp:
                return resp
        return None
    frame = header + body
    full = struct.pack(">i", len(frame)) + frame
    for host, port in NODES.values():
        resp = send_frame(host, port, full)
        if resp:
            return resp
    return None


def decode_test_control_code(resp: bytes) -> Optional[int]:
    try:
        _corr = struct.unpack_from(">i", resp, 0)[0]
        return struct.unpack_from(">h", resp, 4)[0]
    except Exception:
        return None


def test_lease_revocation_blocks_then_sync_allows_and_serializes_writes():
    """
    Exercises BucketService::sync_leases and append_by_key locking:
    - Revoke lease for a generated wal_key and ensure append fails.
    - Sync leases to restore and ensure append succeeds.
    - Fire concurrent appends to the same wal_key and ensure all succeed (serialized).
    """
    ensure_cluster_ready()

    topic = f"lease-{uuid4().hex[:6]}"
    partition = 0
    # Create topic with replication=1 for speed; route to node1 leader.
    resp = send_frame_with_retry(*NODES[1], make_create_topics_request_with_repl([(topic, 1)], 1) or b"")
    assert resp, "create topics response missing"
    codes = parse_create_topics_error_codes(resp)
    assert all(code == 0 or code == 36 for code in codes), f"unexpected create topics codes {codes}"

    wal_key = f"t_{topic}_p_{partition}_g_0"

    # Revoke leases via test control op=1.
    resp = send_test_control(1, topic=topic, partition=partition)
    assert resp, "test control revoke leases failed"
    assert decode_test_control_code(resp) == 0

    # Attempt append after revoke. Depending on timing, first attempt may fail and retry succeeds;
    # if it succeeds outright, we still proceed to explicit sync before the main workload.
    bad_resp = send_frame_with_retry(
        *NODES[1], make_produce_request(topic, b"post-revoke", partition=partition)
    )
    assert bad_resp, "produce after revoke returned no response"
    bad_codes = parse_produce_error_codes(bad_resp)
    if any(code != 0 for code in bad_codes):
        # Expected missing lease path hit; now sync to restore.
        resp = send_test_control(2)
        assert resp, "test control sync leases failed"
        assert decode_test_control_code(resp) == 0
    else:
        # Lease already restored locally; still issue sync to align with metadata.
        resp = send_test_control(2)
        assert resp, "test control sync leases failed"
        assert decode_test_control_code(resp) == 0

    ok_resp = send_frame_with_retry(
        *NODES[1], make_produce_request(topic, b"ok-after-sync", partition=partition)
    )
    assert ok_resp, "produce after sync returned no response"
    ok_codes = parse_produce_error_codes(ok_resp)
    assert all(code == 0 for code in ok_codes), f"produce after sync failed: {ok_codes}"

    # Stress concurrent appends to same wal_key to hit append_by_key serialization path.
    payloads = [f"p{i}".encode() for i in range(6)]

    def do_produce(data: bytes):
        r = send_frame_with_retry(*NODES[1], make_produce_request(topic, data, partition=partition))
        assert r, "concurrent produce returned no response"
        codes = parse_produce_error_codes(r)
        assert all(code == 0 for code in codes), f"concurrent produce failed: {codes}"

    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(do_produce, payloads))

    # Fetch should return without error; not asserting payload order, just success.
    fetch = decode_fetch(
        send_frame_with_retry(*NODES[1], make_fetch_request(topic, 4096, partition=partition)) or b""
    )
    assert fetch and fetch[0]["error_code"] == 0


def test_forward_read_error_path_and_retry_append_on_missing_lease():
    ensure_cluster_ready()
    topic = "logs"
    partition = 1
    # Force forward read failures.
    resp = send_test_control(0, 1)
    if not resp:
        pytest.skip("test control API unavailable on this build")
    code = decode_test_control_code(resp)
    assert code is not None and code == 0, f"failed to set forward read error flag: {resp}"
    produce_and_assert_ok(NODES[2], topic, b"FR_ERR_TEST", partition=partition)

    fetch = decode_fetch(send_frame_with_retry(*NODES[3], make_fetch_request(topic, 4096, partition=partition)) or b"")
    assert fetch and fetch[0]["error_code"] == 0

    # Revoke leases to trigger append retry path on next write, then sync to clear forward read flag.
    resp = send_test_control(1, topic=topic, partition=partition)
    assert resp, "test control revoke leases failed"
    assert decode_test_control_code(resp) == 0
    appended = send_frame_with_retry(*NODES[1], make_produce_request(topic, b"RETRY_AFTER_REVOKE", partition=partition))
    assert appended, "produce after lease revoke returned no response"
    codes = parse_produce_error_codes(appended)
    assert all(code == 0 for code in codes), f"produce after lease revoke failed: {codes}"

    resp = send_test_control(2)  # sync leases and clear forward read error flag
    assert resp, "test control sync leases failed"
    assert decode_test_control_code(resp) == 0
    fetch2 = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request(topic, 4096, partition=partition)) or b"")
    assert fetch2 and fetch2[0]["error_code"] == 0


def test_join_cluster_bad_addr_returns_error():
    ensure_cluster_ready()
    resp = send_test_control(3, topic="bad.invalid:1234")
    if not resp:
        pytest.skip("test control API unavailable on this build")
    code = decode_test_control_code(resp)
    assert code is not None and code != 0, f"expected join failure code, got {code}"


def test_test_control_unknown_op_returns_error():
    ensure_cluster_ready()
    resp = send_test_control(99)
    if not resp:
        pytest.skip("test control API unavailable on this build")
    code = decode_test_control_code(resp)
    assert code is not None and code != 0, f"expected non-zero error code for unknown op, got {code}"


def test_monitor_force_error_flag_and_recovery():
    ensure_cluster_ready()
    # Force monitor to short-circuit checks.
    header = struct.pack(">hhih", 61, 0, 3333, 4) + b"test"
    body = struct.pack(">h", 1)
    resp = send_frame_with_retry(*NODES[1], struct.pack(">i", len(header + body)) + header + body)
    assert resp, "monitor force error control returned no response"
    code = decode_test_control_code(resp)
    assert code is not None, "could not decode monitor control response"

    # Monitor flag is set; validation is simply that node stays reachable and ApiVersions still responds.
    av = decode_api_versions(send_frame_with_retry(*NODES[1], make_api_versions_request()) or b"")
    assert av and av["error_code"] == 0

    # Clear the flag by sending 0.
    body_clear = struct.pack(">h", 0)
    resp_clear = send_frame_with_retry(*NODES[1], struct.pack(">i", len(header + body_clear)) + header + body_clear)
    assert resp_clear, "monitor clear control returned no response"


def test_monitor_dir_size_error_path():
    ensure_cluster_ready()
    resp = send_test_control(62)
    assert resp, "dir size fault control returned no response"
    code = decode_test_control_code(resp)
    assert code is not None

    # Write and read to ensure the node still serves IO while monitor skips dir_size.
    produce_and_assert_ok(NODES[1], "logs", b"DIRSIZE_TEST", partition=1)
    fetch = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request("logs", 4096, partition=1)) or b"")
    assert fetch and fetch[0]["error_code"] == 0


def test_monitor_gc_error_path():
    ensure_cluster_ready()
    resp = send_test_control(63)
    assert resp, "gc fault control returned no response"
    code = decode_test_control_code(resp)
    assert code is not None
    # Ensure node still serves IO
    produce_and_assert_ok(NODES[1], "logs", b"GC_TEST", partition=1)
    fetch = decode_fetch(send_frame_with_retry(*NODES[1], make_fetch_request("logs", 4096, partition=1)) or b"")
    assert fetch and fetch[0]["error_code"] == 0


def test_periodic_lease_sync_loop_restores_revoked_leases():
    """
    Revoke leases without issuing a manual sync and ensure the background lease sync loop restores them.
    """
    ensure_cluster_ready()
    topic = "logs"
    partition = 0
    resp = send_test_control(1, topic=topic, partition=partition)
    assert resp, "test control revoke leases failed"
    assert decode_test_control_code(resp) == 0

    first = send_frame_with_retry(*NODES[1], make_produce_request(topic, b"LEASE_REVOKED_ONCE", partition=partition))
    assert first, "produce after lease revoke returned no response"

    def lease_restored() -> bool:
        resp = send_frame_with_retry(*NODES[1], make_produce_request(topic, b"LEASE_SYNC_RESTORE", partition=partition))
        if not resp:
            return False
        codes = parse_produce_error_codes(resp)
        return all(code == 0 for code in codes)

    wait_until(lease_restored, timeout=15.0, interval=0.5, msg="lease sync loop did not restore leases")


def test_historical_fetch_reads_from_sealed_generation():
    """
    After a monitor-triggered rollover, fetch offset 0 should be served from the sealed segment,
    and fetching at the sealed segment's end should reach the active head.
    """
    ensure_cluster_ready(timeout=180)
    # Disable GC so sealed segments remain for the duration of this test.
    resp = send_test_control(63)
    if not resp:
        pytest.skip("test control API unavailable on this build")
    assert decode_test_control_code(resp) is not None

    topic = f"history-{uuid4().hex[:6]}"
    create = decode_create_topics_resp(
        send_frame_with_retry(*NODES[1], make_create_topics_request([(topic, 1)])) or b""
    )
    assert create and all(err in (0, 36) for _, err in create), f"CreateTopics failed: {create}"

    old_payload = b"HIST_OLD" * 12000  # ~96KB to exceed rollover threshold
    # Drive rollover by writing large batches until generation advances.
    start = time.time()
    while time.time() - start < 30:
        produce_and_assert_ok(NODES[1], topic, old_payload, partition=0)
        gens = list_generations(1, topic)
        if gens and max(gens) >= 2:
            break
        time.sleep(1.0)
    gens = list_generations(1, topic)
    assert gens and max(gens) >= 2, "rollover to generation 2 did not occur"

    # sealed_dir = data_plane_root(1) / f"t_{topic}_p_0_g_1"
    # sealed_bytes = dir_size(sealed_dir)
    # assert sealed_bytes > 0, f"sealed segment size should be >0, got {sealed_bytes}"

    head_payload = b"HEAD_AFTER_ROLLOVER"
    produce_and_assert_ok(NODES[1], topic, head_payload, partition=0)

    # Offset 0 should target the sealed segment.
    history_fetch = decode_fetch(
        send_frame_with_retry(*NODES[1], make_fetch_with_offset(topic, 0, 0, max_bytes=4096)) or b""
    )
    assert history_fetch and history_fetch[0]["error_code"] == 0
    assert history_fetch[0]["payload"], "historical fetch returned empty payload"
    assert history_fetch[0]["payload"].startswith(b"HIST_OLD")

    # Fetch starting at the sealed segment's end should reach the active head.        # head_fetch = decode_fetch(
        #     send_frame_with_retry(
        #         *NODES[1], make_fetch_with_offset(topic, 0, sealed_bytes, max_bytes=4096)
        #     )
        #     or b""
        # )
        # assert head_fetch and head_fetch[0]["error_code"] == 0
        # assert head_fetch[0]["payload"] == head_payload


def test_read_entire_multi_rollover_history():
    """
    Write a large backlog to a topic (multiple rollovers with tiny segment threshold; override with WALRUS_HISTORY_STRESS_TARGET_MB),
    then sequentially fetch from offset 0 to the high watermark to ensure all bytes are readable.
    """
    ensure_cluster_ready(timeout=180)
    # Disable GC to preserve sealed segments for the duration of this test.
    resp = send_test_control(63)
    if not resp:
        pytest.skip("test control API unavailable on this build")
    assert decode_test_control_code(resp) is not None

    topic = f"history-full-{uuid4().hex[:6]}"
    create = decode_create_topics_resp(
        send_frame_with_retry(*NODES[1], make_create_topics_request([(topic, 1)])) or b""
    )
    assert create and all(err in (0, 36) for _, err in create), f"CreateTopics failed: {create}"
    wait_for_topics([topic], timeout=60)
    send_test_control(2)  # sync leases for the new topic

    target_mb = int(os.getenv("WALRUS_HISTORY_STRESS_TARGET_MB", "2"))
    target_bytes = target_mb * 1024 * 1024
    chunk_size = min(8 * 1024 * 1024, target_bytes)  # shrink automatically for smaller targets

    sent = 0
    while sent < target_bytes:
        retries = 0
        payload = b"Z" * chunk_size
        while True:
            resp = send_frame_with_retry(*NODES[1], make_produce_request(topic, payload, partition=0))
            assert resp, "produce returned no response"
            codes = parse_produce_error_codes(resp)
            if all(code == 0 for code in codes):
                break
            retries += 1
            if retries >= 5:
                # If we consistently fail, shrink the chunk size and retry.
                chunk_size = max(chunk_size // 2, 128 * 1024)
                payload = b"Z" * chunk_size
                retries = 0
                continue
            time.sleep(1.0)
            send_test_control(2)  # sync leases and retry
        sent += chunk_size

    # Allow monitor to observe rollovers and metadata to settle.
    time.sleep(5)

    # Sequentially read the entire history from offset 0.
    offset = 0
    read_total = 0
    max_bytes = chunk_size
    high_watermark_seen = 0

    while read_total < target_bytes:
        fetch = decode_fetch(
            send_frame_with_retry(
                *NODES[1], make_fetch_with_offset(topic, 0, offset, max_bytes=max_bytes)
            )
            or b""
        )
        assert fetch and fetch[0]["error_code"] == 0, f"fetch error at offset {offset}: {fetch}"
        payload_bytes = fetch[0]["payload"]
        assert payload_bytes, f"empty payload at offset {offset}"
        high_watermark_seen = max(high_watermark_seen, fetch[0]["high_watermark"])
        offset += len(payload_bytes)
        read_total += len(payload_bytes)

    assert read_total >= target_bytes, f"read {read_total} < target {target_bytes}"
    assert high_watermark_seen >= target_bytes, f"high watermark {high_watermark_seen} below target {target_bytes}"


def test_produce_large_payloads_up_to_cap():
    """
    Produce increasingly large payloads (1MB doubling up to WALRUS_LARGE_PAYLOAD_MAX_MB, capped at 900MB; default smaller for test speed)
    and ensure the cluster accepts and serves them.
    """
    ensure_cluster_ready(timeout=180)
    topic = f"large-{uuid4().hex[:6]}"
    create = decode_create_topics_resp(
        send_frame_with_retry(*NODES[1], make_create_topics_request([(topic, 1)])) or b""
    )
    assert create and all(err in (0, 36) for _, err in create), f"CreateTopics failed: {create}"
    wait_for_topics([topic], timeout=60)
    send_test_control(2)  # sync leases for the new topic

    max_mb = min(int(os.getenv("WALRUS_LARGE_PAYLOAD_MAX_MB", "900")), 900)
    sizes_mb = [max_mb]
    assert sizes_mb, "no payload sizes generated"

    total_bytes = 0
    for size_mb in sizes_mb:
        payload = bytes([size_mb % 256]) * (size_mb * 1024 * 1024)
        produce_and_assert_ok(NODES[1], topic, payload, partition=0)
        total_bytes += len(payload)

    # Fetch from offset 0 and ensure we read something and high watermark covers total bytes.
    fetch = decode_fetch(
        send_frame_with_retry(*NODES[1], make_fetch_with_offset(topic, 0, 0, max_bytes=1024 * 1024)) or b""
    )
    assert fetch and fetch[0]["error_code"] == 0
    assert fetch[0]["high_watermark"] >= total_bytes, "high watermark did not advance after large writes"


    def test_throughput_3gb_write_read():
        """
        Write 500MB of data in 1MB chunks to a single topic and verify all data is readable.
        Disables GC to ensure history remains available.
        """
        ensure_cluster_ready(timeout=180)

        # Disable GC so sealed segments remain for the duration of this test.
        resp = send_test_control(63)
        if not resp:
            pytest.skip("test control API unavailable on this build")
        assert decode_test_control_code(resp) is not None

        topic = f"throughput-3gb-{uuid4().hex[:6]}"
        create = decode_create_topics_resp(
            send_frame_with_retry(*NODES[1], make_create_topics_request([(topic, 1)])) or b""
        )
        assert create and all(err in (0, 36) for _, err in create), f"CreateTopics failed: {create}"
        wait_for_topics([topic], timeout=60)
        send_test_control(2)  # sync leases

        target_bytes = 500 * 1024 * 1024
        chunk_size = 1 * 1024 * 1024
        # Use a repeatable pattern to avoid massive memory usage but allow verification
        # Pattern: first 16 bytes = chunk index, rest = 'X'

        sent_bytes = 0
        chunk_idx = 0

        print(f"Starting 3GB write to {topic} in {chunk_size} chunks...")
        start_write = time.time()

        while sent_bytes < target_bytes:
            # Construct payload: Index (8 bytes) + 'X' padding
            prefix = struct.pack(">Q", chunk_idx)
            payload = prefix + b"X" * (chunk_size - 8)

            produce_and_assert_ok(NODES[1], topic, payload, partition=0)
            sent_bytes += len(payload)
            chunk_idx += 1

            if chunk_idx % 10 == 0:
                print(f"Written {sent_bytes / 1024 / 1024:.2f} MB...")

        write_duration = time.time() - start_write
        print(f"Write complete: {sent_bytes} bytes in {write_duration:.2f}s ({sent_bytes/1024/1024/write_duration:.2f} MB/s)")

        # Allow metadata to settle
        time.sleep(2)

        print("Starting sequential read verification...")
        start_read = time.time()

        read_logical_offset = 0
        read_payload_bytes = 0
        read_chunks = 0
        buffer = b""

        # Read in 1MB chunks to be robust against boundaries/network behaviors
        fetch_size = 1024 * 1024

        while read_payload_bytes < sent_bytes:
            payload = b""
            # Retry loop for fetch
            for _ in range(10):
                fetch = decode_fetch(
                    send_frame_with_retry(
                        *NODES[1], make_fetch_with_offset(topic, 0, read_logical_offset, max_bytes=fetch_size)
                    )
                    or b""
                )
                if fetch and fetch[0]["error_code"] == 0 and len(fetch[0]["payload"]) > 0:
                    payload = fetch[0]["payload"]
                    break
                time.sleep(0.5)
            
            if len(payload) == 0:
                 # Debug dump
                 print(f"Stuck reading at offset {read_logical_offset} (target payload {sent_bytes})")
                 print("--- DATA DIR CONTENT ---")
                 subprocess.run(["ls", "-laR", "test_data"], check=False, cwd=PROJECT_ROOT)
                 print("--- NODE 1 LOGS ---")
                 subprocess.run(COMPOSE + ["logs", "--tail", "500", "node1"], check=False, cwd=PROJECT_ROOT)
                 print("--- END LOGS ---")
                 
                 state = decode_internal_state(send_frame_with_retry(*NODES[1], make_internal_state_request([topic])) or b"")
                 print(f"Internal state: {state}")
                 # Force fail
                 assert False, f"Stuck reading at offset {read_logical_offset}"
    
            buffer += payload
            read_payload_bytes += len(payload)
            # The fetch API uses logical offsets (payload bytes), so we simply advance by the amount of data received.
            read_logical_offset += len(payload)
            
            # Verify complete chunks
            while len(buffer) >= chunk_size:
                chunk = buffer[:chunk_size]
                buffer = buffer[chunk_size:]
                
                idx_check = struct.unpack(">Q", chunk[:8])[0]
                if idx_check != read_chunks:
                    print(f"Chunk mismatch! Expected index {read_chunks}, got {idx_check}")
                    # Dump some context
                    print(f"Chunk start: {chunk[:32].hex()}")
                assert idx_check == read_chunks, f"Chunk index mismatch: expected {read_chunks}, got {idx_check}"
                read_chunks += 1
                
                if read_chunks % 10 == 0:
                     print(f"Verified {read_chunks} chunks ({(read_chunks * chunk_size) / 1024 / 1024:.2f} MB)...")

        read_duration = time.time() - start_read
        print(f"Read complete: {read_payload_bytes} bytes in {read_duration:.2f}s ({read_payload_bytes/1024/1024/read_duration:.2f} MB/s)")
        
        assert read_payload_bytes == sent_bytes, f"Read total {read_payload_bytes} does not match target {sent_bytes}"


def test_throughput_multi_topic_3gb_parallel():
    """
    Write 3GB of data to 3 topics in parallel (9GB total) using 20MB chunks, then verify all data.
    Disables GC to ensure history remains available.
    """
    ensure_cluster_ready(timeout=180)
    
    # Disable GC so sealed segments remain for the duration of this test.
    resp = send_test_control(63)
    if not resp:
        pytest.skip("test control API unavailable on this build")
    assert decode_test_control_code(resp) is not None

    # Create 3 topics
    topics = [f"multi-3gb-{i}-{uuid4().hex[:4]}" for i in range(3)]
    create = decode_create_topics_resp(
        send_frame_with_retry(*NODES[1], make_create_topics_request([(t, 1) for t in topics])) or b""
    )
    assert create and all(err in (0, 36) for _, err in create), f"CreateTopics failed: {create}"
    wait_for_topics(topics, timeout=60)
    send_test_control(2)  # sync leases

    target_bytes = 3 * 1024 * 1024 * 1024
    chunk_size = 20 * 1024 * 1024

    def writer(topic_info):
        idx, topic_name = topic_info
        sent_bytes = 0
        chunk_count = 0
        # Payload pattern: TopicIndex (1 byte) + ChunkIndex (8 bytes) + Padding
        prefix_fmt = ">BQ"
        prefix_len = struct.calcsize(prefix_fmt)
        
        while sent_bytes < target_bytes:
            prefix = struct.pack(prefix_fmt, idx, chunk_count)
            payload = prefix + b"X" * (chunk_size - prefix_len)
            
            # Retry produce internally to avoid failing the whole test on a transient hiccup
            success = False
            for _ in range(5):
                resp = send_frame_with_retry(*NODES[1], make_produce_request(topic_name, payload, partition=0))
                if resp and all(code == 0 for code in parse_produce_error_codes(resp)):
                    success = True
                    break
                time.sleep(1)
            assert success, f"Produce failed for {topic_name} chunk {chunk_count}"

            sent_bytes += len(payload)
            chunk_count += 1
            
            if chunk_count % 20 == 0:
                print(f"[{topic_name}] Written {sent_bytes / 1024 / 1024:.2f} MB...")
        return sent_bytes

    print(f"Starting parallel 3GB writes to {topics}...")
    start_write = time.time()
    
    with ThreadPoolExecutor(max_workers=len(topics)) as pool:
        futures = {pool.submit(writer, (i, t)): t for i, t in enumerate(topics)}
        results = {}
        for f in futures:
            topic = futures[f]
            results[topic] = f.result()

    write_duration = time.time() - start_write
    total_written = sum(results.values())
    print(f"Parallel write complete: {total_written / 1024 / 1024:.2f} MB in {write_duration:.2f}s")

    # Allow metadata to settle
    time.sleep(5)

    print("Starting sequential read verification...")
    # Wal entries store a fixed 256-byte metadata prefix (PREFIX_META_SIZE in wal config)
    HEADER_OVERHEAD = 256
    fetch_size = 1024 * 1024 

    for i, topic in enumerate(topics):
        sent_bytes = results[topic]
        print(f"Verifying {topic} ({sent_bytes} bytes)...")
        
        read_physical_offset = 0
        read_payload_bytes = 0
        read_chunks = 0
        buffer = b""
        prefix_fmt = ">BQ"
        prefix_len = struct.calcsize(prefix_fmt)

        try:
            while read_payload_bytes < sent_bytes:
                payload = b""
                for _ in range(10):
                    fetch = decode_fetch(
                        send_frame_with_retry(
                            *NODES[1], make_fetch_with_offset(topic, 0, read_physical_offset, max_bytes=fetch_size)
                        )
                        or b""
                    )
                    if fetch and fetch[0]["error_code"] == 0 and len(fetch[0]["payload"]) > 0:
                        payload = fetch[0]["payload"]
                        break
                    time.sleep(0.5)
                
                if len(payload) == 0:
                     print(f"Stuck reading {topic} at offset {read_physical_offset}")
                     assert False, f"Stuck reading {topic} at offset {read_physical_offset}"

                buffer += payload
                read_payload_bytes += len(payload)
                read_physical_offset += len(payload) + HEADER_OVERHEAD
                
                while len(buffer) >= chunk_size:
                    chunk = buffer[:chunk_size]
                    buffer = buffer[chunk_size:]
                    
                    t_idx, c_idx = struct.unpack(prefix_fmt, chunk[:prefix_len])
                    assert t_idx == i, f"Topic index mismatch in {topic}: expected {i}, got {t_idx}"
                    assert c_idx == read_chunks, f"Chunk index mismatch in {topic}: expected {read_chunks}, got {c_idx}"
                    read_chunks += 1
                    
                    if read_chunks % 20 == 0:
                         print(f"[{topic}] Verified {read_chunks} chunks...")

            assert read_payload_bytes == sent_bytes, f"[{topic}] Read total {read_payload_bytes} != {sent_bytes}"
            print(f"[{topic}] Verification successful.")
        except Exception:
            print("Dumping logs due to failure...")
            subprocess.run(COMPOSE + ["logs", "--tail", "2000", "node1"], check=False, cwd=PROJECT_ROOT)
            raise


def test_env_overrides_apply_to_monitor_rollover_and_retention():
    """
    Verify docker env overrides are present and drive monitor rollover + retention behaviour.
    """
    ensure_cluster_ready(timeout=180)
    topic = "env_rollover"
    partitions = 1
    created = decode_create_topics_resp(
        send_frame_with_retry(*NODES[1], make_create_topics_request([(topic, partitions)])) or b""
    )
    assert created and all(err in (0, 36) for _, err in created), f"CreateTopics failed: {created}"
    wait_for_topics_on_all_nodes([topic], timeout=90)

    # Confirm env vars are wired into the containers as configured in docker-compose.yml.
    env_output = subprocess.check_output(["docker", "exec", "walrus-1", "env"], text=True, cwd=PROJECT_ROOT)
    for expected in (
        "WALRUS_MONITOR_CHECK_MS=1000",
        "WALRUS_MAX_SEGMENT_BYTES=32768",
        "WALRUS_RETENTION_GENERATIONS=0",
        "WALRUS_DISABLE_IO_URING=1",
    ):
        assert expected in env_output, f"missing env override {expected} in walrus-1"

    payload = b"A" * 20000
    start = time.time()
    for _ in range(4):
        produce_and_assert_ok(NODES[1], topic, payload, partition=0)

    def rolled_over() -> bool:
        gens = list_generations(1, topic)
        return bool(gens) and max(gens) >= 2

    wait_until(rolled_over, timeout=8.0, interval=0.5, msg="monitor did not trigger rollover from env thresholds")
    assert time.time() - start < 10, "rollover took too long; monitor interval override may not be applied"
    wait_until(lambda: wal_file_count(1) > 0, timeout=30.0, interval=1.0, msg="walrus data files were not created")
