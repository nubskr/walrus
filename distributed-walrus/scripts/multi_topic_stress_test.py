#!/usr/bin/env python3
import json
import socket
import struct
import subprocess
import time
import threading
import sys
import shutil
from pathlib import Path
from contextlib import closing

DURATION_SEC = 60
NUM_CLIENTS = 100
ENTRY_SIZE = 1024
NODES = [1, 2, 3]
PORT_MAP = {1: 10091, 2: 10092, 3: 10093}

class PersistentClient:
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=30)
    
    def send_cmd(self, cmd):
        data = cmd.encode()
        self.sock.sendall(struct.pack("<I", len(data)))
        self.sock.sendall(data)
        
        raw_len = self.sock.recv(4)
        if len(raw_len) < 4:
            raise RuntimeError("Socket closed")
        msg_len = struct.unpack("<I", raw_len)[0]
        
        chunks = []
        remaining = msg_len
        while remaining > 0:
            chunk = self.sock.recv(remaining)
            if not chunk:
                raise RuntimeError("Socket closed mid-body")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b".".join(chunks).decode()

    def close(self):
        try:
            self.sock.close()
        except:
            pass

def get_leader_port():
    for n in NODES:
        try:
            client = PersistentClient("localhost", PORT_MAP[n])
            resp = client.send_cmd("METRICS")
            client.close()
            if resp.startswith("{"):
                m = json.loads(resp)
                l = m.get("current_leader")
                if l in PORT_MAP:
                    return PORT_MAP[l]
        except:
            pass
    return PORT_MAP[1]

class TopicWorker(threading.Thread):
    def __init__(self, worker_id, leader_port, start_barrier, stop_event):
        super().__init__()
        self.worker_id = worker_id
        self.topic = f"stress_topic_{worker_id}"
        self.leader_port = leader_port
        self.start_barrier = start_barrier
        self.stop_event = stop_event
        self.payload_template = "x" * (ENTRY_SIZE - 20) # Reserve space for seq prefix
        self.acked_count = 0
        self.write_errors = 0
        self.read_errors = 0
        self.verified = False
        
    def run(self):
        self.start_barrier.wait()
        
        # 1. Register
        client = None
        for attempt in range(10):
            try:
                client = PersistentClient("localhost", self.leader_port)
                resp = client.send_cmd(f"REGISTER {self.topic}")
                if resp.startswith("OK"):
                    break
                print(f"[{self.topic}] Register failed (attempt {attempt + 1}): {resp}")
            except Exception as e:
                print(f"[{self.topic}] Register error (attempt {attempt + 1}): {e}")
            if client:
                client.close()
                client = None
            time.sleep(0.5)
        else:
            return

        # 2. Write Loop
        seq = 0
        while not self.stop_event.is_set():
            try:
                # Embed seq in payload for verification: "seq-000000:xxxx..."
                msg = f"seq-{seq:06d}:{self.payload_template}"
                cmd = f"PUT {self.topic} {msg}"
                resp = client.send_cmd(cmd)
                
                if resp.startswith("OK"):
                    seq += 1
                    self.acked_count = seq
                else:
                    self.write_errors += 1
                    # Backoff slightly on error
                    time.sleep(0.1)
            except Exception:
                self.write_errors += 1
                client.close()
                try:
                    client = PersistentClient("localhost", self.leader_port)
                except:
                    time.sleep(0.5)
        
        client.close()
        
    def verify(self):
        # 3. Read Verification
        # Create fresh connection for reading
        try:
            client = PersistentClient("localhost", self.leader_port)
            
            # We expect 'acked_count' messages, from 0 to acked_count-1
            for expected_seq in range(self.acked_count):
                resp = client.send_cmd(f"GET {self.topic}")
                
                if resp.startswith("OK "):
                    payload = resp[3:]
                    prefix = f"seq-{expected_seq:06d}:"
                    if not payload.startswith(prefix):
                        print(f"[{self.topic}] ORDER ERROR! Expected prefix {prefix}, got start of {payload[:20]}...")
                        return
                else:
                    print(f"[{self.topic}] Read error at seq {expected_seq}: {resp}")
                    self.read_errors += 1
                    return

            # Ensure no more data
            resp = client.send_cmd(f"GET {self.topic}")
            if resp != "EMPTY":
                 print(f"[{self.topic}] ERROR: Expected EMPTY, got {resp}")
                 return

            self.verified = True
            client.close()
        except Exception as e:
            print(f"[{self.topic}] Verify exception: {e}")

def wait_ports(addrs, deadline=120):
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

def main():
    print(f"--- Starting Multi-Topic ({NUM_CLIENTS}) Stress Test ---")
    for path in (Path("test_data"), Path("test_data_rollover")):
        if path.exists():
            shutil.rmtree(path)
    
    subprocess.check_call(["docker", "compose", "-p", "walrus-test", "-f", "docker-compose.yml", "-f", "docker-compose.test.yml", "down", "-v"])
    subprocess.check_call(["docker", "rm", "-f", "walrus-1", "walrus-2", "walrus-3"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.check_call(["docker", "compose", "-p", "walrus-test", "-f", "docker-compose.yml", "-f", "docker-compose.test.yml", "up", "--build", "-d"])
    
    try:
        print("Waiting for cluster init...")
        wait_ports([("localhost", 10091), ("localhost", 10092), ("localhost", 10093)], 180)
        
        leader_port = get_leader_port()
        print(f"Targeting Leader at port {leader_port}")
        
        barrier = threading.Barrier(NUM_CLIENTS + 1)
        stop_event = threading.Event()
        workers = [TopicWorker(i, leader_port, barrier, stop_event) for i in range(NUM_CLIENTS)]
        
        print(f"Spawning {NUM_CLIENTS} workers...")
        for w in workers:
            w.start()
            
        barrier.wait()
        start_time = time.time()
        print(f"Writing for {DURATION_SEC} seconds...")
        
        time.sleep(DURATION_SEC)
        stop_event.set()
        
        for w in workers:
            w.join()
            
        write_elapsed = time.time() - start_time
        total_writes = sum(w.acked_count for w in workers)
        total_write_errors = sum(w.write_errors for w in workers)
        
        print(f"\n--- Write Phase Complete ---")
        print(f"Total Writes: {total_writes}")
        print(f"Write Throughput: {total_writes / write_elapsed:.2f} ops/sec")
        print(f"Write Errors: {total_write_errors}")
        
        if total_writes == 0:
            raise RuntimeError("No messages written!")

        print("\n--- Starting Verification Phase ---")
        verify_start = time.time()
        
        # Run verification in parallel (using threads) or it will take forever
        # We can reuse the thread objects or logic, but let's just call verify on them sequentially for simplicity
        # actually 100 topics * N messages might be slow to verify sequentially.
        # Let's verify in parallel.
        
        verifiers = []
        for w in workers:
            t = threading.Thread(target=w.verify)
            t.start()
            verifiers.append(t)
            
        for t in verifiers:
            t.join()
            
        verify_elapsed = time.time() - verify_start
        print(f"Verification took {verify_elapsed:.2f} s")
        
        failures = [w for w in workers if not w.verified]
        
        if len(failures) > 0:
            print(f"FAILED: {len(failures)} topics failed verification.")
            raise RuntimeError("Verification failed")
            
        print("PASSED: Multi-Topic Stress Test")
        print(f"All {NUM_CLIENTS} topics verified consistent ordering.")

    finally:
        subprocess.check_call(["docker", "compose", "-p", "walrus-test", "-f", "docker-compose.yml", "-f", "docker-compose.test.yml", "down", "-v"])

if __name__ == "__main__":
    main()
