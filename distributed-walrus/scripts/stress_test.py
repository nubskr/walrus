#!/usr/bin/env python3
import json
import socket
import struct
import subprocess
import time
import threading
import sys
from contextlib import closing

TOPIC = "stress_test_topic"
DURATION_SEC = 60
NUM_THREADS = 16
ENTRY_SIZE = 1024  # 1KB payload
NODES = [1, 2, 3]
PORT_MAP = {1: 10091, 2: 10092, 3: 10093}

class PersistentClient:
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=10)
    
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
        return b"".join(chunks).decode()

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

class StressWorker(threading.Thread):
    def __init__(self, target_port, start_barrier):
        super().__init__()
        self.target_port = target_port
        self.start_barrier = start_barrier
        # Generate a 1KB payload string
        self.payload_utf8 = "x" * ENTRY_SIZE
        self.count = 0
        self.running = True
        self.errors = 0
        
    def run(self):
        client = None
        # Wait for all threads to be ready to hammer at once
        self.start_barrier.wait()
        
        while self.running:
            if client is None:
                try:
                    client = PersistentClient("localhost", self.target_port)
                except Exception:
                    time.sleep(0.1)
                    continue
            
            try:
                cmd = f"PUT {TOPIC} {self.payload_utf8}"
                resp = client.send_cmd(cmd)
                if resp.startswith("OK"):
                    self.count += 1
                else:
                    self.errors += 1
            except Exception:
                if client:
                    client.close()
                client = None
                self.errors += 1
                time.sleep(0.1)
        
        if client:
            client.close()

def main():
    print("--- Starting 1-Minute Stress Test ---")
    
    # Setup
    subprocess.check_call(["docker", "compose", "-p", "walrus-test", "-f", "docker-compose.yml", "-f", "docker-compose.test.yml", "down", "-v"])
    subprocess.check_call(["docker", "rm", "-f", "walrus-1", "walrus-2", "walrus-3"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.check_call(["docker", "compose", "-p", "walrus-test", "-f", "docker-compose.yml", "-f", "docker-compose.test.yml", "up", "--build", "-d"])
    
    try:
        print("Waiting for cluster init...")
        time.sleep(10)
        
        leader_port = get_leader_port()
        print(f"Targeting Leader at port {leader_port}")
        
        # Register topic
        c = PersistentClient("localhost", leader_port)
        resp = c.send_cmd(f"REGISTER {TOPIC}")
        print(f"Register topic response: {resp}")
        c.close()
        
        barrier = threading.Barrier(NUM_THREADS + 1)
        workers = [StressWorker(leader_port, barrier) for _ in range(NUM_THREADS)]
        
        print(f"Spawning {NUM_THREADS} threads sending {ENTRY_SIZE}B entries...")
        for w in workers:
            w.start()
            
        # Synchronize start
        barrier.wait()
        start_time = time.time()
        print(f"Stress test running for {DURATION_SEC} seconds...")
        
        time.sleep(DURATION_SEC)
        
        print("Stopping workers...")
        for w in workers:
            w.running = False
        for w in workers:
            w.join()
            
        elapsed = time.time() - start_time
        total_entries = sum(w.count for w in workers)
        total_errors = sum(w.errors for w in workers)
        total_bytes = total_entries * ENTRY_SIZE
        
        print("\n" + "="*40)
        print(f"RESULTS ({NUM_THREADS} threads, 1KB entries)")
        print(f"Total Time:    {elapsed:.2f} s")
        print(f"Total Writes:  {total_entries}")
        print(f"Total Errors:  {total_errors}")
        print(f"Throughput:    {total_entries / elapsed:.2f} ops/sec")
        print(f"Bandwidth:     {total_bytes / (1024*1024) / elapsed:.2f} MB/sec")
        print("="*40 + "\n")
        
        if total_entries == 0:
            raise RuntimeError("Zero entries written!")
            
    finally:
        subprocess.check_call(["docker", "compose", "-p", "walrus-test", "-f", "docker-compose.yml", "-f", "docker-compose.test.yml", "down", "-v"])

if __name__ == "__main__":
    main()
