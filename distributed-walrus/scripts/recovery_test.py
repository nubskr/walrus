#!/usr/bin/env python3
import json
import socket
import struct
import subprocess
import time
import random
import sys
import shutil
from pathlib import Path
from contextlib import closing

# Configuration
TOPIC = "recovery_test_topic"
NODES = [1, 2, 3]
PORT_MAP = {1: 10091, 2: 10092, 3: 10093}

def send_cmd(host, port, cmd):
    try:
        with closing(socket.create_connection((host, port), timeout=5)) as sock:
            data = cmd.encode()
            sock.sendall(struct.pack("<I", len(data)))
            sock.sendall(data)
            raw_len = sock.recv(4)
            if len(raw_len) < 4:
                return None
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
    except Exception as e:
        return None

def get_leader_port():
    for n in NODES:
        resp = send_cmd("localhost", PORT_MAP[n], "METRICS")
        if resp and resp.startswith("{"):
            try:
                m = json.loads(resp)
                l = m.get("current_leader")
                if l in PORT_MAP:
                    return PORT_MAP[l]
            except:
                pass
    return PORT_MAP[1] # Fallback

def main():
    print("--- Starting Full Cluster Recovery Test ---")
    for path in (Path("test_data"), Path("test_data_rollover")):
        if path.exists():
            shutil.rmtree(path)
    
    # Setup
    subprocess.check_call(["docker", "compose", "-p", "walrus-test", "-f", "docker-compose.yml", "-f", "docker-compose.test.yml", "down", "-v"])
    subprocess.check_call(["docker", "rm", "-f", "walrus-1", "walrus-2", "walrus-3"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.check_call(["docker", "compose", "-p", "walrus-test", "-f", "docker-compose.yml", "-f", "docker-compose.test.yml", "up", "--build", "-d"])
    
    try:
        print("Waiting for cluster init...")
        time.sleep(10)
        
        leader_port = get_leader_port()
        
        # 1. Register Topic
        print("Registering topic...")
        resp = send_cmd("localhost", leader_port, f"REGISTER {TOPIC}")
        if not resp or not resp.startswith("OK"):
            raise RuntimeError(f"Failed to register topic: {resp}")
            
        # 2. Write Pre-Crash Data
        print("Writing pre-crash data...")
        for i in range(10):
            resp = send_cmd("localhost", leader_port, f"PUT {TOPIC} msg-{i}")
            if not resp or not resp.startswith("OK"):
                raise RuntimeError(f"Failed to write msg-{i}")
        
        # 3. Full Cluster Shutdown
        print("Simulating full cluster crash (power loss)...")
        subprocess.check_call(["docker", "compose", "-p", "walrus-test", "-f", "docker-compose.yml", "-f", "docker-compose.test.yml", "stop"])
        
        time.sleep(5)
        
        # 4. Restart Cluster
        print("Restarting cluster...")
        subprocess.check_call(["docker", "compose", "-p", "walrus-test", "-f", "docker-compose.yml", "-f", "docker-compose.test.yml", "start"])
        
        print("Waiting for recovery...")
        time.sleep(15)
        
        # 5. Find new leader
        leader_port = get_leader_port()
        
        # 6. Verify Pre-Crash Data Persisted
        print("Verifying data persistence...")
        messages = []
        tries = 0
        
        # Attempt to read 10 messages
        while len(messages) < 10 and tries < 30:
            resp = send_cmd("localhost", leader_port, f"GET {TOPIC}")
            if resp and resp.startswith("OK "):
                payload = resp[3:]
                messages.append(payload)
            elif resp == "EMPTY":
                # Wait for data to be available/loaded? 
                # Or maybe we read it all?
                if len(messages) == 10:
                    break
                time.sleep(1)
                tries += 1
            else:
                print(f"Read error: {resp}")
                time.sleep(1)
                tries += 1
                leader_port = get_leader_port() # Re-check leader if connection failed
        
        print(f"Recovered {len(messages)} messages: {messages}")
        
        expected = [f"msg-{i}" for i in range(10)]
        if messages != expected:
            # It's possible we got duplicates if we retried or if the cursor behavior is weird?
            # But the loop above just appends.
            # Strict equality check.
            raise RuntimeError(f"Data mismatch! Expected {expected}, got {messages}")
            
        # 7. Verify System is still Writable
        print("Verifying write availability after recovery...")
        write_ok = False
        for _ in range(10):
            leader_port = get_leader_port()
            resp = send_cmd("localhost", leader_port, f"PUT {TOPIC} post-crash-msg")
            if resp and resp.startswith("OK"):
                write_ok = True
                break
            time.sleep(1)
        if not write_ok:
            raise RuntimeError("Failed to write post-crash message")
            
        # 8. Verify we can read the new message
        found_post = False
        for _ in range(5):
            resp = send_cmd("localhost", leader_port, f"GET {TOPIC}")
            if resp and resp.startswith("OK ") and "post-crash-msg" in resp:
                found_post = True
                break
            time.sleep(0.5)
            
        if not found_post:
             raise RuntimeError("Failed to read post-crash message")

        print("PASSED: Full Cluster Recovery Test")

    finally:
        subprocess.check_call(["docker", "compose", "-p", "walrus-test", "-f", "docker-compose.yml", "-f", "docker-compose.test.yml", "down", "-v"])

if __name__ == "__main__":
    main()
