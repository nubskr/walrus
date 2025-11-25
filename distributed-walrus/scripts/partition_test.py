#!/usr/bin/env python3
import json
import socket
import struct
import subprocess
import time
import random
import sys
from contextlib import closing

# Configuration
TOPIC = "partition_test_topic"
NODES = [1, 2, 3]
PORT_MAP = {1: 10091, 2: 10092, 3: 10093}
CONTAINER_MAP = {1: "walrus-1", 2: "walrus-2", 3: "walrus-3"}
NETWORK_NAME = "walrus-test_walrus-net"

def send_cmd(host, port, cmd, timeout=30):
    try:
        with closing(socket.create_connection((host, port), timeout=timeout)) as sock:
            data = cmd.encode()
            sock.sendall(struct.pack("<I", len(data)))
            sock.sendall(data)
            # read len
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

def get_leader_id():
    # Ask all nodes, see who they think is leader.
    # Ideally they agree.
    votes = {}
    for n in NODES:
        resp = send_cmd("localhost", PORT_MAP[n], "METRICS")
        if resp and resp.startswith("{"):
            try:
                m = json.loads(resp)
                l = m.get("current_leader")
                if l:
                    votes[l] = votes.get(l, 0) + 1
            except:
                pass
    if not votes:
        return None
    # Return the one with most votes
    return max(votes, key=votes.get)

def partition_node(node_id):
    container = CONTAINER_MAP[node_id]
    print(f"[Partition] Disconnecting {container} from {NETWORK_NAME}...")
    subprocess.call(
        ["docker", "network", "disconnect", NETWORK_NAME, container],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

def heal_node(node_id):
    container = CONTAINER_MAP[node_id]
    print(f"[Partition] Reconnecting {container} to {NETWORK_NAME}...")
    subprocess.call(
        ["docker", "network", "connect", NETWORK_NAME, container],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

def main():
    print("--- Starting Partition Tolerance (Fencing) Test ---")
    
    # Setup
    subprocess.check_call(["docker", "compose", "-p", "walrus-test", "-f", "docker-compose.yml", "-f", "docker-compose.test.yml", "down", "-v"])
    subprocess.check_call(["docker", "rm", "-f", "walrus-1", "walrus-2", "walrus-3"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.check_call(["docker", "compose", "-p", "walrus-test", "-f", "docker-compose.yml", "-f", "docker-compose.test.yml", "up", "--build", "-d"])
    
    try:
        print("Waiting for cluster init...")
        time.sleep(10)
        
        # 1. Register Topic
        leader = get_leader_id()
        if not leader:
            raise RuntimeError("No leader found at startup")
        print(f"Initial leader is Node {leader}")
        
        resp = send_cmd("localhost", PORT_MAP[leader], f"REGISTER {TOPIC}")
        if not resp or not resp.startswith("OK"):
            raise RuntimeError("Failed to register topic")
            
        # 2. Write some data to ensure it works
        resp = send_cmd("localhost", PORT_MAP[leader], f"PUT {TOPIC} before-split")
        if not resp or not resp.startswith("OK"):
            raise RuntimeError("Failed initial write")
        print("Initial write successful")
        
        # 3. Partition the leader
        old_leader = leader
        partition_node(old_leader)
        
        print(f"Partitioned Node {old_leader}. Waiting for new election...")
        time.sleep(15) # Wait for election timeout (approx 10s max in current config?)
        
        # 4. Verify we have a NEW leader among the remaining nodes
        remaining_nodes = [n for n in NODES if n != old_leader]
        new_leader = None
        for _ in range(10):
            candidate = get_leader_id()
            if candidate and candidate != old_leader and candidate in remaining_nodes:
                new_leader = candidate
                break
            time.sleep(1)
            
        if not new_leader:
            raise RuntimeError(f"Cluster failed to elect new leader. Visible: {candidate}")
        print(f"New leader is Node {new_leader}")
        
        # 5. Write to NEW leader (should succeed)
        resp = send_cmd("localhost", PORT_MAP[new_leader], f"PUT {TOPIC} during-split-new")
        if not resp or not resp.startswith("OK"):
            raise RuntimeError("Failed write to new leader")
        print("Write to new leader successful")
        
        # 6. Attempt write to OLD leader (should FAIL or TIMEOUT)
        # Note: We talk to localhost:port, which is mapped to the container. 
        # Even if the container is disconnected from the docker network, 
        # the port mapping on host might still accept TCP connect, 
        # but the node inside can't talk to peers.
        print(f"Attempting write to OLD leader Node {old_leader} (expecting failure)...")
        resp = send_cmd("localhost", PORT_MAP[old_leader], f"PUT {TOPIC} split-brain-write", timeout=3)
        
        if resp and resp.startswith("OK"):
            raise RuntimeError("VIOLATION: Old leader accepted write while partitioned!")
        else:
            print(f"Old leader correctly rejected/timed-out write. Resp: {resp}")

        # 7. Heal partition
        heal_node(old_leader)
        print("Healed partition. Waiting for convergence...")
        time.sleep(10)
        
        # 8. Verify Old Leader steps down / syncs
        # It might take a moment.
        final_leader = get_leader_id()
        print(f"Final leader is Node {final_leader}")
        
        # 9. Verify Consistency (Read all data)
        # We expect: "before-split", "during-split-new". 
        # "split-brain-write" should NOT be there.
        
        print("Verifying data consistency...")
        messages = []
        # Read until EMPTY
        offset = 0
        # We need to handle the segment rollover if it happened? 
        # Unlikely for just 2 messages.
        # Let's try reading from the current leader
        
        # We'll fetch sequentially
        # We assume everything is in segment 1 for this small test
        # If rollover happened, we'd need more complex logic, but for 2 msgs, seg 1 is fine.
        
        cursor = 0
        tries = 0
        while tries < 10:
            resp = send_cmd("localhost", PORT_MAP[final_leader], f"FETCH {TOPIC} 1 {cursor}")
            if resp and resp.startswith("OK "):
                parts = resp.split(" ", 2)
                next_off = int(parts[1])
                payload = parts[2]
                messages.append(payload)
                cursor = next_off
            elif resp == "EMPTY":
                break
            else:
                time.sleep(1)
                tries += 1
        
        print(f"Messages found: {messages}")
        
        if "before-split" not in messages:
            raise RuntimeError("Missing 'before-split'")
        if "during-split-new" not in messages:
            raise RuntimeError("Missing 'during-split-new'")
        if "split-brain-write" in messages:
            raise RuntimeError("VIOLATION: Split brain write was committed!")
            
        print("PASSED: Partition Tolerance Test")

    finally:
        # Cleanup: Ensure network is connected so down works cleanly? 
        # Actually `down -v` usually handles it, but reconnecting is safer to avoid dangling endpoints
        try:
            subprocess.call(["docker", "network", "connect", NETWORK_NAME, "walrus-1"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.call(["docker", "network", "connect", NETWORK_NAME, "walrus-2"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.call(["docker", "network", "connect", NETWORK_NAME, "walrus-3"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except:
            pass
            
        subprocess.check_call(["docker", "compose", "-p", "walrus-test", "-f", "docker-compose.yml", "-f", "docker-compose.test.yml", "down", "-v"])

if __name__ == "__main__":
    main()
