#!/usr/bin/env python3
import socket
import sys
import time

def wait_for_ports(ports, host="localhost", timeout=90):
    deadline = time.time() + timeout

    def ready(addr):
        s = socket.socket()
        s.settimeout(1.5)
        try:
            s.connect(addr)
            return True
        except Exception:
            return False
        finally:
            s.close()

    while time.time() < deadline:
        if all(ready((host, p)) for p in ports):
            print("Cluster ports ready")
            return 0
        time.sleep(1)

    print(f"Cluster did not become ready within {timeout}s", file=sys.stderr)
    return 1


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: wait_for_ports.py <port> [<port> ...]", file=sys.stderr)
        sys.exit(1)
    ports = [int(p) for p in sys.argv[1:]]
    sys.exit(wait_for_ports(ports))
