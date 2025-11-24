
cluster-bootstrap: cluster-up
	@echo "Waiting for cluster ports 9091-9093 to come up..."
	@python - <<-'PY'
		import socket
		import time

		nodes = [("localhost", 9091), ("localhost", 9092), ("localhost", 9093)]
		deadline = time.time() + 90

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
		    if all(ready(n) for n in nodes):
		        print("Cluster ports ready")
		        raise SystemExit(0)
		    time.sleep(1)

		raise SystemExit("Cluster did not become ready within timeout")
	PY

cluster-up:
	docker compose -f docker-compose.yml up --build -d

cluster-down:
	docker compose -f docker-compose.yml down -v

cluster-restart:
	$(MAKE) cluster-down
	$(MAKE) cluster-up

cluster-logs:
	docker compose -f docker-compose.yml logs -f