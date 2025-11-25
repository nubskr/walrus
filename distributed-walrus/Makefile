
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

cluster-clean:
	rm -rf ./test_data/* ./test_data_rollover/*

.PHONY: cluster-test-logs
cluster-test-logs: cluster-clean
	python3 ./scripts/logging_smoke_test.py

.PHONY: cluster-test-rollover
cluster-test-rollover: cluster-clean
	python3 ./scripts/rollover_read_test.py

.PHONY: cluster-test-resilience
cluster-test-resilience: cluster-clean
	python3 ./scripts/resilience_test.py

.PHONY: cluster-test-recovery
cluster-test-recovery: cluster-clean
	python3 ./scripts/recovery_test.py

.PHONY: cluster-test-stress
cluster-test-stress: cluster-clean
	python3 ./scripts/stress_test.py

.PHONY: cluster-test-multi-topic
cluster-test-multi-topic: cluster-clean
	python3 ./scripts/multi_topic_stress_test.py

.PHONY: test
test: cluster-test-logs cluster-test-rollover cluster-test-resilience cluster-test-recovery cluster-test-stress cluster-test-multi-topic
