PYTHON ?= $(VENV)/bin/python
PYTEST ?= $(PYTHON) -m pytest
PIP ?= $(PYTHON) -m pip
UV ?= uv
UV_VENV_FLAGS ?= --if-exists reuse
VENV ?= .venv
PYFLAGS ?= -vv
PYTEST_MARK_EXPR ?= not soak
SKIP_BUILD ?= 1
SOAK_LONG_DURATION_SEC ?= 1200
SOAK_LONG_CLIENTS ?= 6

# Run each cluster test individually to keep runs isolated.
TESTS := \
	test_write_forwarding \
	test_metadata_connectivity \
	test_followers_forward_produce_to_leader \
	test_metadata_filtered_request_returns_only_logs \
	test_metadata_filtered_unknown_topic_returns_empty \
	test_api_versions_support_known_keys \
	test_produce_unknown_topic_returns_error_and_no_disk_change \
	test_produce_invalid_partition_returns_error_and_no_disk_change \
	test_metadata_unfiltered_returns_logs_topic \
	test_fetch_from_follower_reads_latest_entry \
	test_fetch_payload_and_offsets_from_leader \
	test_create_topic_and_roundtrip_partition_zero \
	test_fetch_offset_strictness_with_high_watermark \
	test_fetch_with_too_small_max_bytes_returns_empty_payload \
	test_fetch_unknown_topic_returns_error \
	test_fetch_invalid_partition_returns_error \
	test_create_topic_invalid_replication_factor_returns_error \
	test_create_topic_invalid_partitions_returns_error \
	test_create_topic_from_follower_rejected_when_not_leader \
	test_create_topic_duplicate_rejected \
	test_partition_one_produce_and_fetch \
	test_restart_preserves_data_and_leadership \
	test_monitor_rollover_and_gc_removes_old_generation \
	test_internal_state_reports_leader_and_generation \
	test_internal_state_filtered_unknown_topic_returns_empty \
	test_hot_join_node4_reports_metadata \
	test_fetch_respects_offsets_and_payloads_from_leader \
	test_leader_failover_timeout_then_recovery \
	test_follower_fetch_during_leader_pause_returns_high_watermark \
	test_leader_pause_causes_timeout_and_resumes \
	test_full_cluster_restart_preserves_data \
	test_follower_restart_catches_up_and_reads \
	test_retention_gc_prunes_old_generations \
	test_produce_after_rollover_generation_advances \
	test_hot_join_rejoin_promotes_node4 \
	test_remove_node_from_membership_and_continue_io \
	test_membership_request_on_follower_returns_not_leader_error \
	test_remove_node_with_invalid_op_returns_error \
	test_remove_nonexistent_node_is_tolerated \
	test_forward_read_error_path_and_retry_append_on_missing_lease \
	test_join_cluster_bad_addr_returns_error \
	test_test_control_unknown_op_returns_error \
	test_monitor_force_error_flag_and_recovery \
	test_monitor_dir_size_error_path \
	test_monitor_gc_error_path \
	test_lease_revocation_blocks_then_sync_allows_and_serializes_writes \
	test_malformed_produce_frame_returns_error \
	test_negative_record_set_size_rejected \
	test_follower_fetch_survives_monitor_rollover_and_gc \
	test_join_cluster_duplicate_node_id_is_tolerated_and_cluster_continues_io \
	test_periodic_lease_sync_loop_restores_revoked_leases \
	test_env_overrides_apply_to_monitor_rollover_and_retention

.PHONY: deps all-tests all-tests-one-by-one soak-test soak-test-batch soak-test-regular soak-tests soak-test-long soak-test-20m cluster-up cluster-down cluster-restart cluster-logs cluster-bootstrap $(TESTS)

deps: $(VENV)/.deps-installed

$(VENV)/.deps-installed:
	@if command -v $(UV) >/dev/null 2>&1; then \
		echo "Using uv to manage virtualenv..."; \
		$(UV) venv $(UV_VENV_FLAGS) $(VENV); \
		$(UV) pip install --python $(PYTHON) -r tests/requirements.txt; \
	else \
		echo "uv not found; falling back to python -m venv"; \
		python3 -m venv $(VENV); \
		$(PIP) install -r tests/requirements.txt; \
	fi
	@touch $(VENV)/.deps-installed

all-tests: deps
	@SKIP_BUILD=$(SKIP_BUILD) $(PYTEST) tests/test_cluster.py -m "$(PYTEST_MARK_EXPR)" $(PYFLAGS)

# Original behaviour: run each test separately (slower; many container restarts).
all-tests-one-by-one: deps
	@for t in $(TESTS); do \
		echo "Running $$t"; \
		$(MAKE) $$t || exit $$?; \
	done

$(TESTS): deps
	@SKIP_BUILD=$(SKIP_BUILD) $(PYTEST) tests/test_cluster.py::$@ $(PYFLAGS)

soak-test-batch: deps
	@SKIP_BUILD=$(SKIP_BUILD) $(PYTEST) tests/test_cluster.py::test_soak_multi_client_produce_and_fetch_read_your_writes -m soak $(PYFLAGS)

soak-test-regular: deps
	@SKIP_BUILD=$(SKIP_BUILD) $(PYTEST) tests/test_cluster.py::test_soak_parallel_produce_multi_topics_and_cross_read -m soak $(PYFLAGS)

soak-test: soak-tests

soak-tests: deps
	@SKIP_BUILD=$(SKIP_BUILD) $(PYTEST) tests/test_cluster.py -m soak $(PYFLAGS)

soak-test-long: deps
	@SOAK_LONG_DURATION_SEC=$(SOAK_LONG_DURATION_SEC) SOAK_LONG_CLIENTS=$(SOAK_LONG_CLIENTS) \
	SKIP_BUILD=$(SKIP_BUILD) $(PYTEST) tests/test_cluster.py -m soak_long $(PYFLAGS)

soak-test-20m: soak-test-long

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
