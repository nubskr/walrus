PYTHON ?= $(VENV)/bin/python
PYTEST ?= $(PYTHON) -m pytest
PIP ?= $(PYTHON) -m pip
UV ?= uv
UV_VENV_FLAGS ?= --if-exists reuse
VENV ?= .venv
PYFLAGS ?= -vv

# Run each cluster test individually to keep runs isolated.
TESTS := \
	test_write_forwarding \
	test_metadata_connectivity \
	test_followers_forward_produce_to_leader \
	test_metadata_filtered_request_returns_only_logs \
	test_api_versions_support_known_keys \
	test_produce_unknown_topic_returns_error_and_no_disk_change \
	test_produce_invalid_partition_returns_error_and_no_disk_change \
	test_metadata_unfiltered_returns_logs_topic \
	test_fetch_from_follower_reads_latest_entry \
	test_fetch_payload_and_offsets_from_leader \
	test_create_topic_and_roundtrip_partition_zero \
	test_fetch_offset_strictness_with_high_watermark \
	test_fetch_unknown_topic_returns_error \
	test_fetch_invalid_partition_returns_error \
	test_partition_one_produce_and_fetch \
	test_restart_preserves_data_and_leadership \
	test_monitor_rollover_and_gc_removes_old_generation \
	test_internal_state_reports_leader_and_generation \
	test_hot_join_node4_reports_metadata \
	test_fetch_respects_offsets_and_payloads_from_leader \
	test_leader_failover_timeout_then_recovery \
	test_leader_pause_causes_timeout_and_resumes \
	test_full_cluster_restart_preserves_data \
	test_follower_restart_catches_up_and_reads \
	test_retention_gc_prunes_old_generations \
	test_hot_join_rejoin_promotes_node4 \
	test_remove_node_from_membership_and_continue_io

.PHONY: deps all-tests $(TESTS)

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
	@for t in $(TESTS); do \
		echo "Running $$t"; \
		$(MAKE) $$t || exit $$?; \
	done

$(TESTS): deps
	@$(PYTEST) tests/test_cluster.py::$@ $(PYFLAGS)
