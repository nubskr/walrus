.PHONY: help bench-writes bench-reads bench-scaling bench-batch-scaling show-writes show-reads show-scaling show-batch-writes show-batch-scaling live-writes live-scaling clean bench-walrus-vs-rocksdb
.PHONY: bench-writes-sync bench-reads-sync bench-scaling-sync bench-writes-fast bench-reads-fast bench-scaling-fast

WALRUS_CSV ?= walrus.csv
ROCKSDB_CSV ?= rocksdb.csv

help:
	@echo "Walrus Benchmarks"
	@echo "=================="
	@echo ""
	@echo "Benchmarks (Default: async fsync every 1000ms):"
	@echo "  bench-writes    Run write-only benchmark (2 min)"
	@echo "  bench-reads     Run read benchmark (1 min write + 2 min read)"
	@echo "  bench-scaling   Run scaling benchmark across thread counts"
	@echo "  bench-batch-scaling   Run batch scaling benchmark across thread counts"
	@echo "  bench-walrus-vs-rocksdb   Run Walrus + RocksDB WAL benchmarks and plot comparison"
	@echo ""
	@echo "Benchmarks (Sync each write - most durable, slowest):"
	@echo "  bench-writes-sync    Run write benchmark with sync-each"
	@echo "  bench-reads-sync     Run read benchmark with sync-each"
	@echo "  bench-scaling-sync   Run scaling benchmark with sync-each"
	@echo ""
	@echo "Benchmarks (Fast async - 100ms fsync interval):"
	@echo "  bench-writes-fast    Run write benchmark with 100ms fsync"
	@echo "  bench-reads-fast     Run read benchmark with 100ms fsync"
	@echo "  bench-scaling-fast   Run scaling benchmark with 100ms fsync"
	@echo ""
	@echo "Custom fsync schedule:"
	@echo "  FSYNC=<schedule> make bench-writes   # e.g., FSYNC=sync-each or FSYNC=500ms"
	@echo ""
	@echo "Storage backend (Linux only):"
	@echo "  BACKEND=fd make bench-writes       # force fd/io_uring backend (default)"
	@echo "  BACKEND=mmap make bench-writes     # force mmap backend"
	@echo ""
	@echo "Custom thread count (scaling benchmark only):"
	@echo "  THREADS=<range> make bench-scaling   # e.g., THREADS=16 or THREADS=2-8"
	@echo "  THREADS=<range> make bench-batch-scaling   # e.g., THREADS=8 or THREADS=4-16"
	@echo "  BATCH=<entries> make bench-batch-scaling   # override batch size (default 256)"
	@echo ""
	@echo "Visualization:"
	@echo "  show-writes     Show write benchmark results"
	@echo "  show-reads      Show read benchmark results"
	@echo "  show-scaling    Show scaling benchmark results"
	@echo "  show-batch-scaling Show batch scaling benchmark results"
	@echo "  live-writes     Live monitoring of write benchmark"
	@echo "  live-scaling    Live monitoring of scaling benchmark"
	@echo ""
	@echo "Utilities:"
	@echo "  clean          Remove all CSV output files"
	@echo ""
	@echo "Fsync Schedule Options:"
	@echo "  sync-each       Fsync after every write (slowest, most durable)"
	@echo "  async           Async fsync every 1000ms (default)"
	@echo "  <number>ms      Async fsync every N milliseconds (e.g., 500ms)"
	@echo "  <number>        Async fsync every N milliseconds (e.g., 500)"

# Benchmark targets (default: async 1000ms)
bench-writes:
	@echo "Running write benchmark (default: async 1000ms fsync)..."
	@if [ -n "$(FSYNC)" ]; then \
		echo "Using custom fsync schedule: $(FSYNC)"; \
		if [ -n "$(BACKEND)" ]; then \
			echo "Using storage backend: $(BACKEND)"; \
			WALRUS_BACKEND=$(BACKEND) WALRUS_FSYNC=$(FSYNC) cargo test --release --test multithreaded_benchmark_writes -- --nocapture; \
		else \
			WALRUS_FSYNC=$(FSYNC) cargo test --release --test multithreaded_benchmark_writes -- --nocapture; \
		fi; \
	else \
		if [ -n "$(BACKEND)" ]; then \
			echo "Using storage backend: $(BACKEND)"; \
			WALRUS_BACKEND=$(BACKEND) cargo test --release --test multithreaded_benchmark_writes -- --nocapture; \
		else \
			cargo test --release --test multithreaded_benchmark_writes -- --nocapture; \
		fi; \
	fi

bench-reads:
	@echo "Running read benchmark (default: async 1000ms fsync)..."
	@if [ -n "$(FSYNC)" ]; then \
		echo "Using custom fsync schedule: $(FSYNC)"; \
		if [ -n "$(BACKEND)" ]; then \
			echo "Using storage backend: $(BACKEND)"; \
			WALRUS_BACKEND=$(BACKEND) WALRUS_FSYNC=$(FSYNC) cargo test --release --test multithreaded_benchmark_reads -- --nocapture; \
		else \
			WALRUS_FSYNC=$(FSYNC) cargo test --release --test multithreaded_benchmark_reads -- --nocapture; \
		fi; \
	else \
		if [ -n "$(BACKEND)" ]; then \
			echo "Using storage backend: $(BACKEND)"; \
			WALRUS_BACKEND=$(BACKEND) cargo test --release --test multithreaded_benchmark_reads -- --nocapture; \
		else \
			cargo test --release --test multithreaded_benchmark_reads -- --nocapture; \
		fi; \
	fi

bench-scaling:
	@echo "Running scaling benchmark (default: 1-10 threads, async 1000ms fsync)..."
	@if [ -n "$(FSYNC)" ] && [ -n "$(THREADS)" ]; then \
		echo "Using custom fsync schedule: $(FSYNC) and thread range: $(THREADS)"; \
		if [ -n "$(BACKEND)" ]; then \
			echo "Using storage backend: $(BACKEND)"; \
			WALRUS_BACKEND=$(BACKEND) WALRUS_FSYNC=$(FSYNC) WALRUS_THREADS=$(THREADS) cargo test --release --test scaling_benchmark -- --nocapture; \
		else \
			WALRUS_FSYNC=$(FSYNC) WALRUS_THREADS=$(THREADS) cargo test --release --test scaling_benchmark -- --nocapture; \
		fi; \
	elif [ -n "$(FSYNC)" ]; then \
		echo "Using custom fsync schedule: $(FSYNC)"; \
		if [ -n "$(BACKEND)" ]; then \
			echo "Using storage backend: $(BACKEND)"; \
			WALRUS_BACKEND=$(BACKEND) WALRUS_FSYNC=$(FSYNC) cargo test --release --test scaling_benchmark -- --nocapture; \
		else \
			WALRUS_FSYNC=$(FSYNC) cargo test --release --test scaling_benchmark -- --nocapture; \
		fi; \
	elif [ -n "$(THREADS)" ]; then \
		echo "Using custom thread range: $(THREADS)"; \
		if [ -n "$(BACKEND)" ]; then \
			echo "Using storage backend: $(BACKEND)"; \
			WALRUS_BACKEND=$(BACKEND) WALRUS_THREADS=$(THREADS) cargo test --release --test scaling_benchmark -- --nocapture; \
		else \
			WALRUS_THREADS=$(THREADS) cargo test --release --test scaling_benchmark -- --nocapture; \
		fi; \
	else \
		if [ -n "$(BACKEND)" ]; then \
			echo "Using storage backend: $(BACKEND)"; \
			WALRUS_BACKEND=$(BACKEND) cargo test --release --test scaling_benchmark -- --nocapture; \
		else \
			cargo test --release --test scaling_benchmark -- --nocapture; \
		fi; \
	fi

# Sync variants (fsync after each write)
bench-writes-sync:
	@echo "Running write benchmark with sync-each (fsync after every write)..."
	@if [ -n "$(BACKEND)" ]; then \
		echo "Using storage backend: $(BACKEND)"; \
		WALRUS_BACKEND=$(BACKEND) WALRUS_FSYNC=sync-each cargo test --release --test multithreaded_benchmark_writes -- --nocapture; \
	else \
		WALRUS_FSYNC=sync-each cargo test --release --test multithreaded_benchmark_writes -- --nocapture; \
	fi

bench-reads-sync:
	@echo "Running read benchmark with sync-each (fsync after every write)..."
	@if [ -n "$(BACKEND)" ]; then \
		echo "Using storage backend: $(BACKEND)"; \
		WALRUS_BACKEND=$(BACKEND) WALRUS_FSYNC=sync-each cargo test --release --test multithreaded_benchmark_reads -- --nocapture; \
	else \
		WALRUS_FSYNC=sync-each cargo test --release --test multithreaded_benchmark_reads -- --nocapture; \
	fi

bench-scaling-sync:
	@echo "Running scaling benchmark with sync-each (fsync after every write)..."
	@if [ -n "$(THREADS)" ]; then \
		echo "Using custom thread range: $(THREADS)"; \
		if [ -n "$(BACKEND)" ]; then \
			echo "Using storage backend: $(BACKEND)"; \
			WALRUS_BACKEND=$(BACKEND) WALRUS_FSYNC=sync-each WALRUS_THREADS=$(THREADS) cargo test --release --test scaling_benchmark -- --nocapture; \
		else \
			WALRUS_FSYNC=sync-each WALRUS_THREADS=$(THREADS) cargo test --release --test scaling_benchmark -- --nocapture; \
		fi; \
	else \
		if [ -n "$(BACKEND)" ]; then \
			echo "Using storage backend: $(BACKEND)"; \
			WALRUS_BACKEND=$(BACKEND) WALRUS_FSYNC=sync-each cargo test --release --test scaling_benchmark -- --nocapture; \
		else \
			WALRUS_FSYNC=sync-each cargo test --release --test scaling_benchmark -- --nocapture; \
		fi; \
	fi

# Fast variants (100ms fsync interval)
bench-writes-fast:
	@echo "Running write benchmark with 100ms fsync interval..."
	@if [ -n "$(BACKEND)" ]; then \
		echo "Using storage backend: $(BACKEND)"; \
		WALRUS_BACKEND=$(BACKEND) WALRUS_FSYNC=100ms cargo test --release --test multithreaded_benchmark_writes -- --nocapture; \
	else \
		WALRUS_FSYNC=100ms cargo test --release --test multithreaded_benchmark_writes -- --nocapture; \
	fi

bench-reads-fast:
	@echo "Running read benchmark with 100ms fsync interval..."
	@if [ -n "$(BACKEND)" ]; then \
		echo "Using storage backend: $(BACKEND)"; \
		WALRUS_BACKEND=$(BACKEND) WALRUS_FSYNC=100ms cargo test --release --test multithreaded_benchmark_reads -- --nocapture; \
	else \
		WALRUS_FSYNC=100ms cargo test --release --test multithreaded_benchmark_reads -- --nocapture; \
	fi

bench-scaling-fast:
	@echo "Running scaling benchmark with 100ms fsync interval..."
	@if [ -n "$(THREADS)" ]; then \
		echo "Using custom thread range: $(THREADS)"; \
		if [ -n "$(BACKEND)" ]; then \
			echo "Using storage backend: $(BACKEND)"; \
			WALRUS_BACKEND=$(BACKEND) WALRUS_FSYNC=100ms WALRUS_THREADS=$(THREADS) cargo test --release --test scaling_benchmark -- --nocapture; \
		else \
			WALRUS_FSYNC=100ms WALRUS_THREADS=$(THREADS) cargo test --release --test scaling_benchmark -- --nocapture; \
		fi; \
	else \
		if [ -n "$(BACKEND)" ]; then \
			echo "Using storage backend: $(BACKEND)"; \
			WALRUS_BACKEND=$(BACKEND) WALRUS_FSYNC=100ms cargo test --release --test scaling_benchmark -- --nocapture; \
		else \
			WALRUS_FSYNC=100ms cargo test --release --test scaling_benchmark -- --nocapture; \
		fi; \
	fi

# Visualization targets
show-writes:
	@echo "Showing write benchmark results..."
	@if [ ! -f benchmark_throughput.csv ]; then \
		echo "benchmark_throughput.csv not found. Run 'make bench-writes' first."; \
		exit 1; \
	fi
	python3 scripts/visualize_throughput.py --file benchmark_throughput.csv

show-reads:
	@echo "Showing read benchmark results..."
	@if [ ! -f read_benchmark_throughput.csv ]; then \
		echo "read_benchmark_throughput.csv not found. Run 'make bench-reads' first."; \
		exit 1; \
	fi
	python3 scripts/show_reads_graph.py

show-scaling:
	@echo "Showing scaling benchmark results..."
	@if [ ! -f scaling_results.csv ]; then \
		echo "scaling_results.csv not found. Run 'make bench-scaling' first."; \
		exit 1; \
	fi
	python3 scripts/show_scaling_graph_writes.py

show-batch-writes:
	@echo "Showing batch benchmark results..."
	@if [ ! -f batch_benchmark_throughput.csv ]; then \
		echo "batch_benchmark_throughput.csv not found. Run 'cargo test multithreaded_batch_benchmark -- --nocapture' first."; \
		exit 1; \
	fi
	python3 scripts/visualize_batch_benchmark.py

show-walrus-vs-rocksdb:
	@echo "Comparing Walrus and RocksDB benchmark CSVs..."
	@if [ ! -f "$(WALRUS_CSV)" ]; then \
		echo "$(WALRUS_CSV) not found. Run 'WALRUS_DURATION=1s WALRUS_FSYNC=no-fsync make bench-walrus-vs-rocksdb' first, or set WALRUS_CSV=<path>."; \
		exit 1; \
	fi
	@if [ ! -f "$(ROCKSDB_CSV)" ]; then \
		echo "$(ROCKSDB_CSV) not found. Run 'WALRUS_DURATION=1s WALRUS_FSYNC=no-fsync make bench-walrus-vs-rocksdb' first, or set ROCKSDB_CSV=<path>."; \
		exit 1; \
	fi
	python3 scripts/compare_walrus_rocksdb.py --walrus "$(WALRUS_CSV)" --rocksdb "$(ROCKSDB_CSV)" --out walrus_vs_rocksdb.png

# Live monitoring targets
live-writes:
	@echo "Starting live write benchmark monitoring..."
	@echo "Run 'make bench-writes' in another terminal"
	python3 scripts/visualize_throughput.py --file benchmark_throughput.csv

live-scaling:
	@echo "Starting live scaling benchmark monitoring..."
	@echo "Run 'make bench-scaling' in another terminal"
	python3 scripts/live_scaling_plot.py

# Utility targets
clean:
	@echo "🧹 Cleaning up CSV files..."
	rm -f benchmark_throughput.csv
	rm -f read_benchmark_throughput.csv
	rm -f scaling_results.csv
	rm -f scaling_results_live.csv
	@echo "Cleanup complete!"

# Combined targets for convenience
bench-and-show-writes: bench-writes show-writes
bench-and-show-reads: bench-reads show-reads
bench-and-show-scaling: bench-scaling show-scaling

# Combined sync targets
bench-and-show-writes-sync: bench-writes-sync show-writes
bench-and-show-reads-sync: bench-reads-sync show-reads
bench-and-show-scaling-sync: bench-scaling-sync show-scaling

# Combined fast targets
bench-and-show-writes-fast: bench-writes-fast show-writes
bench-and-show-reads-fast: bench-reads-fast show-reads
bench-and-show-scaling-fast: bench-scaling-fast show-scaling
bench-batch-scaling:
	@echo "Running batch scaling benchmark (default: 1-10 threads, async 1000ms fsync, 256 entries/batch)..."
	@bash -c '\
		set -e; \
		if [ -n "$(BACKEND)" ]; then echo "Using storage backend: $(BACKEND)"; fi; \
		if [ -n "$(FSYNC)" ]; then export WALRUS_FSYNC="$(FSYNC)"; fi; \
		if [ -n "$(THREADS)" ]; then export WALRUS_THREADS="$(THREADS)"; fi; \
		if [ -n "$(BATCH)" ]; then export WALRUS_BATCH_SIZE="$(BATCH)"; fi; \
		if [ -n "$(BACKEND)" ]; then export WALRUS_BACKEND="$(BACKEND)"; fi; \
		cargo test --release --test batch_scaling_benchmark -- --nocapture \
	'

bench-walrus-vs-rocksdb:
	@echo "Running Walrus write benchmark (baseline)..."
	@$(MAKE) bench-writes FSYNC="$(FSYNC)" BACKEND="$(BACKEND)"
	@echo "Running RocksDB WAL benchmark..."
	@bash -c '\
		set -e; \
		if [ -n "$(FSYNC)" ]; then export WALRUS_FSYNC="$(FSYNC)"; fi; \
		if [ -n "$(DURATION)" ]; then export WALRUS_DURATION="$(DURATION)"; fi; \
		cargo test --release --test rocksdb_multithreaded_benchmark_writes -- --nocapture \
	'
	@echo "Generating Walrus vs RocksDB comparison plot..."
	@python3 scripts/compare_walrus_rocksdb.py --walrus benchmark_throughput.csv --rocksdb rocksdb_benchmark_throughput.csv --out walrus_vs_rocksdb.png
show-batch-scaling:
	@echo "Showing batch scaling benchmark results..."
	@if [ ! -f batch_scaling_results.csv ]; then \
		echo "batch_scaling_results.csv not found. Run 'make bench-batch-scaling' first."; \
		exit 1; \
	fi
	python3 scripts/show_batch_scaling_graph.py
