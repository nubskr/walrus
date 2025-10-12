.PHONY: help bench-writes bench-reads bench-scaling show-writes show-reads show-scaling live-writes live-scaling clean
.PHONY: bench-writes-sync bench-reads-sync bench-scaling-sync bench-writes-fast bench-reads-fast bench-scaling-fast

help:
	@echo "Walrus Benchmarks"
	@echo "=================="
	@echo ""
	@echo "Benchmarks (Default: async fsync every 1000ms):"
	@echo "  bench-writes    Run write-only benchmark (2 min)"
	@echo "  bench-reads     Run read benchmark (1 min write + 2 min read)"
	@echo "  bench-scaling   Run scaling benchmark across thread counts"
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
	@echo "Custom thread count (scaling benchmark only):"
	@echo "  THREADS=<range> make bench-scaling   # e.g., THREADS=16 or THREADS=2-8"
	@echo ""
	@echo "Visualization:"
	@echo "  show-writes     Show write benchmark results"
	@echo "  show-reads      Show read benchmark results"
	@echo "  show-scaling    Show scaling benchmark results"
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
		WALRUS_FSYNC=$(FSYNC) cargo test --test multithreaded_benchmark_writes -- --nocapture; \
	else \
		cargo test --test multithreaded_benchmark_writes -- --nocapture; \
	fi

bench-reads:
	@echo "Running read benchmark (default: async 1000ms fsync)..."
	@if [ -n "$(FSYNC)" ]; then \
		echo "Using custom fsync schedule: $(FSYNC)"; \
		WALRUS_FSYNC=$(FSYNC) cargo test --test multithreaded_benchmark_reads -- --nocapture; \
	else \
		cargo test --test multithreaded_benchmark_reads -- --nocapture; \
	fi

bench-scaling:
	@echo "Running scaling benchmark (default: 1-10 threads, async 1000ms fsync)..."
	@if [ -n "$(FSYNC)" ] && [ -n "$(THREADS)" ]; then \
		echo "Using custom fsync schedule: $(FSYNC) and thread range: $(THREADS)"; \
		WALRUS_FSYNC=$(FSYNC) WALRUS_THREADS=$(THREADS) cargo test --test scaling_benchmark -- --nocapture; \
	elif [ -n "$(FSYNC)" ]; then \
		echo "Using custom fsync schedule: $(FSYNC)"; \
		WALRUS_FSYNC=$(FSYNC) cargo test --test scaling_benchmark -- --nocapture; \
	elif [ -n "$(THREADS)" ]; then \
		echo "Using custom thread range: $(THREADS)"; \
		WALRUS_THREADS=$(THREADS) cargo test --test scaling_benchmark -- --nocapture; \
	else \
		cargo test --test scaling_benchmark -- --nocapture; \
	fi

# Sync variants (fsync after each write)
bench-writes-sync:
	@echo "Running write benchmark with sync-each (fsync after every write)..."
	WALRUS_FSYNC=sync-each cargo test --test multithreaded_benchmark_writes -- --nocapture

bench-reads-sync:
	@echo "Running read benchmark with sync-each (fsync after every write)..."
	WALRUS_FSYNC=sync-each cargo test --test multithreaded_benchmark_reads -- --nocapture

bench-scaling-sync:
	@echo "Running scaling benchmark with sync-each (fsync after every write)..."
	@if [ -n "$(THREADS)" ]; then \
		echo "Using custom thread range: $(THREADS)"; \
		WALRUS_FSYNC=sync-each WALRUS_THREADS=$(THREADS) cargo test --test scaling_benchmark -- --nocapture; \
	else \
		WALRUS_FSYNC=sync-each cargo test --test scaling_benchmark -- --nocapture; \
	fi

# Fast variants (100ms fsync interval)
bench-writes-fast:
	@echo "Running write benchmark with 100ms fsync interval..."
	WALRUS_FSYNC=100ms cargo test --test multithreaded_benchmark_writes -- --nocapture

bench-reads-fast:
	@echo "Running read benchmark with 100ms fsync interval..."
	WALRUS_FSYNC=100ms cargo test --test multithreaded_benchmark_reads -- --nocapture

bench-scaling-fast:
	@echo "Running scaling benchmark with 100ms fsync interval..."
	@if [ -n "$(THREADS)" ]; then \
		echo "Using custom thread range: $(THREADS)"; \
		WALRUS_FSYNC=100ms WALRUS_THREADS=$(THREADS) cargo test --test scaling_benchmark -- --nocapture; \
	else \
		WALRUS_FSYNC=100ms cargo test --test scaling_benchmark -- --nocapture; \
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
