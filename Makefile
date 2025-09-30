.PHONY: help bench-writes bench-reads bench-scaling bench-all show-writes show-reads show-scaling live-writes live-scaling clean

help:
	@echo "Walrus Benchmarks"
	@echo "=================="
	@echo ""
	@echo "Benchmarks:"
	@echo "  bench-writes    Run write-only benchmark (2 min)"
	@echo "  bench-reads     Run read benchmark (1 min write + 2 min read)"
	@echo "  bench-scaling   Run scaling benchmark across thread counts"
	@echo "  bench-all       Run all benchmarks sequentially"
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

# Benchmark targets
bench-writes:
	@echo "🚀 Running write benchmark..."
	cargo test --test multithreaded_benchmark_writes -- --nocapture

bench-reads:
	@echo "🚀 Running read benchmark..."
	cargo test --test multithreaded_benchmark_reads -- --nocapture

bench-scaling:
	@echo "🚀 Running scaling benchmark..."
	cargo test --test scaling_benchmark -- --nocapture

bench-all: bench-writes bench-reads bench-scaling
	@echo "✅ All benchmarks completed!"

# Visualization targets
show-writes:
	@echo "📊 Showing write benchmark results..."
	@if [ ! -f benchmark_throughput.csv ]; then \
		echo "❌ benchmark_throughput.csv not found. Run 'make bench-writes' first."; \
		exit 1; \
	fi
	python3 visualize_throughput.py --file benchmark_throughput.csv

show-reads:
	@echo "📊 Showing read benchmark results..."
	@if [ ! -f read_benchmark_throughput.csv ]; then \
		echo "❌ read_benchmark_throughput.csv not found. Run 'make bench-reads' first."; \
		exit 1; \
	fi
	python3 show_reads_graph.py

show-scaling:
	@echo "📊 Showing scaling benchmark results..."
	@if [ ! -f scaling_results.csv ]; then \
		echo "❌ scaling_results.csv not found. Run 'make bench-scaling' first."; \
		exit 1; \
	fi
	python3 show_scaling_graph_writes.py

# Live monitoring targets
live-writes:
	@echo "📈 Starting live write benchmark monitoring..."
	@echo "💡 Run 'make bench-writes' in another terminal"
	python3 visualize_throughput.py --file benchmark_throughput.csv

live-scaling:
	@echo "📈 Starting live scaling benchmark monitoring..."
	@echo "💡 Run 'make bench-scaling' in another terminal"
	python3 live_scaling_plot.py

# Utility targets
clean:
	@echo "🧹 Cleaning up CSV files..."
	rm -f benchmark_throughput.csv
	rm -f read_benchmark_throughput.csv
	rm -f scaling_results.csv
	rm -f scaling_results_live.csv
	@echo "✅ Cleanup complete!"

# Combined targets for convenience
bench-and-show-writes: bench-writes show-writes
bench-and-show-reads: bench-reads show-reads
bench-and-show-scaling: bench-scaling show-scaling
