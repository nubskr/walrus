Walrus: a high performance Write Ahead Log

## Benchmarks

### Available Benchmarks
- **Write benchmark**: 10 threads, 2-minute write phase
- **Read benchmark**: 10 threads, 1-minute write + 2-minute read phases  
- **Scaling benchmark**: Tests performance across different thread counts

### Quick Start (using Makefile)
```bash
# Run benchmarks
make bench-writes      # Write benchmark
make bench-reads       # Read benchmark  
make bench-scaling     # Scaling benchmark
make bench-all         # All benchmarks

# Show results
make show-writes       # Visualize write results
make show-reads        # Visualize read results
make show-scaling      # Visualize scaling results

# Live monitoring
make live-writes       # Live write throughput
make live-scaling      # Live scaling progress
```

### Manual Commands
```bash
# Write benchmark
cargo test --test multithreaded_benchmark_writes -- --nocapture

# Read benchmark  
cargo test --test multithreaded_benchmark_reads -- --nocapture

# Scaling benchmark
cargo test --test scaling_benchmark -- --nocapture
```

### Visualization Scripts
- `visualize_throughput.py` - Write benchmark graphs
- `show_reads_graph.py` - Read benchmark graphs  
- `show_scaling_graph_writes.py` - Scaling results
- `live_scaling_plot.py` - Live scaling monitoring

