#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt

# Read the data
df = pd.read_csv('scaling_results.csv')

# Create the plot
plt.figure(figsize=(10, 6))
plt.plot(df['threads'], df['throughput'], 'bo-', linewidth=2, markersize=8)
plt.xlabel('Number of Threads')
plt.ylabel('Throughput (ops/sec)')
plt.title('WAL Write Throughput Scaling')
plt.grid(True, alpha=0.3)

# Format Y-axis to avoid scientific notation
ax = plt.gca()
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))

# Set integer ticks on X-axis
plt.xticks(range(1, 11))

# Add some styling
plt.tight_layout()

# Show the plot
plt.show()

print("Scaling benchmark complete!")
print("Data saved to: scaling_results.csv")
