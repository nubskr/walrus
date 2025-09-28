#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import os
import time

class LiveScalingPlot:
    def __init__(self):
        self.fig, self.ax = plt.subplots(figsize=(10, 6))
        self.ax.set_xlabel('Number of Threads')
        self.ax.set_ylabel('Throughput (ops/sec)')
        self.ax.set_title('WAL Write Throughput Scaling (Live)')
        self.ax.grid(True, alpha=0.3)
        self.ax.set_xlim(0.5, 10.5)
        self.ax.set_xticks(range(1, 11))
        
        # Format Y-axis to avoid scientific notation
        self.ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
        
        plt.tight_layout()
        
    def update_plot(self, frame):
        if not os.path.exists('scaling_results_live.csv'):
            return
            
        try:
            df = pd.read_csv('scaling_results_live.csv')
            if df.empty:
                return
                
            self.ax.clear()
            
            # Plot the data
            self.ax.plot(df['threads'], df['throughput'], 'bo-', linewidth=2, markersize=8)
            
            # Styling
            self.ax.set_xlabel('Number of Threads')
            self.ax.set_ylabel('Throughput (ops/sec)')
            self.ax.set_title(f'WAL Write Throughput Scaling (Live) - {len(df)}/10 tests complete')
            self.ax.grid(True, alpha=0.3)
            self.ax.set_xlim(0.5, 10.5)
            self.ax.set_xticks(range(1, 11))
            
            # Format Y-axis
            self.ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
            
            # Set Y-axis limits with some padding
            if len(df) > 0:
                max_throughput = df['throughput'].max()
                self.ax.set_ylim(0, max_throughput * 1.1)
            
            plt.tight_layout()
            
        except Exception as e:
            print(f"Error updating plot: {e}")
    
    def start_monitoring(self):
        print("🚀 Starting live scaling visualization...")
        print("📊 Graph will update as each test completes")
        print("💡 Close the plot window to stop monitoring")
        
        ani = animation.FuncAnimation(self.fig, self.update_plot, 
                                    interval=1000, blit=False, cache_frame_data=False)
        
        try:
            plt.show()
        except KeyboardInterrupt:
            print("\n⏹️  Monitoring stopped by user")

if __name__ == '__main__':
    plotter = LiveScalingPlot()
    plotter.start_monitoring()
