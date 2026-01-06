#!/usr/bin/env python3
"""
Visualization script for PostgreSQL vs DynamoDB benchmark results.
Generates comparison charts for throughput, latency, and cost analysis.
"""

import json
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path

# Set style
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)


def load_results(filename):
    """Load benchmark results from JSON file."""
    try:
        with open(Path('benchmarks/results') / filename, 'r') as f:
            data = json.load(f)
            return data.get('results', [])
    except FileNotFoundError:
        print(
            f"Warning: {filename} not found. Run benchmarks first with 'make bench-all'")
        return []


def plot_throughput_comparison():
    """Compare operations per second across different test scenarios."""
    pg_writes = load_results('postgres-write-results.json')
    pg_reads = load_results('postgres-read-results.json')
    ddb_writes = load_results('dynamodb-write-results.json')

    if not pg_writes:
        print("No benchmark data available. Please run: make bench-all")
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # Write throughput
    write_tests = [r['test_name'] for r in pg_writes[:5]]
    pg_ops = [r['operations_per_sec'] for r in pg_writes[:5]]
    ddb_ops = [r['operations_per_sec']
               for r in ddb_writes[:5]] if ddb_writes else [0]*5

    x = np.arange(len(write_tests))
    width = 0.35

    ax1.bar(x - width/2, pg_ops, width, label='PostgreSQL', color='#336791')
    ax1.bar(x + width/2, ddb_ops, width, label='DynamoDB', color='#FF9900')
    ax1.set_xlabel('Test Scenario')
    ax1.set_ylabel('Operations per Second')
    ax1.set_title('Write Throughput Comparison')
    ax1.set_xticks(x)
    ax1.set_xticklabels(write_tests, rotation=45, ha='right')
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)

    # Read throughput
    if pg_reads:
        read_tests = [r['test_name'] for r in pg_reads[:5]]
        pg_read_ops = [r['operations_per_sec'] for r in pg_reads[:5]]

        ax2.bar(read_tests, pg_read_ops, color='#336791', label='PostgreSQL')
        ax2.set_xlabel('Test Scenario')
        ax2.set_ylabel('Operations per Second')
        ax2.set_title('Read Throughput - PostgreSQL')
        ax2.set_xticklabels(read_tests, rotation=45, ha='right')
        ax2.legend()
        ax2.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig('benchmarks/results/throughput-comparison.png',
                dpi=300, bbox_inches='tight')
    print("Generated: benchmarks/results/throughput-comparison.png")


def plot_latency_comparison():
    """Compare latency distributions (P50, P95, P99) between databases."""
    pg_writes = load_results('postgres-write-results.json')
    ddb_writes = load_results('dynamodb-write-results.json')

    if not pg_writes:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # PostgreSQL latency distribution
    tests = [r['test_name'] for r in pg_writes[:4]]
    avg_lat = [r['avg_duration_ms'] /
               1000000 for r in pg_writes[:4]]  # Convert to ms
    p95_lat = [r['p95_duration_ms'] / 1000000 for r in pg_writes[:4]]
    p99_lat = [r['p99_duration_ms'] / 1000000 for r in pg_writes[:4]]

    x = np.arange(len(tests))
    width = 0.25

    ax1.bar(x - width, avg_lat, width, label='Avg', color='#5cb85c')
    ax1.bar(x, p95_lat, width, label='P95', color='#f0ad4e')
    ax1.bar(x + width, p99_lat, width, label='P99', color='#d9534f')
    ax1.set_xlabel('Test Scenario')
    ax1.set_ylabel('Latency (ms)')
    ax1.set_title('PostgreSQL Latency Distribution')
    ax1.set_xticks(x)
    ax1.set_xticklabels(tests, rotation=45, ha='right')
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)

    # DynamoDB latency distribution (if available)
    if ddb_writes:
        ddb_tests = [r['test_name'] for r in ddb_writes[:4]]
        ddb_avg = [r['avg_duration_ms'] / 1000000 for r in ddb_writes[:4]]
        ddb_p95 = [r['p95_duration_ms'] / 1000000 for r in ddb_writes[:4]]
        ddb_p99 = [r['p99_duration_ms'] / 1000000 for r in ddb_writes[:4]]

        x2 = np.arange(len(ddb_tests))
        ax2.bar(x2 - width, ddb_avg, width, label='Avg', color='#5cb85c')
        ax2.bar(x2, ddb_p95, width, label='P95', color='#f0ad4e')
        ax2.bar(x2 + width, ddb_p99, width, label='P99', color='#d9534f')
        ax2.set_xlabel('Test Scenario')
        ax2.set_ylabel('Latency (ms)')
        ax2.set_title('DynamoDB Latency Distribution')
        ax2.set_xticks(x2)
        ax2.set_xticklabels(ddb_tests, rotation=45, ha='right')
        ax2.legend()
        ax2.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig('benchmarks/results/latency-comparison.png',
                dpi=300, bbox_inches='tight')
    print("Generated: benchmarks/results/latency-comparison.png")


def plot_cost_analysis():
    """Generate cost comparison chart for different traffic levels."""
    traffic_levels = ['10K/day', '100K/day', '1M/day', '10M/day']
    pg_costs = [81, 160, 600, 3000]  # Monthly cost in USD
    ddb_costs = [6, 60, 600, 6000]

    fig, ax = plt.subplots(figsize=(12, 6))

    x = np.arange(len(traffic_levels))
    width = 0.35

    ax.bar(x - width/2, pg_costs, width,
           label='PostgreSQL (RDS)', color='#336791')
    ax.bar(x + width/2, ddb_costs, width,
           label='DynamoDB (On-Demand)', color='#FF9900')

    ax.set_xlabel('Transaction Volume')
    ax.set_ylabel('Monthly Cost (USD)')
    ax.set_title('Cost Comparison by Traffic Level')
    ax.set_xticks(x)
    ax.set_xticklabels(traffic_levels)
    ax.legend()
    ax.set_yscale('log')
    ax.grid(axis='y', alpha=0.3, which='both')

    # Add cost values on bars
    for i, (pg, ddb) in enumerate(zip(pg_costs, ddb_costs)):
        ax.text(i - width/2, pg, f'${pg}',
                ha='center', va='bottom', fontsize=9)
        ax.text(i + width/2, ddb, f'${ddb}',
                ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    plt.savefig('benchmarks/results/cost-analysis.png',
                dpi=300, bbox_inches='tight')
    print("Generated: benchmarks/results/cost-analysis.png")


def plot_concurrency_scaling():
    """Show how each database handles increasing concurrency."""
    pg_writes = load_results('postgres-write-results.json')

    if not pg_writes:
        return

    # Extract concurrent write tests
    concurrent_tests = [r for r in pg_writes if 'Concurrent' in r['test_name']]

    if not concurrent_tests:
        return

    concurrency_levels = [r['concurrency'] for r in concurrent_tests]
    throughput = [r['operations_per_sec'] for r in concurrent_tests]
    p99_latency = [r['p99_duration_ms'] / 1000000 for r in concurrent_tests]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Throughput vs concurrency
    ax1.plot(concurrency_levels, throughput,
             marker='o', linewidth=2, color='#336791')
    ax1.set_xlabel('Concurrent Goroutines')
    ax1.set_ylabel('Operations per Second')
    ax1.set_title('PostgreSQL: Throughput vs Concurrency')
    ax1.grid(alpha=0.3)

    # Latency vs concurrency
    ax2.plot(concurrency_levels, p99_latency,
             marker='o', linewidth=2, color='#d9534f')
    ax2.set_xlabel('Concurrent Goroutines')
    ax2.set_ylabel('P99 Latency (ms)')
    ax2.set_title('PostgreSQL: P99 Latency vs Concurrency')
    ax2.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig('benchmarks/results/concurrency-scaling.png',
                dpi=300, bbox_inches='tight')
    print("Generated: benchmarks/results/concurrency-scaling.png")


def generate_summary_table():
    """Generate a summary table of all benchmark results."""
    pg_writes = load_results('postgres-write-results.json')
    pg_reads = load_results('postgres-read-results.json')

    print("\n" + "="*80)
    print("BENCHMARK SUMMARY")
    print("="*80)

    if pg_writes:
        print("\nPostgreSQL Write Performance:")
        print("-" * 80)
        print(f"{'Test Name':<40} {'Ops/Sec':<12} {'Avg Lat':<12} {'P99 Lat':<12}")
        print("-" * 80)
        for r in pg_writes:
            ops = f"{r['operations_per_sec']:.1f}"
            avg = f"{r['avg_duration_ms']/1000000:.2f}ms"
            p99 = f"{r['p99_duration_ms']/1000000:.2f}ms"
            print(f"{r['test_name']:<40} {ops:<12} {avg:<12} {p99:<12}")

    if pg_reads:
        print("\nPostgreSQL Read Performance:")
        print("-" * 80)
        print(f"{'Test Name':<40} {'Ops/Sec':<12} {'Avg Lat':<12} {'P99 Lat':<12}")
        print("-" * 80)
        for r in pg_reads:
            ops = f"{r['operations_per_sec']:.1f}"
            avg = f"{r['avg_duration_ms']/1000000:.2f}ms"
            p99 = f"{r['p99_duration_ms']/1000000:.2f}ms"
            print(f"{r['test_name']:<40} {ops:<12} {avg:<12} {p99:<12}")

    print("\n" + "="*80 + "\n")


def main():
    """Generate all visualization charts."""
    print("Generating benchmark comparison charts...")

    # Create results directory if it doesn't exist
    Path('benchmarks/results').mkdir(parents=True, exist_ok=True)

    # Generate all charts
    plot_throughput_comparison()
    plot_latency_comparison()
    plot_cost_analysis()
    plot_concurrency_scaling()
    generate_summary_table()

    print("\nAll charts generated successfully!")
    print("View results in: benchmarks/results/")


if __name__ == '__main__':
    main()
