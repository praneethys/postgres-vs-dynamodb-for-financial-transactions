# Financial Database Benchmark: PostgreSQL vs DynamoDB

A comprehensive benchmarking suite comparing PostgreSQL and DynamoDB for financial transaction workloads, with a focus on double-entry bookkeeping, ACID compliance, and real-world financial operations.

## Project Structure

```
├── README.md
├── whitepaper/
│   ├── whitepaper.md              # Comprehensive analysis whitepaper
│   ├── images/                    # Charts and diagrams
│   └── references.md              # Citations and references
├── benchmarks/
│   ├── postgres/
│   │   ├── schema.sql             # PostgreSQL schema with double-entry bookkeeping
│   │   ├── seed-data.go           # Seed 100K+ transactions
│   │   ├── benchmark-writes.go    # Write performance tests
│   │   ├── benchmark-reads.go     # Read performance tests
│   │   └── benchmark-reconciliation.go  # Complex query tests
│   ├── dynamodb/
│   │   ├── schema.json            # DynamoDB single-table design
│   │   ├── seed-data.go           # Seed 100K+ transactions
│   │   ├── benchmark-writes.go    # Write performance tests
│   │   ├── benchmark-reads.go     # Read performance tests
│   │   └── benchmark-scans.go     # Scan and aggregation tests
│   └── results/
│       ├── postgres-results.json  # Benchmark results
│       ├── dynamodb-results.json  # Benchmark results
│       └── comparison-charts.py   # Visualization script
├── docker-compose.yml             # Local PostgreSQL and DynamoDB
├── go.mod                         # Go dependencies
└── Makefile                       # Automation commands
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Go 1.21+
- Python 3.8+ (for visualization)
- AWS CLI (for DynamoDB Local)

### Setup

1. Start databases:
```bash
make start
```

2. Initialize DynamoDB tables:
```bash
make init-dynamodb
```

3. Seed test data:
```bash
make seed-all
```

4. Run benchmarks:
```bash
make bench-all
```

5. Generate comparison charts:
```bash
make results
```

Or run everything at once:
```bash
make full-benchmark
```

## Benchmark Scenarios

### 1. Write Performance

- **Single Transaction Inserts**: Individual transaction commits
- **Batch Inserts**: 100, 1000, and 10000 record batches
- **Concurrent Writes**: 10, 50, and 100 concurrent goroutines
- **Double-Entry Bookkeeping**: Atomic multi-record transactions

### 2. Read Performance

- **Single Record by ID**: Point lookups
- **Range Queries**: Last 24 hours, last 30 days
- **Account Balance Lookups**: Current balance with transaction count
- **Hot vs Cold Data**: Recently accessed vs historical data

### 3. Complex Queries

- **Reconciliation**: SUM, GROUP BY, HAVING aggregations
- **JOIN Operations**: Transactions + Accounts + Merchants
- **Time-Series Aggregations**: Daily, weekly, monthly rollups
- **Top N Queries**: Largest transactions, most active accounts

### 4. Real-World Patterns

- **Payment Processing**: Simultaneous debit + credit
- **Idempotency Handling**: Duplicate request detection
- **Concurrent Balance Updates**: Optimistic locking
- **Failed Transaction Rollbacks**: ACID compliance testing

## Database Schema Design

### PostgreSQL (Relational)

Traditional normalized schema with:
- `merchants` table
- `accounts` table with optimistic locking
- `transactions` header table
- `transaction_legs` for double-entry bookkeeping
- Indexes on foreign keys and query patterns
- Triggers for timestamp management

### DynamoDB (Single-Table Design)

Single table with composite keys:
- **PK/SK**: Primary access pattern
- **GSI1**: Status and time-based queries
- **GSI2**: Idempotency key lookups
- Denormalized data for read optimization
- Item collections for transaction atomicity

## Key Metrics Measured

- **Throughput**: Operations per second
- **Latency**: Average, P95, P99 response times
- **Scalability**: Performance under concurrent load
- **Consistency**: ACID compliance verification
- **Cost**: Estimated AWS pricing for production workloads

## Results

Results are saved in JSON format in `benchmarks/results/` and include:
- Detailed timing for each operation
- Success/error counts
- Percentile latencies (P50, P95, P99)
- Operations per second
- Concurrency impact

Visualization charts are generated showing:
- Throughput comparison
- Latency distribution
- Scalability curves
- Cost analysis

## Database Configuration

### PostgreSQL
- Version: PostgreSQL 16
- Shared Buffers: 256MB
- Max Connections: 200
- Work Memory: 16MB
- Effective Cache Size: 512MB

### DynamoDB Local
- Provisioned Throughput: 100 RCU/WCU
- 2 Global Secondary Indexes
- Streams enabled for CDC
- Encryption at rest

## Makefile Commands

```bash
make help                    # Show all commands
make start                   # Start databases
make stop                    # Stop databases
make clean                   # Clean all data
make seed-all               # Seed both databases
make bench-postgres         # Run PostgreSQL benchmarks
make bench-dynamodb         # Run DynamoDB benchmarks
make bench-all              # Run all benchmarks
make results                # Generate charts
make full-benchmark         # Complete benchmark suite
make psql                   # Connect to PostgreSQL CLI
make logs                   # Show docker logs
```

## Reading the Whitepaper

The comprehensive analysis is available in [whitepaper/whitepaper.md](whitepaper/whitepaper.md) and covers:

1. Executive Summary
2. Introduction to Financial Data Requirements
3. Database Architecture Comparison
4. Benchmark Methodology
5. Detailed Results and Analysis
6. Cost Analysis
7. Use Case Recommendations
8. Conclusions and Best Practices

## Contributing

This benchmark suite is designed for:
- Database architects evaluating technology choices
- Engineering teams building financial systems
- Academic research on database performance
- Vendor-neutral performance analysis

## License

MIT License - See LICENSE file for details

## Acknowledgments

- PostgreSQL Community
- AWS DynamoDB Team
- Go Database Libraries (lib/pq, aws-sdk-go-v2)
- Financial technology best practices from Stripe, Square, and traditional banking systems

## Contact

For questions or improvements, please open an issue on GitHub.
