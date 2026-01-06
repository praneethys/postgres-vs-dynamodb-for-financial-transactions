.PHONY: help setup start stop clean seed-postgres seed-dynamodb bench-all bench-postgres bench-dynamodb results

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Setup project dependencies
	go mod download
	go mod tidy

start: ## Start PostgreSQL and DynamoDB Local
	docker compose up -d
	@echo "Waiting for databases to be ready..."
	@sleep 5
	@echo "PostgreSQL available at localhost:5432"
	@echo "DynamoDB Local available at localhost:8000"
	@echo "DynamoDB Admin UI available at http://localhost:8001"

stop: ## Stop all services
	docker compose down

clean: ## Clean up all data and stop services
	docker compose down -v
	rm -f benchmarks/results/*.json
	rm -f benchmarks/results/*.png

init-dynamodb: ## Initialize DynamoDB tables
	AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local \
	aws dynamodb create-table \
		--endpoint-url http://localhost:8000 \
		--region us-east-1 \
		--cli-input-json file://benchmarks/dynamodb/schema.json \
		|| echo "Table may already exist"

seed-postgres: ## Seed PostgreSQL with test data
	go run benchmarks/postgres/seed-data.go

seed-dynamodb: init-dynamodb ## Seed DynamoDB with test data
	go run benchmarks/dynamodb/seed-data.go

seed-all: seed-postgres seed-dynamodb ## Seed both databases

bench-postgres-writes: ## Run PostgreSQL write benchmarks
	go run benchmarks/postgres/benchmark-writes.go

bench-postgres-reads: ## Run PostgreSQL read benchmarks
	go run benchmarks/postgres/benchmark-reads.go

bench-postgres-reconciliation: ## Run PostgreSQL reconciliation benchmarks
	go run benchmarks/postgres/benchmark-reconciliation.go

bench-postgres: bench-postgres-writes bench-postgres-reads bench-postgres-reconciliation ## Run all PostgreSQL benchmarks

bench-dynamodb-writes: ## Run DynamoDB write benchmarks
	go run benchmarks/dynamodb/benchmark-writes.go

bench-dynamodb-reads: ## Run DynamoDB read benchmarks
	go run benchmarks/dynamodb/benchmark-reads.go

bench-dynamodb-scans: ## Run DynamoDB scan benchmarks
	go run benchmarks/dynamodb/benchmark-scans.go

bench-dynamodb: bench-dynamodb-writes bench-dynamodb-reads bench-dynamodb-scans ## Run all DynamoDB benchmarks

bench-all: bench-postgres bench-dynamodb ## Run all benchmarks

results: ## Generate comparison charts and analysis
	python3 benchmarks/results/comparison-charts.py

full-benchmark: clean start seed-all bench-all results ## Run complete benchmark suite
	@echo "Benchmark complete! Check benchmarks/results/ for outputs"

# Development helpers
logs: ## Show docker logs
	docker compose logs -f

psql: ## Connect to PostgreSQL
	docker exec -it financial-benchmark-postgres psql -U benchmark -d financial_benchmark

restart: stop start ## Restart all services
