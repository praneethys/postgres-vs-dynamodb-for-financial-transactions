package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/shopspring/decimal"
)

type BenchmarkResult struct {
	TestName          string        `json:"test_name"`
	Database          string        `json:"database"`
	NumOperations     int           `json:"num_operations"`
	Concurrency       int           `json:"concurrency"`
	TotalDuration     time.Duration `json:"total_duration_ms"`
	AverageDuration   time.Duration `json:"avg_duration_ms"`
	MedianDuration    time.Duration `json:"median_duration_ms"`
	P95Duration       time.Duration `json:"p95_duration_ms"`
	P99Duration       time.Duration `json:"p99_duration_ms"`
	OperationsPerSec  float64       `json:"operations_per_sec"`
	SuccessCount      int           `json:"success_count"`
	ErrorCount        int           `json:"error_count"`
	Timestamp         time.Time     `json:"timestamp"`
}

type BenchmarkSuite struct {
	Results []BenchmarkResult `json:"results"`
}

var (
	accountIDs []uuid.UUID
	merchantIDs []uuid.UUID
)

func main() {
	connStr := "host=localhost port=5432 user=benchmark password=benchmark123 dbname=financial_benchmark sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)

	if err := db.Ping(); err != nil {
		log.Fatal("Failed to ping database:", err)
	}

	log.Println("Connected to PostgreSQL")

	// Load existing accounts and merchants for testing
	loadTestData(db)

	suite := BenchmarkSuite{Results: make([]BenchmarkResult, 0)}

	// Run benchmarks
	log.Println("\n=== Running Write Performance Benchmarks ===\n")

	// 1. Single transaction inserts
	suite.Results = append(suite.Results, benchmarkSingleInserts(db, 1000))

	// 2. Batch inserts
	suite.Results = append(suite.Results, benchmarkBatchInserts(db, 100, 100))
	suite.Results = append(suite.Results, benchmarkBatchInserts(db, 10, 1000))
	suite.Results = append(suite.Results, benchmarkBatchInserts(db, 1, 10000))

	// 3. Concurrent writes
	suite.Results = append(suite.Results, benchmarkConcurrentWrites(db, 1000, 10))
	suite.Results = append(suite.Results, benchmarkConcurrentWrites(db, 1000, 50))
	suite.Results = append(suite.Results, benchmarkConcurrentWrites(db, 1000, 100))

	// 4. Double-entry atomic writes
	suite.Results = append(suite.Results, benchmarkDoubleEntryWrites(db, 1000, 1))
	suite.Results = append(suite.Results, benchmarkDoubleEntryWrites(db, 1000, 10))

	// Save results
	saveResults(suite, "benchmarks/results/postgres-write-results.json")
	printSummary(suite)
}

func loadTestData(db *sql.DB) {
	log.Println("Loading test data...")

	// Load 100 random accounts
	rows, err := db.Query("SELECT id FROM accounts LIMIT 100")
	if err != nil {
		log.Fatal("Failed to load accounts:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			log.Fatal("Failed to scan account:", err)
		}
		accountIDs = append(accountIDs, id)
	}

	// Load 100 random merchants
	rows, err = db.Query("SELECT id FROM merchants LIMIT 100")
	if err != nil {
		log.Fatal("Failed to load merchants:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			log.Fatal("Failed to scan merchant:", err)
		}
		merchantIDs = append(merchantIDs, id)
	}

	log.Printf("Loaded %d accounts and %d merchants", len(accountIDs), len(merchantIDs))
}

func benchmarkSingleInserts(db *sql.DB, count int) BenchmarkResult {
	log.Printf("Benchmarking single transaction inserts (%d operations)...", count)

	durations := make([]time.Duration, 0, count)
	successCount := 0
	errorCount := 0
	start := time.Now()

	for i := 0; i < count; i++ {
		opStart := time.Now()
		err := insertTransaction(db)
		duration := time.Since(opStart)
		durations = append(durations, duration)

		if err != nil {
			errorCount++
		} else {
			successCount++
		}
	}

	totalDuration := time.Since(start)

	return calculateResults("Single Transaction Inserts", count, 1, durations, successCount, errorCount, totalDuration)
}

func benchmarkBatchInserts(db *sql.DB, numBatches, batchSize int) BenchmarkResult {
	testName := fmt.Sprintf("Batch Inserts (%d batches of %d)", numBatches, batchSize)
	log.Printf("Benchmarking %s...", testName)

	durations := make([]time.Duration, 0, numBatches)
	successCount := 0
	errorCount := 0
	start := time.Now()

	for i := 0; i < numBatches; i++ {
		opStart := time.Now()
		err := insertBatch(db, batchSize)
		duration := time.Since(opStart)
		durations = append(durations, duration)

		if err != nil {
			errorCount++
		} else {
			successCount++
		}
	}

	totalDuration := time.Since(start)

	return calculateResults(testName, numBatches*batchSize, 1, durations, successCount*batchSize, errorCount, totalDuration)
}

func benchmarkConcurrentWrites(db *sql.DB, opsPerGoroutine, numGoroutines int) BenchmarkResult {
	testName := fmt.Sprintf("Concurrent Writes (%d goroutines, %d ops each)", numGoroutines, opsPerGoroutine)
	log.Printf("Benchmarking %s...", testName)

	var wg sync.WaitGroup
	var mu sync.Mutex
	durations := make([]time.Duration, 0, opsPerGoroutine*numGoroutines)
	successCount := 0
	errorCount := 0

	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				opStart := time.Now()
				err := insertTransaction(db)
				duration := time.Since(opStart)

				mu.Lock()
				durations = append(durations, duration)
				if err != nil {
					errorCount++
				} else {
					successCount++
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	totalDuration := time.Since(start)

	return calculateResults(testName, opsPerGoroutine*numGoroutines, numGoroutines, durations, successCount, errorCount, totalDuration)
}

func benchmarkDoubleEntryWrites(db *sql.DB, count, concurrency int) BenchmarkResult {
	testName := fmt.Sprintf("Double-Entry Atomic Writes (%d ops, %d concurrent)", count, concurrency)
	log.Printf("Benchmarking %s...", testName)

	var wg sync.WaitGroup
	var mu sync.Mutex
	durations := make([]time.Duration, 0, count)
	successCount := 0
	errorCount := 0

	opsPerGoroutine := count / concurrency
	start := time.Now()

	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				opStart := time.Now()
				err := insertDoubleEntryTransaction(db)
				duration := time.Since(opStart)

				mu.Lock()
				durations = append(durations, duration)
				if err != nil {
					errorCount++
				} else {
					successCount++
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	totalDuration := time.Since(start)

	return calculateResults(testName, count, concurrency, durations, successCount, errorCount, totalDuration)
}

func insertTransaction(db *sql.DB) error {
	txnID := uuid.New()
	idempotencyKey := uuid.New().String()
	merchantID := merchantIDs[rand.Intn(len(merchantIDs))]
	amount := decimal.NewFromFloat(rand.Float64() * 1000)
	debitAccount := accountIDs[rand.Intn(len(accountIDs))]
	creditAccount := accountIDs[rand.Intn(len(accountIDs))]

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO transactions (id, idempotency_key, transaction_type, status, merchant_id, description)
		VALUES ($1, $2, 'payment', 'completed', $3, 'Benchmark transaction')
	`, txnID, idempotencyKey, merchantID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		INSERT INTO transaction_legs (transaction_id, account_id, leg_type, amount, currency)
		VALUES ($1, $2, 'debit', $3, 'USD'), ($4, $5, 'credit', $6, 'USD')
	`, txnID, debitAccount, amount, txnID, creditAccount, amount)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func insertBatch(db *sql.DB, batchSize int) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO transactions (id, idempotency_key, transaction_type, status, merchant_id, description)
		VALUES ($1, $2, 'payment', 'completed', $3, 'Batch benchmark transaction')
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	legStmt, err := tx.Prepare(`
		INSERT INTO transaction_legs (transaction_id, account_id, leg_type, amount, currency)
		VALUES ($1, $2, $3, $4, 'USD')
	`)
	if err != nil {
		return err
	}
	defer legStmt.Close()

	for i := 0; i < batchSize; i++ {
		txnID := uuid.New()
		idempotencyKey := uuid.New().String()
		merchantID := merchantIDs[rand.Intn(len(merchantIDs))]
		amount := decimal.NewFromFloat(rand.Float64() * 1000)

		_, err = stmt.Exec(txnID, idempotencyKey, merchantID)
		if err != nil {
			return err
		}

		debitAccount := accountIDs[rand.Intn(len(accountIDs))]
		creditAccount := accountIDs[rand.Intn(len(accountIDs))]

		_, err = legStmt.Exec(txnID, debitAccount, "debit", amount)
		if err != nil {
			return err
		}

		_, err = legStmt.Exec(txnID, creditAccount, "credit", amount)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func insertDoubleEntryTransaction(db *sql.DB) error {
	return insertTransaction(db) // Same as single insert with ACID guarantees
}

func calculateResults(testName string, totalOps, concurrency int, durations []time.Duration, success, errors int, totalDuration time.Duration) BenchmarkResult {
	// Sort durations for percentile calculations
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	var avgDuration time.Duration
	if len(durations) > 0 {
		var sum time.Duration
		for _, d := range durations {
			sum += d
		}
		avgDuration = sum / time.Duration(len(durations))
	}

	median := sorted[len(sorted)/2]
	p95 := sorted[int(float64(len(sorted))*0.95)]
	p99 := sorted[int(float64(len(sorted))*0.99)]

	opsPerSec := float64(totalOps) / totalDuration.Seconds()

	return BenchmarkResult{
		TestName:         testName,
		Database:         "PostgreSQL",
		NumOperations:    totalOps,
		Concurrency:      concurrency,
		TotalDuration:    totalDuration,
		AverageDuration:  avgDuration,
		MedianDuration:   median,
		P95Duration:      p95,
		P99Duration:      p99,
		OperationsPerSec: opsPerSec,
		SuccessCount:     success,
		ErrorCount:       errors,
		Timestamp:        time.Now(),
	}
}

func saveResults(suite BenchmarkSuite, filename string) {
	data, err := json.MarshalIndent(suite, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal results: %v", err)
		return
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		log.Printf("Failed to write results: %v", err)
		return
	}

	log.Printf("\nResults saved to %s", filename)
}

func printSummary(suite BenchmarkSuite) {
	fmt.Println("\n=== Benchmark Summary ===\n")
	for _, result := range suite.Results {
		fmt.Printf("Test: %s\n", result.TestName)
		fmt.Printf("  Operations: %d (Success: %d, Errors: %d)\n", result.NumOperations, result.SuccessCount, result.ErrorCount)
		fmt.Printf("  Total Duration: %v\n", result.TotalDuration)
		fmt.Printf("  Ops/sec: %.2f\n", result.OperationsPerSec)
		fmt.Printf("  Avg Latency: %v\n", result.AverageDuration)
		fmt.Printf("  P95 Latency: %v\n", result.P95Duration)
		fmt.Printf("  P99 Latency: %v\n", result.P99Duration)
		fmt.Println()
	}
}
