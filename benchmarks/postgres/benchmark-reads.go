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
	transactionIDs []uuid.UUID
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
	loadTestData(db)

	suite := BenchmarkSuite{Results: make([]BenchmarkResult, 0)}

	log.Println("\n=== Running Read Performance Benchmarks ===\n")

	// Single record lookups
	suite.Results = append(suite.Results, benchmarkPointReads(db, 1000, "transaction"))
	suite.Results = append(suite.Results, benchmarkPointReads(db, 1000, "account"))

	// Range queries
	suite.Results = append(suite.Results, benchmarkRangeQuery(db, 100, 24))  // Last 24 hours
	suite.Results = append(suite.Results, benchmarkRangeQuery(db, 100, 720)) // Last 30 days

	// Account balance lookups
	suite.Results = append(suite.Results, benchmarkAccountBalance(db, 1000))

	// Transaction history for account
	suite.Results = append(suite.Results, benchmarkAccountHistory(db, 100, 100))

	// Concurrent reads
	suite.Results = append(suite.Results, benchmarkConcurrentReads(db, 1000, 10))
	suite.Results = append(suite.Results, benchmarkConcurrentReads(db, 1000, 50))
	suite.Results = append(suite.Results, benchmarkConcurrentReads(db, 1000, 100))

	saveResults(suite, "benchmarks/results/postgres-read-results.json")
	printSummary(suite)
}

func loadTestData(db *sql.DB) {
	log.Println("Loading test data...")

	rows, err := db.Query("SELECT id FROM accounts LIMIT 100")
	if err != nil {
		log.Fatal("Failed to load accounts:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id uuid.UUID
		rows.Scan(&id)
		accountIDs = append(accountIDs, id)
	}

	rows, err = db.Query("SELECT id FROM transactions LIMIT 1000")
	if err != nil {
		log.Fatal("Failed to load transactions:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id uuid.UUID
		rows.Scan(&id)
		transactionIDs = append(transactionIDs, id)
	}

	log.Printf("Loaded %d accounts and %d transactions", len(accountIDs), len(transactionIDs))
}

func benchmarkPointReads(db *sql.DB, count int, entityType string) BenchmarkResult {
	testName := fmt.Sprintf("Point Reads - %s by ID", entityType)
	log.Printf("Benchmarking %s (%d operations)...", testName, count)

	durations := make([]time.Duration, 0, count)
	successCount := 0
	errorCount := 0
	start := time.Now()

	for i := 0; i < count; i++ {
		opStart := time.Now()

		var err error
		if entityType == "transaction" {
			txnID := transactionIDs[rand.Intn(len(transactionIDs))]
			var id uuid.UUID
			var status string
			err = db.QueryRow("SELECT id, status FROM transactions WHERE id = $1", txnID).Scan(&id, &status)
		} else {
			accountID := accountIDs[rand.Intn(len(accountIDs))]
			var id uuid.UUID
			var balance float64
			err = db.QueryRow("SELECT id, balance FROM accounts WHERE id = $1", accountID).Scan(&id, &balance)
		}

		duration := time.Since(opStart)
		durations = append(durations, duration)

		if err != nil {
			errorCount++
		} else {
			successCount++
		}
	}

	totalDuration := time.Since(start)
	return calculateResults(testName, count, 1, durations, successCount, errorCount, totalDuration)
}

func benchmarkRangeQuery(db *sql.DB, count, hoursBack int) BenchmarkResult {
	testName := fmt.Sprintf("Range Query - Last %d hours", hoursBack)
	log.Printf("Benchmarking %s (%d operations)...", testName, count)

	durations := make([]time.Duration, 0, count)
	successCount := 0
	errorCount := 0
	start := time.Now()

	for i := 0; i < count; i++ {
		opStart := time.Now()

		since := time.Now().Add(-time.Duration(hoursBack) * time.Hour)
		rows, err := db.Query(`
			SELECT t.id, t.status, t.created_at
			FROM transactions t
			WHERE t.created_at >= $1
			ORDER BY t.created_at DESC
			LIMIT 100
		`, since)

		if err == nil {
			rowCount := 0
			for rows.Next() {
				var id uuid.UUID
				var status string
				var createdAt time.Time
				rows.Scan(&id, &status, &createdAt)
				rowCount++
			}
			rows.Close()
		}

		duration := time.Since(opStart)
		durations = append(durations, duration)

		if err != nil {
			errorCount++
		} else {
			successCount++
		}
	}

	totalDuration := time.Since(start)
	return calculateResults(testName, count, 1, durations, successCount, errorCount, totalDuration)
}

func benchmarkAccountBalance(db *sql.DB, count int) BenchmarkResult {
	testName := "Account Balance Lookup"
	log.Printf("Benchmarking %s (%d operations)...", testName, count)

	durations := make([]time.Duration, 0, count)
	successCount := 0
	errorCount := 0
	start := time.Now()

	for i := 0; i < count; i++ {
		opStart := time.Now()

		accountID := accountIDs[rand.Intn(len(accountIDs))]
		var balance float64
		var txnCount int
		err := db.QueryRow(`
			SELECT a.balance, COUNT(tl.id)
			FROM accounts a
			LEFT JOIN transaction_legs tl ON a.id = tl.account_id
			WHERE a.id = $1
			GROUP BY a.id, a.balance
		`, accountID).Scan(&balance, &txnCount)

		duration := time.Since(opStart)
		durations = append(durations, duration)

		if err != nil {
			errorCount++
		} else {
			successCount++
		}
	}

	totalDuration := time.Since(start)
	return calculateResults(testName, count, 1, durations, successCount, errorCount, totalDuration)
}

func benchmarkAccountHistory(db *sql.DB, count, limit int) BenchmarkResult {
	testName := fmt.Sprintf("Account Transaction History (last %d txns)", limit)
	log.Printf("Benchmarking %s (%d operations)...", testName, count)

	durations := make([]time.Duration, 0, count)
	successCount := 0
	errorCount := 0
	start := time.Now()

	for i := 0; i < count; i++ {
		opStart := time.Now()

		accountID := accountIDs[rand.Intn(len(accountIDs))]
		rows, err := db.Query(`
			SELECT tl.transaction_id, tl.leg_type, tl.amount, tl.created_at
			FROM transaction_legs tl
			WHERE tl.account_id = $1
			ORDER BY tl.created_at DESC
			LIMIT $2
		`, accountID, limit)

		if err == nil {
			for rows.Next() {
				var txnID uuid.UUID
				var legType string
				var amount float64
				var createdAt time.Time
				rows.Scan(&txnID, &legType, &amount, &createdAt)
			}
			rows.Close()
		}

		duration := time.Since(opStart)
		durations = append(durations, duration)

		if err != nil {
			errorCount++
		} else {
			successCount++
		}
	}

	totalDuration := time.Since(start)
	return calculateResults(testName, count, 1, durations, successCount, errorCount, totalDuration)
}

func benchmarkConcurrentReads(db *sql.DB, opsPerGoroutine, numGoroutines int) BenchmarkResult {
	testName := fmt.Sprintf("Concurrent Reads (%d goroutines, %d ops each)", numGoroutines, opsPerGoroutine)
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

				txnID := transactionIDs[rand.Intn(len(transactionIDs))]
				var id uuid.UUID
				var status string
				err := db.QueryRow("SELECT id, status FROM transactions WHERE id = $1", txnID).Scan(&id, &status)

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

func calculateResults(testName string, totalOps, concurrency int, durations []time.Duration, success, errors int, totalDuration time.Duration) BenchmarkResult {
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
