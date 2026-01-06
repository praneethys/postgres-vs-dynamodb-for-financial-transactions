package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type BenchmarkResult struct {
	TestName          string        `json:"test_name"`
	Database          string        `json:"database"`
	NumOperations     int           `json:"num_operations"`
	TotalDuration     time.Duration `json:"total_duration_ms"`
	AverageDuration   time.Duration `json:"avg_duration_ms"`
	OperationsPerSec  float64       `json:"operations_per_sec"`
	RowsScanned       int64         `json:"rows_scanned"`
	RowsReturned      int           `json:"rows_returned"`
	SuccessCount      int           `json:"success_count"`
	ErrorCount        int           `json:"error_count"`
	Timestamp         time.Time     `json:"timestamp"`
}

type BenchmarkSuite struct {
	Results []BenchmarkResult `json:"results"`
}

var accountIDs []uuid.UUID

func main() {
	connStr := "host=localhost port=5432 user=benchmark password=benchmark123 dbname=financial_benchmark sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal("Failed to ping database:", err)
	}

	log.Println("Connected to PostgreSQL")
	loadTestData(db)

	suite := BenchmarkSuite{Results: make([]BenchmarkResult, 0)}

	log.Println("\n=== Running Reconciliation & Complex Query Benchmarks ===\n")

	suite.Results = append(suite.Results, benchmarkAccountReconciliation(db, 100))
	suite.Results = append(suite.Results, benchmarkDailySummary(db, 10))
	suite.Results = append(suite.Results, benchmarkMerchantAnalysis(db, 50))
	suite.Results = append(suite.Results, benchmarkTopAccounts(db, 100))
	suite.Results = append(suite.Results, benchmarkBalanceVerification(db, 50))
	suite.Results = append(suite.Results, benchmarkJoinQuery(db, 100))

	saveResults(suite, "benchmarks/results/postgres-reconciliation-results.json")
	printSummary(suite)
}

func loadTestData(db *sql.DB) {
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
	log.Printf("Loaded %d accounts", len(accountIDs))
}

func benchmarkAccountReconciliation(db *sql.DB, count int) BenchmarkResult {
	testName := "Account Reconciliation (SUM by account)"
	log.Printf("Benchmarking %s (%d operations)...", testName, count)

	successCount := 0
	errorCount := 0
	var totalRows int64
	start := time.Now()

	for i := 0; i < count; i++ {
		accountID := accountIDs[rand.Intn(len(accountIDs))]

		rows, err := db.Query(`
			SELECT
				leg_type,
				COUNT(*) as count,
				SUM(amount) as total,
				AVG(amount) as average,
				MIN(amount) as min,
				MAX(amount) as max
			FROM transaction_legs
			WHERE account_id = $1
			GROUP BY leg_type
		`, accountID)

		if err == nil {
			for rows.Next() {
				var legType string
				var count, total, avg, min, max float64
				rows.Scan(&legType, &count, &total, &avg, &min, &max)
				totalRows++
			}
			rows.Close()
			successCount++
		} else {
			errorCount++
		}
	}

	totalDuration := time.Since(start)
	avgDuration := totalDuration / time.Duration(count)
	opsPerSec := float64(count) / totalDuration.Seconds()

	return BenchmarkResult{
		TestName:         testName,
		Database:         "PostgreSQL",
		NumOperations:    count,
		TotalDuration:    totalDuration,
		AverageDuration:  avgDuration,
		OperationsPerSec: opsPerSec,
		RowsScanned:      totalRows,
		RowsReturned:     int(totalRows),
		SuccessCount:     successCount,
		ErrorCount:       errorCount,
		Timestamp:        time.Now(),
	}
}

func benchmarkDailySummary(db *sql.DB, count int) BenchmarkResult {
	testName := "Daily Transaction Summary (GROUP BY date)"
	log.Printf("Benchmarking %s (%d operations)...", testName, count)

	successCount := 0
	errorCount := 0
	var totalRows int64
	start := time.Now()

	for i := 0; i < count; i++ {
		rows, err := db.Query(`
			SELECT
				DATE(t.created_at) as date,
				t.transaction_type,
				COUNT(*) as count,
				SUM(tl.amount) as total_amount
			FROM transactions t
			JOIN transaction_legs tl ON t.id = tl.transaction_id
			WHERE t.created_at >= NOW() - INTERVAL '30 days'
				AND tl.leg_type = 'debit'
			GROUP BY DATE(t.created_at), t.transaction_type
			ORDER BY date DESC
		`)

		if err == nil {
			for rows.Next() {
				var date time.Time
				var txnType string
				var count int
				var total float64
				rows.Scan(&date, &txnType, &count, &total)
				totalRows++
			}
			rows.Close()
			successCount++
		} else {
			errorCount++
		}
	}

	totalDuration := time.Since(start)
	avgDuration := totalDuration / time.Duration(count)
	opsPerSec := float64(count) / totalDuration.Seconds()

	return BenchmarkResult{
		TestName:         testName,
		Database:         "PostgreSQL",
		NumOperations:    count,
		TotalDuration:    totalDuration,
		AverageDuration:  avgDuration,
		OperationsPerSec: opsPerSec,
		RowsScanned:      totalRows,
		RowsReturned:     int(totalRows),
		SuccessCount:     successCount,
		ErrorCount:       errorCount,
		Timestamp:        time.Now(),
	}
}

func benchmarkMerchantAnalysis(db *sql.DB, count int) BenchmarkResult {
	testName := "Merchant Analysis (JOIN with aggregation)"
	log.Printf("Benchmarking %s (%d operations)...", testName, count)

	successCount := 0
	errorCount := 0
	var totalRows int64
	start := time.Now()

	for i := 0; i < count; i++ {
		rows, err := db.Query(`
			SELECT
				m.id,
				m.name,
				m.category,
				COUNT(t.id) as transaction_count,
				SUM(tl.amount) as total_volume
			FROM merchants m
			JOIN transactions t ON m.id = t.merchant_id
			JOIN transaction_legs tl ON t.id = tl.transaction_id
			WHERE tl.leg_type = 'debit'
				AND t.created_at >= NOW() - INTERVAL '7 days'
			GROUP BY m.id, m.name, m.category
			HAVING COUNT(t.id) > 5
			ORDER BY total_volume DESC
			LIMIT 50
		`)

		if err == nil {
			for rows.Next() {
				var id uuid.UUID
				var name, category string
				var count int
				var volume float64
				rows.Scan(&id, &name, &category, &count, &volume)
				totalRows++
			}
			rows.Close()
			successCount++
		} else {
			errorCount++
		}
	}

	totalDuration := time.Since(start)
	avgDuration := totalDuration / time.Duration(count)
	opsPerSec := float64(count) / totalDuration.Seconds()

	return BenchmarkResult{
		TestName:         testName,
		Database:         "PostgreSQL",
		NumOperations:    count,
		TotalDuration:    totalDuration,
		AverageDuration:  avgDuration,
		OperationsPerSec: opsPerSec,
		RowsScanned:      totalRows,
		RowsReturned:     int(totalRows),
		SuccessCount:     successCount,
		ErrorCount:       errorCount,
		Timestamp:        time.Now(),
	}
}

func benchmarkTopAccounts(db *sql.DB, count int) BenchmarkResult {
	testName := "Top N Accounts by Activity"
	log.Printf("Benchmarking %s (%d operations)...", testName, count)

	successCount := 0
	errorCount := 0
	var totalRows int64
	start := time.Now()

	for i := 0; i < count; i++ {
		rows, err := db.Query(`
			SELECT
				a.id,
				a.account_type,
				a.balance,
				COUNT(tl.id) as transaction_count,
				SUM(CASE WHEN tl.leg_type = 'debit' THEN tl.amount ELSE 0 END) as total_debits,
				SUM(CASE WHEN tl.leg_type = 'credit' THEN tl.amount ELSE 0 END) as total_credits
			FROM accounts a
			LEFT JOIN transaction_legs tl ON a.id = tl.account_id
			WHERE tl.created_at >= NOW() - INTERVAL '30 days'
			GROUP BY a.id, a.account_type, a.balance
			ORDER BY transaction_count DESC
			LIMIT 100
		`)

		if err == nil {
			for rows.Next() {
				var id uuid.UUID
				var accountType string
				var balance float64
				var count int
				var debits, credits float64
				rows.Scan(&id, &accountType, &balance, &count, &debits, &credits)
				totalRows++
			}
			rows.Close()
			successCount++
		} else {
			errorCount++
		}
	}

	totalDuration := time.Since(start)
	avgDuration := totalDuration / time.Duration(count)
	opsPerSec := float64(count) / totalDuration.Seconds()

	return BenchmarkResult{
		TestName:         testName,
		Database:         "PostgreSQL",
		NumOperations:    count,
		TotalDuration:    totalDuration,
		AverageDuration:  avgDuration,
		OperationsPerSec: opsPerSec,
		RowsScanned:      totalRows,
		RowsReturned:     int(totalRows),
		SuccessCount:     successCount,
		ErrorCount:       errorCount,
		Timestamp:        time.Now(),
	}
}

func benchmarkBalanceVerification(db *sql.DB, count int) BenchmarkResult {
	testName := "Balance Verification (debits = credits)"
	log.Printf("Benchmarking %s (%d operations)...", testName, count)

	successCount := 0
	errorCount := 0
	var totalRows int64
	start := time.Now()

	for i := 0; i < count; i++ {
		rows, err := db.Query(`
			SELECT
				t.id,
				SUM(CASE WHEN tl.leg_type = 'debit' THEN tl.amount ELSE 0 END) as total_debits,
				SUM(CASE WHEN tl.leg_type = 'credit' THEN tl.amount ELSE 0 END) as total_credits,
				SUM(CASE WHEN tl.leg_type = 'debit' THEN tl.amount ELSE 0 END) -
				SUM(CASE WHEN tl.leg_type = 'credit' THEN tl.amount ELSE 0 END) as difference
			FROM transactions t
			JOIN transaction_legs tl ON t.id = tl.transaction_id
			GROUP BY t.id
			HAVING SUM(CASE WHEN tl.leg_type = 'debit' THEN tl.amount ELSE 0 END) !=
				   SUM(CASE WHEN tl.leg_type = 'credit' THEN tl.amount ELSE 0 END)
			LIMIT 100
		`)

		if err == nil {
			for rows.Next() {
				var id uuid.UUID
				var debits, credits, diff float64
				rows.Scan(&id, &debits, &credits, &diff)
				totalRows++
			}
			rows.Close()
			successCount++
		} else {
			errorCount++
		}
	}

	totalDuration := time.Since(start)
	avgDuration := totalDuration / time.Duration(count)
	opsPerSec := float64(count) / totalDuration.Seconds()

	return BenchmarkResult{
		TestName:         testName,
		Database:         "PostgreSQL",
		NumOperations:    count,
		TotalDuration:    totalDuration,
		AverageDuration:  avgDuration,
		OperationsPerSec: opsPerSec,
		RowsScanned:      totalRows,
		RowsReturned:     int(totalRows),
		SuccessCount:     successCount,
		ErrorCount:       errorCount,
		Timestamp:        time.Now(),
	}
}

func benchmarkJoinQuery(db *sql.DB, count int) BenchmarkResult {
	testName := "Multi-table JOIN Query"
	log.Printf("Benchmarking %s (%d operations)...", testName, count)

	successCount := 0
	errorCount := 0
	var totalRows int64
	start := time.Now()

	for i := 0; i < count; i++ {
		rows, err := db.Query(`
			SELECT
				t.id,
				t.transaction_type,
				t.status,
				m.name as merchant_name,
				m.category as merchant_category,
				a.account_type,
				tl.leg_type,
				tl.amount,
				t.created_at
			FROM transactions t
			JOIN merchants m ON t.merchant_id = m.id
			JOIN transaction_legs tl ON t.id = tl.transaction_id
			JOIN accounts a ON tl.account_id = a.id
			WHERE t.created_at >= NOW() - INTERVAL '7 days'
			ORDER BY t.created_at DESC
			LIMIT 100
		`)

		if err == nil {
			for rows.Next() {
				var txnID uuid.UUID
				var txnType, status, merchantName, merchantCategory, accountType, legType string
				var amount float64
				var createdAt time.Time
				rows.Scan(&txnID, &txnType, &status, &merchantName, &merchantCategory, &accountType, &legType, &amount, &createdAt)
				totalRows++
			}
			rows.Close()
			successCount++
		} else {
			errorCount++
		}
	}

	totalDuration := time.Since(start)
	avgDuration := totalDuration / time.Duration(count)
	opsPerSec := float64(count) / totalDuration.Seconds()

	return BenchmarkResult{
		TestName:         testName,
		Database:         "PostgreSQL",
		NumOperations:    count,
		TotalDuration:    totalDuration,
		AverageDuration:  avgDuration,
		OperationsPerSec: opsPerSec,
		RowsScanned:      totalRows,
		RowsReturned:     int(totalRows),
		SuccessCount:     successCount,
		ErrorCount:       errorCount,
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
		fmt.Printf("  Avg Duration: %v\n", result.AverageDuration)
		fmt.Printf("  Ops/sec: %.2f\n", result.OperationsPerSec)
		fmt.Printf("  Rows Scanned/Returned: %d\n", result.RowsScanned)
		fmt.Println()
	}
}
