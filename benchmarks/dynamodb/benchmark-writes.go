package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
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
	ConsumedWCU       float64       `json:"consumed_wcu"`
	Timestamp         time.Time     `json:"timestamp"`
}

type BenchmarkSuite struct {
	Results []BenchmarkResult `json:"results"`
}

type Transaction struct {
	PK              string    `dynamodbav:"PK"`
	SK              string    `dynamodbav:"SK"`
	GSI1PK          string    `dynamodbav:"GSI1PK"`
	GSI1SK          string    `dynamodbav:"GSI1SK"`
	GSI2PK          string    `dynamodbav:"GSI2PK"`
	GSI2SK          string    `dynamodbav:"GSI2SK"`
	Type            string    `dynamodbav:"Type"`
	ID              string    `dynamodbav:"ID"`
	IdempotencyKey  string    `dynamodbav:"IdempotencyKey"`
	TransactionType string    `dynamodbav:"TransactionType"`
	Status          string    `dynamodbav:"Status"`
	MerchantID      string    `dynamodbav:"MerchantID"`
	Description     string    `dynamodbav:"Description"`
	CreatedAt       time.Time `dynamodbav:"CreatedAt"`
}

type TransactionLeg struct {
	PK            string          `dynamodbav:"PK"`
	SK            string          `dynamodbav:"SK"`
	GSI1PK        string          `dynamodbav:"GSI1PK"`
	GSI1SK        string          `dynamodbav:"GSI1SK"`
	Type          string          `dynamodbav:"Type"`
	ID            string          `dynamodbav:"ID"`
	TransactionID string          `dynamodbav:"TransactionID"`
	AccountID     string          `dynamodbav:"AccountID"`
	LegType       string          `dynamodbav:"LegType"`
	Amount        decimal.Decimal `dynamodbav:"Amount"`
	Currency      string          `dynamodbav:"Currency"`
	CreatedAt     time.Time       `dynamodbav:"CreatedAt"`
}

var (
	client      *dynamodb.Client
	ctx         = context.Background()
	accountIDs  []string
	merchantIDs []string
)

func main() {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:8000"}, nil
			})),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")),
	)
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}

	client = dynamodb.NewFromConfig(cfg)
	log.Println("Connected to DynamoDB Local")

	loadTestData()

	suite := BenchmarkSuite{Results: make([]BenchmarkResult, 0)}

	log.Println("\n=== Running DynamoDB Write Performance Benchmarks ===\n")

	suite.Results = append(suite.Results, benchmarkSingleWrites(1000))
	suite.Results = append(suite.Results, benchmarkBatchWrites(100, 25))
	suite.Results = append(suite.Results, benchmarkBatchWrites(10, 25))
	suite.Results = append(suite.Results, benchmarkConcurrentWrites(1000, 10))
	suite.Results = append(suite.Results, benchmarkConcurrentWrites(1000, 50))
	suite.Results = append(suite.Results, benchmarkTransactWrites(1000, 1))
	suite.Results = append(suite.Results, benchmarkTransactWrites(1000, 10))

	saveResults(suite, "benchmarks/results/dynamodb-write-results.json")
	printSummary(suite)
}

func loadTestData() {
	log.Println("Loading test data from DynamoDB...")

	// Scan for 100 accounts
	output, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName:        aws.String("FinancialTransactions"),
		FilterExpression: aws.String("#t = :type"),
		ExpressionAttributeNames: map[string]string{
			"#t": "Type",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":type": &types.AttributeValueMemberS{Value: "Account"},
		},
		Limit: aws.Int32(100),
	})

	if err == nil {
		for _, item := range output.Items {
			if id, ok := item["ID"].(*types.AttributeValueMemberS); ok {
				accountIDs = append(accountIDs, id.Value)
			}
		}
	}

	// Scan for 100 merchants
	output, err = client.Scan(ctx, &dynamodb.ScanInput{
		TableName:        aws.String("FinancialTransactions"),
		FilterExpression: aws.String("#t = :type"),
		ExpressionAttributeNames: map[string]string{
			"#t": "Type",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":type": &types.AttributeValueMemberS{Value: "Merchant"},
		},
		// Limit: aws.Int32(100),
	})

	log.Printf("Scan for merchants error: %v", output)

	if err == nil {
		for _, item := range output.Items {
			if id, ok := item["ID"].(*types.AttributeValueMemberS); ok {
				merchantIDs = append(merchantIDs, id.Value)
			}
		}
	}

	log.Printf("Loaded %d accounts and %d merchants", len(accountIDs), len(merchantIDs))

	// Validate that we have test data
	if len(accountIDs) == 0 || len(merchantIDs) == 0 {
		log.Fatal("\n❌ ERROR: No test data found in DynamoDB!\n\n" +
			"The table is empty. Please seed data first:\n" +
			"  1. Run: make seed-dynamodb\n" +
			"  2. Or: go run benchmarks/dynamodb/seed-data.go\n\n" +
			"This will create merchants, accounts, and transactions for benchmarking.\n")
	}
}

func benchmarkSingleWrites(count int) BenchmarkResult {
	log.Printf("Benchmarking single PutItem operations (%d operations)...", count)

	durations := make([]time.Duration, 0, count)
	successCount := 0
	errorCount := 0
	totalWCU := 0.0
	start := time.Now()

	for i := 0; i < count; i++ {
		opStart := time.Now()
		wcu, err := writeSingleTransaction()
		duration := time.Since(opStart)
		durations = append(durations, duration)

		if err != nil {
			errorCount++
		} else {
			successCount++
			totalWCU += wcu
		}
	}

	totalDuration := time.Since(start)
	return calculateResults("Single PutItem Writes", count, 1, durations, successCount, errorCount, totalDuration, totalWCU)
}

func benchmarkBatchWrites(numBatches, batchSize int) BenchmarkResult {
	testName := fmt.Sprintf("BatchWriteItem (%d batches of %d)", numBatches, batchSize)
	log.Printf("Benchmarking %s...", testName)

	durations := make([]time.Duration, 0, numBatches)
	successCount := 0
	errorCount := 0
	totalWCU := 0.0
	start := time.Now()

	for i := 0; i < numBatches; i++ {
		opStart := time.Now()
		wcu, err := writeBatch(batchSize)
		duration := time.Since(opStart)
		durations = append(durations, duration)

		if err != nil {
			errorCount++
		} else {
			successCount++
			totalWCU += wcu
		}
	}

	totalDuration := time.Since(start)
	return calculateResults(testName, numBatches*batchSize, 1, durations, successCount*batchSize, errorCount, totalDuration, totalWCU)
}

func benchmarkConcurrentWrites(opsPerGoroutine, numGoroutines int) BenchmarkResult {
	testName := fmt.Sprintf("Concurrent Writes (%d goroutines, %d ops each)", numGoroutines, opsPerGoroutine)
	log.Printf("Benchmarking %s...", testName)

	var wg sync.WaitGroup
	var mu sync.Mutex
	durations := make([]time.Duration, 0, opsPerGoroutine*numGoroutines)
	successCount := 0
	errorCount := 0
	totalWCU := 0.0

	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				opStart := time.Now()
				wcu, err := writeSingleTransaction()
				duration := time.Since(opStart)

				mu.Lock()
				durations = append(durations, duration)
				if err != nil {
					errorCount++
				} else {
					successCount++
					totalWCU += wcu
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	totalDuration := time.Since(start)

	return calculateResults(testName, opsPerGoroutine*numGoroutines, numGoroutines, durations, successCount, errorCount, totalDuration, totalWCU)
}

func benchmarkTransactWrites(count, concurrency int) BenchmarkResult {
	testName := fmt.Sprintf("TransactWriteItems (%d ops, %d concurrent)", count, concurrency)
	log.Printf("Benchmarking %s...", testName)

	var wg sync.WaitGroup
	var mu sync.Mutex
	durations := make([]time.Duration, 0, count)
	successCount := 0
	errorCount := 0
	totalWCU := 0.0

	opsPerGoroutine := count / concurrency
	start := time.Now()

	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				opStart := time.Now()
				wcu, err := writeTransactionalTransaction()
				duration := time.Since(opStart)

				mu.Lock()
				durations = append(durations, duration)
				if err != nil {
					errorCount++
				} else {
					successCount++
					totalWCU += wcu
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	totalDuration := time.Since(start)

	return calculateResults(testName, count, concurrency, durations, successCount, errorCount, totalDuration, totalWCU)
}

func writeSingleTransaction() (float64, error) {
	txnID := uuid.New().String()
	txn := Transaction{
		PK:              fmt.Sprintf("TXN#%s", txnID),
		SK:              "METADATA",
		GSI1PK:          "STATUS#completed",
		GSI1SK:          fmt.Sprintf("CREATED#%s", time.Now().Format(time.RFC3339Nano)),
		GSI2PK:          fmt.Sprintf("IDEMPOTENCY#%s", uuid.New().String()),
		GSI2SK:          "TXN",
		Type:            "Transaction",
		ID:              txnID,
		IdempotencyKey:  uuid.New().String(),
		TransactionType: "payment",
		Status:          "completed",
		MerchantID:      merchantIDs[rand.Intn(len(merchantIDs))],
		Description:     "Benchmark transaction",
		CreatedAt:       time.Now(),
	}

	item, err := attributevalue.MarshalMap(txn)
	if err != nil {
		return 0, err
	}

	output, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:              aws.String("FinancialTransactions"),
		Item:                   item,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})

	wcu := 0.0
	if output != nil && output.ConsumedCapacity != nil {
		wcu = *output.ConsumedCapacity.CapacityUnits
	}

	return wcu, err
}

func writeBatch(batchSize int) (float64, error) {
	requests := make([]types.WriteRequest, 0, batchSize)

	for i := 0; i < batchSize; i++ {
		txnID := uuid.New().String()
		txn := Transaction{
			PK:              fmt.Sprintf("TXN#%s", txnID),
			SK:              "METADATA",
			GSI1PK:          "STATUS#completed",
			GSI1SK:          fmt.Sprintf("CREATED#%s", time.Now().Format(time.RFC3339Nano)),
			Type:            "Transaction",
			ID:              txnID,
			TransactionType: "payment",
			Status:          "completed",
			CreatedAt:       time.Now(),
		}

		item, _ := attributevalue.MarshalMap(txn)
		requests = append(requests, types.WriteRequest{
			PutRequest: &types.PutRequest{Item: item},
		})
	}

	output, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"FinancialTransactions": requests,
		},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})

	wcu := 0.0
	if output != nil && len(output.ConsumedCapacity) > 0 {
		wcu = *output.ConsumedCapacity[0].CapacityUnits
	}

	return wcu, err
}

func writeTransactionalTransaction() (float64, error) {
	txnID := uuid.New().String()
	createdAt := time.Now()

	txn := Transaction{
		PK:              fmt.Sprintf("TXN#%s", txnID),
		SK:              "METADATA",
		Type:            "Transaction",
		ID:              txnID,
		TransactionType: "payment",
		Status:          "completed",
		CreatedAt:       createdAt,
	}

	debitLeg := TransactionLeg{
		PK:            fmt.Sprintf("TXN#%s", txnID),
		SK:            fmt.Sprintf("LEG#%s", uuid.New().String()),
		Type:          "TransactionLeg",
		TransactionID: txnID,
		AccountID:     accountIDs[rand.Intn(len(accountIDs))],
		LegType:       "debit",
		Amount:        decimal.NewFromFloat(rand.Float64() * 1000),
		Currency:      "USD",
		CreatedAt:     createdAt,
	}

	creditLeg := TransactionLeg{
		PK:            fmt.Sprintf("TXN#%s", txnID),
		SK:            fmt.Sprintf("LEG#%s", uuid.New().String()),
		Type:          "TransactionLeg",
		TransactionID: txnID,
		AccountID:     accountIDs[rand.Intn(len(accountIDs))],
		LegType:       "credit",
		Amount:        debitLeg.Amount,
		Currency:      "USD",
		CreatedAt:     createdAt,
	}

	txnItem, _ := attributevalue.MarshalMap(txn)
	debitItem, _ := attributevalue.MarshalMap(debitLeg)
	creditItem, _ := attributevalue.MarshalMap(creditLeg)

	output, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{Put: &types.Put{TableName: aws.String("FinancialTransactions"), Item: txnItem}},
			{Put: &types.Put{TableName: aws.String("FinancialTransactions"), Item: debitItem}},
			{Put: &types.Put{TableName: aws.String("FinancialTransactions"), Item: creditItem}},
		},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})

	wcu := 0.0
	if output != nil && len(output.ConsumedCapacity) > 0 {
		for _, cc := range output.ConsumedCapacity {
			if cc.CapacityUnits != nil {
				wcu += *cc.CapacityUnits
			}
		}
	}

	return wcu, err
}

func calculateResults(testName string, totalOps, concurrency int, durations []time.Duration, success, errors int, totalDuration time.Duration, totalWCU float64) BenchmarkResult {
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
		Database:         "DynamoDB",
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
		ConsumedWCU:      totalWCU,
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
		fmt.Printf("  Total WCU: %.2f\n", result.ConsumedWCU)
		fmt.Println()
	}
}
