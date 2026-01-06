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
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
	ConsumedRCU       float64       `json:"consumed_rcu"`
	ItemsReturned     int           `json:"items_returned"`
	Timestamp         time.Time     `json:"timestamp"`
}

type BenchmarkSuite struct {
	Results []BenchmarkResult `json:"results"`
}

var (
	client         *dynamodb.Client
	ctx            = context.Background()
	accountIDs     []string
	transactionIDs []string
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

	log.Println("\n=== Running DynamoDB Read Performance Benchmarks ===\n")

	// Point lookups
	suite.Results = append(suite.Results, benchmarkGetItem(1000, "transaction"))
	suite.Results = append(suite.Results, benchmarkGetItem(1000, "account"))

	// Batch reads
	suite.Results = append(suite.Results, benchmarkBatchGetItem(100, 10))
	suite.Results = append(suite.Results, benchmarkBatchGetItem(100, 25))

	// Query operations
	suite.Results = append(suite.Results, benchmarkQueryByStatus(100, 24))   // Last 24 hours
	suite.Results = append(suite.Results, benchmarkQueryByStatus(100, 720))  // Last 30 days
	suite.Results = append(suite.Results, benchmarkQueryAccountHistory(100, 100))

	// Concurrent reads
	suite.Results = append(suite.Results, benchmarkConcurrentReads(1000, 10))
	suite.Results = append(suite.Results, benchmarkConcurrentReads(1000, 50))
	suite.Results = append(suite.Results, benchmarkConcurrentReads(1000, 100))

	// Strongly consistent vs eventually consistent
	suite.Results = append(suite.Results, benchmarkConsistencyComparison(500))

	saveResults(suite, "benchmarks/results/dynamodb-read-results.json")
	printSummary(suite)
}

func loadTestData() {
	log.Println("Loading test data from DynamoDB...")

	// Scan for accounts
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

	// Scan for transactions
	output, err = client.Scan(ctx, &dynamodb.ScanInput{
		TableName:        aws.String("FinancialTransactions"),
		FilterExpression: aws.String("#t = :type"),
		ExpressionAttributeNames: map[string]string{
			"#t": "Type",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":type": &types.AttributeValueMemberS{Value: "Transaction"},
		},
		Limit: aws.Int32(1000),
	})

	if err == nil {
		for _, item := range output.Items {
			if id, ok := item["ID"].(*types.AttributeValueMemberS); ok {
				transactionIDs = append(transactionIDs, id.Value)
			}
		}
	}

	log.Printf("Loaded %d accounts and %d transactions", len(accountIDs), len(transactionIDs))

	// Validate that we have test data
	if len(accountIDs) == 0 || len(transactionIDs) == 0 {
		log.Fatal("\n❌ ERROR: No test data found in DynamoDB!\n\n" +
			"The table is empty. Please seed data first:\n" +
			"  1. Run: make seed-dynamodb\n" +
			"  2. Or: go run benchmarks/dynamodb/seed-data.go\n\n" +
			"This will create merchants, accounts, and transactions for benchmarking.\n")
	}
}

func benchmarkGetItem(count int, entityType string) BenchmarkResult {
	testName := fmt.Sprintf("GetItem - %s by ID", entityType)
	log.Printf("Benchmarking %s (%d operations)...", testName, count)

	durations := make([]time.Duration, 0, count)
	successCount := 0
	errorCount := 0
	totalRCU := 0.0
	itemsReturned := 0
	start := time.Now()

	for i := 0; i < count; i++ {
		opStart := time.Now()

		var pk, sk string
		if entityType == "transaction" {
			if len(transactionIDs) == 0 {
				errorCount++
				continue
			}
			txnID := transactionIDs[rand.Intn(len(transactionIDs))]
			pk = fmt.Sprintf("TXN#%s", txnID)
			sk = "METADATA"
		} else {
			if len(accountIDs) == 0 {
				errorCount++
				continue
			}
			accountID := accountIDs[rand.Intn(len(accountIDs))]
			pk = fmt.Sprintf("ACCOUNT#%s", accountID)
			sk = "METADATA"
		}

		output, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String("FinancialTransactions"),
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: pk},
				"SK": &types.AttributeValueMemberS{Value: sk},
			},
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		})

		duration := time.Since(opStart)
		durations = append(durations, duration)

		if err != nil {
			errorCount++
		} else {
			successCount++
			if output.Item != nil {
				itemsReturned++
			}
			if output.ConsumedCapacity != nil {
				totalRCU += *output.ConsumedCapacity.CapacityUnits
			}
		}
	}

	totalDuration := time.Since(start)
	return calculateResults(testName, count, 1, durations, successCount, errorCount, totalDuration, totalRCU, itemsReturned)
}

func benchmarkBatchGetItem(numBatches, batchSize int) BenchmarkResult {
	testName := fmt.Sprintf("BatchGetItem (%d batches of %d)", numBatches, batchSize)
	log.Printf("Benchmarking %s...", testName, numBatches, batchSize)

	if len(transactionIDs) < batchSize {
		log.Printf("Warning: Not enough transactions loaded for batch size %d", batchSize)
		return BenchmarkResult{TestName: testName, Database: "DynamoDB", ErrorCount: numBatches}
	}

	durations := make([]time.Duration, 0, numBatches)
	successCount := 0
	errorCount := 0
	totalRCU := 0.0
	itemsReturned := 0
	start := time.Now()

	for i := 0; i < numBatches; i++ {
		opStart := time.Now()

		// Build batch request
		keys := make([]map[string]types.AttributeValue, 0, batchSize)
		for j := 0; j < batchSize; j++ {
			txnID := transactionIDs[rand.Intn(len(transactionIDs))]
			keys = append(keys, map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("TXN#%s", txnID)},
				"SK": &types.AttributeValueMemberS{Value: "METADATA"},
			})
		}

		output, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
			RequestItems: map[string]types.KeysAndAttributes{
				"FinancialTransactions": {
					Keys: keys,
				},
			},
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		})

		duration := time.Since(opStart)
		durations = append(durations, duration)

		if err != nil {
			errorCount++
		} else {
			successCount++
			if items, ok := output.Responses["FinancialTransactions"]; ok {
				itemsReturned += len(items)
			}
			if len(output.ConsumedCapacity) > 0 {
				for _, cc := range output.ConsumedCapacity {
					if cc.CapacityUnits != nil {
						totalRCU += *cc.CapacityUnits
					}
				}
			}
		}
	}

	totalDuration := time.Since(start)
	return calculateResults(testName, numBatches*batchSize, 1, durations, successCount*batchSize, errorCount, totalDuration, totalRCU, itemsReturned)
}

func benchmarkQueryByStatus(count, hoursBack int) BenchmarkResult {
	testName := fmt.Sprintf("Query by Status (last %d hours)", hoursBack)
	log.Printf("Benchmarking %s (%d operations)...", testName, count)

	durations := make([]time.Duration, 0, count)
	successCount := 0
	errorCount := 0
	totalRCU := 0.0
	itemsReturned := 0
	start := time.Now()

	since := time.Now().Add(-time.Duration(hoursBack) * time.Hour)
	sinceStr := since.Format(time.RFC3339Nano)

	for i := 0; i < count; i++ {
		opStart := time.Now()

		output, err := client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String("FinancialTransactions"),
			IndexName:              aws.String("GSI1"),
			KeyConditionExpression: aws.String("GSI1PK = :status AND GSI1SK >= :since"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":status": &types.AttributeValueMemberS{Value: "STATUS#completed"},
				":since":  &types.AttributeValueMemberS{Value: fmt.Sprintf("CREATED#%s", sinceStr)},
			},
			Limit:                  aws.Int32(100),
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		})

		duration := time.Since(opStart)
		durations = append(durations, duration)

		if err != nil {
			errorCount++
		} else {
			successCount++
			itemsReturned += len(output.Items)
			if output.ConsumedCapacity != nil {
				totalRCU += *output.ConsumedCapacity.CapacityUnits
			}
		}
	}

	totalDuration := time.Since(start)
	return calculateResults(testName, count, 1, durations, successCount, errorCount, totalDuration, totalRCU, itemsReturned)
}

func benchmarkQueryAccountHistory(count, limit int) BenchmarkResult {
	testName := fmt.Sprintf("Query Account History (last %d items)", limit)
	log.Printf("Benchmarking %s (%d operations)...", testName, count)

	if len(accountIDs) == 0 {
		log.Println("Warning: No accounts loaded")
		return BenchmarkResult{TestName: testName, Database: "DynamoDB", ErrorCount: count}
	}

	durations := make([]time.Duration, 0, count)
	successCount := 0
	errorCount := 0
	totalRCU := 0.0
	itemsReturned := 0
	start := time.Now()

	for i := 0; i < count; i++ {
		opStart := time.Now()

		accountID := accountIDs[rand.Intn(len(accountIDs))]

		output, err := client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String("FinancialTransactions"),
			IndexName:              aws.String("GSI1"),
			KeyConditionExpression: aws.String("GSI1PK = :account AND begins_with(GSI1SK, :prefix)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":account": &types.AttributeValueMemberS{Value: fmt.Sprintf("ACCOUNT#%s", accountID)},
				":prefix":  &types.AttributeValueMemberS{Value: "LEG#"},
			},
			Limit:                  aws.Int32(int32(limit)),
			ScanIndexForward:       aws.Bool(false), // Descending order (newest first)
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		})

		duration := time.Since(opStart)
		durations = append(durations, duration)

		if err != nil {
			errorCount++
		} else {
			successCount++
			itemsReturned += len(output.Items)
			if output.ConsumedCapacity != nil {
				totalRCU += *output.ConsumedCapacity.CapacityUnits
			}
		}
	}

	totalDuration := time.Since(start)
	return calculateResults(testName, count, 1, durations, successCount, errorCount, totalDuration, totalRCU, itemsReturned)
}

func benchmarkConcurrentReads(opsPerGoroutine, numGoroutines int) BenchmarkResult {
	testName := fmt.Sprintf("Concurrent Reads (%d goroutines, %d ops each)", numGoroutines, opsPerGoroutine)
	log.Printf("Benchmarking %s...", testName)

	if len(transactionIDs) == 0 {
		log.Println("Warning: No transactions loaded")
		return BenchmarkResult{TestName: testName, Database: "DynamoDB", ErrorCount: opsPerGoroutine * numGoroutines}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	durations := make([]time.Duration, 0, opsPerGoroutine*numGoroutines)
	successCount := 0
	errorCount := 0
	totalRCU := 0.0
	itemsReturned := 0

	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				opStart := time.Now()

				txnID := transactionIDs[rand.Intn(len(transactionIDs))]
				output, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String("FinancialTransactions"),
					Key: map[string]types.AttributeValue{
						"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("TXN#%s", txnID)},
						"SK": &types.AttributeValueMemberS{Value: "METADATA"},
					},
					ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
				})

				duration := time.Since(opStart)

				mu.Lock()
				durations = append(durations, duration)
				if err != nil {
					errorCount++
				} else {
					successCount++
					if output.Item != nil {
						itemsReturned++
					}
					if output.ConsumedCapacity != nil {
						totalRCU += *output.ConsumedCapacity.CapacityUnits
					}
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	totalDuration := time.Since(start)

	return calculateResults(testName, opsPerGoroutine*numGoroutines, numGoroutines, durations, successCount, errorCount, totalDuration, totalRCU, itemsReturned)
}

func benchmarkConsistencyComparison(count int) BenchmarkResult {
	testName := "Strongly Consistent vs Eventually Consistent Reads"
	log.Printf("Benchmarking %s (%d operations each)...", testName, count)

	if len(transactionIDs) == 0 {
		log.Println("Warning: No transactions loaded")
		return BenchmarkResult{TestName: testName, Database: "DynamoDB", ErrorCount: count * 2}
	}

	// Eventually consistent reads
	eventualDurations := make([]time.Duration, 0, count)
	eventualRCU := 0.0

	for i := 0; i < count; i++ {
		opStart := time.Now()
		txnID := transactionIDs[rand.Intn(len(transactionIDs))]

		output, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String("FinancialTransactions"),
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("TXN#%s", txnID)},
				"SK": &types.AttributeValueMemberS{Value: "METADATA"},
			},
			ConsistentRead:         aws.Bool(false), // Eventually consistent (default)
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		})

		duration := time.Since(opStart)
		eventualDurations = append(eventualDurations, duration)

		if err == nil && output.ConsumedCapacity != nil {
			eventualRCU += *output.ConsumedCapacity.CapacityUnits
		}
	}

	// Strongly consistent reads
	strongDurations := make([]time.Duration, 0, count)
	strongRCU := 0.0

	for i := 0; i < count; i++ {
		opStart := time.Now()
		txnID := transactionIDs[rand.Intn(len(transactionIDs))]

		output, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String("FinancialTransactions"),
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("TXN#%s", txnID)},
				"SK": &types.AttributeValueMemberS{Value: "METADATA"},
			},
			ConsistentRead:         aws.Bool(true), // Strongly consistent
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		})

		duration := time.Since(opStart)
		strongDurations = append(strongDurations, duration)

		if err == nil && output.ConsumedCapacity != nil {
			strongRCU += *output.ConsumedCapacity.CapacityUnits
		}
	}

	// Calculate averages
	eventualAvg := calculateAverage(eventualDurations)
	strongAvg := calculateAverage(strongDurations)

	log.Printf("  Eventually Consistent: Avg=%.2fms, RCU=%.2f", float64(eventualAvg.Microseconds())/1000, eventualRCU)
	log.Printf("  Strongly Consistent:   Avg=%.2fms, RCU=%.2f (2x cost)", float64(strongAvg.Microseconds())/1000, strongRCU)

	// Return combined result
	allDurations := append(eventualDurations, strongDurations...)
	return calculateResults(testName, count*2, 1, allDurations, count*2, 0, eventualAvg+strongAvg, eventualRCU+strongRCU, count*2)
}

func calculateAverage(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range durations {
		sum += d
	}
	return sum / time.Duration(len(durations))
}

func calculateResults(testName string, totalOps, concurrency int, durations []time.Duration, success, errors int, totalDuration time.Duration, totalRCU float64, itemsReturned int) BenchmarkResult {
	if len(durations) == 0 {
		return BenchmarkResult{
			TestName:      testName,
			Database:      "DynamoDB",
			NumOperations: totalOps,
			ErrorCount:    errors,
			Timestamp:     time.Now(),
		}
	}

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
		ConsumedRCU:      totalRCU,
		ItemsReturned:    itemsReturned,
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
		fmt.Printf("  Total RCU: %.2f\n", result.ConsumedRCU)
		fmt.Printf("  Items Returned: %d\n", result.ItemsReturned)
		fmt.Println()
	}
}
