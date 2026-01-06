package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
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
	TotalDuration     time.Duration `json:"total_duration_ms"`
	AverageDuration   time.Duration `json:"avg_duration_ms"`
	OperationsPerSec  float64       `json:"operations_per_sec"`
	ConsumedRCU       float64       `json:"consumed_rcu"`
	ItemsScanned      int           `json:"items_scanned"`
	ItemsReturned     int           `json:"items_returned"`
	FilterEfficiency  float64       `json:"filter_efficiency_percent"`
	SuccessCount      int           `json:"success_count"`
	ErrorCount        int           `json:"error_count"`
	Timestamp         time.Time     `json:"timestamp"`
}

type BenchmarkSuite struct {
	Results []BenchmarkResult `json:"results"`
}

var (
	client *dynamodb.Client
	ctx    = context.Background()
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

	suite := BenchmarkSuite{Results: make([]BenchmarkResult, 0)}

	log.Println("\n=== Running DynamoDB Scan Performance Benchmarks ===\n")
	log.Println("NOTE: Scans are NOT recommended for production workloads!")
	log.Println("These benchmarks demonstrate why Query operations should be preferred.\n")

	// Full table scan (worst case)
	suite.Results = append(suite.Results, benchmarkFullTableScan())

	// Scan with filter (still inefficient)
	suite.Results = append(suite.Results, benchmarkScanWithFilter("Transaction"))
	suite.Results = append(suite.Results, benchmarkScanWithFilter("Account"))

	// Parallel scan (multiple segments)
	suite.Results = append(suite.Results, benchmarkParallelScan(4))
	suite.Results = append(suite.Results, benchmarkParallelScan(8))

	// Scan vs Query comparison
	suite.Results = append(suite.Results, benchmarkScanVsQueryComparison())

	// Count operations
	suite.Results = append(suite.Results, benchmarkCountScan())

	saveResults(suite, "benchmarks/results/dynamodb-scan-results.json")
	printSummary(suite)
	printBestPractices()
}

func benchmarkFullTableScan() BenchmarkResult {
	testName := "Full Table Scan (NO filter)"
	log.Printf("Benchmarking %s...", testName)

	start := time.Now()
	itemsScanned := 0
	totalRCU := 0.0
	errorCount := 0

	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		input := &dynamodb.ScanInput{
			TableName:              aws.String("FinancialTransactions"),
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		}

		if lastEvaluatedKey != nil {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		output, err := client.Scan(ctx, input)
		if err != nil {
			errorCount++
			log.Printf("Scan error: %v", err)
			break
		}

		itemsScanned += len(output.Items)

		if output.ConsumedCapacity != nil {
			totalRCU += *output.ConsumedCapacity.CapacityUnits
		}

		// Stop after scanning 10,000 items to avoid excessive time
		if itemsScanned >= 10000 || output.LastEvaluatedKey == nil {
			lastEvaluatedKey = nil
			break
		}

		lastEvaluatedKey = output.LastEvaluatedKey
	}

	totalDuration := time.Since(start)

	log.Printf("  Scanned %d items in %v (RCU: %.2f)", itemsScanned, totalDuration, totalRCU)
	log.Printf("  ⚠️  WARNING: Full table scans are very expensive and slow!")

	return BenchmarkResult{
		TestName:         testName,
		Database:         "DynamoDB",
		NumOperations:    1,
		TotalDuration:    totalDuration,
		AverageDuration:  totalDuration,
		OperationsPerSec: 1.0 / totalDuration.Seconds(),
		ConsumedRCU:      totalRCU,
		ItemsScanned:     itemsScanned,
		ItemsReturned:    itemsScanned,
		FilterEfficiency: 100.0,
		SuccessCount:     1,
		ErrorCount:       errorCount,
		Timestamp:        time.Now(),
	}
}

func benchmarkScanWithFilter(entityType string) BenchmarkResult {
	testName := fmt.Sprintf("Scan with FilterExpression (Type=%s)", entityType)
	log.Printf("Benchmarking %s...", testName)

	start := time.Now()
	itemsScanned := 0
	itemsReturned := 0
	totalRCU := 0.0
	errorCount := 0

	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		input := &dynamodb.ScanInput{
			TableName:        aws.String("FinancialTransactions"),
			FilterExpression: aws.String("#t = :type"),
			ExpressionAttributeNames: map[string]string{
				"#t": "Type",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":type": &types.AttributeValueMemberS{Value: entityType},
			},
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		}

		if lastEvaluatedKey != nil {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		output, err := client.Scan(ctx, input)
		if err != nil {
			errorCount++
			log.Printf("Scan error: %v", err)
			break
		}

		itemsReturned += len(output.Items)
		itemsScanned += int(output.ScannedCount)

		if output.ConsumedCapacity != nil {
			totalRCU += *output.ConsumedCapacity.CapacityUnits
		}

		// Stop after returning 1,000 items
		if itemsReturned >= 1000 || output.LastEvaluatedKey == nil {
			lastEvaluatedKey = nil
			break
		}

		lastEvaluatedKey = output.LastEvaluatedKey
	}

	totalDuration := time.Since(start)
	efficiency := 0.0
	if itemsScanned > 0 {
		efficiency = (float64(itemsReturned) / float64(itemsScanned)) * 100
	}

	log.Printf("  Scanned %d items, returned %d (%.1f%% efficiency)", itemsScanned, itemsReturned, efficiency)
	log.Printf("  Duration: %v, RCU: %.2f", totalDuration, totalRCU)
	log.Printf("  ⚠️  WARNING: You paid for ALL scanned items, not just returned items!")

	return BenchmarkResult{
		TestName:         testName,
		Database:         "DynamoDB",
		NumOperations:    1,
		TotalDuration:    totalDuration,
		AverageDuration:  totalDuration,
		OperationsPerSec: 1.0 / totalDuration.Seconds(),
		ConsumedRCU:      totalRCU,
		ItemsScanned:     itemsScanned,
		ItemsReturned:    itemsReturned,
		FilterEfficiency: efficiency,
		SuccessCount:     1,
		ErrorCount:       errorCount,
		Timestamp:        time.Now(),
	}
}

func benchmarkParallelScan(totalSegments int) BenchmarkResult {
	testName := fmt.Sprintf("Parallel Scan (%d segments)", totalSegments)
	log.Printf("Benchmarking %s...", testName)

	start := time.Now()
	itemsScanned := 0
	totalRCU := 0.0

	type segmentResult struct {
		items int
		rcu   float64
		err   error
	}

	results := make(chan segmentResult, totalSegments)

	// Launch parallel scan segments
	for segment := 0; segment < totalSegments; segment++ {
		go func(seg int) {
			segmentItems := 0
			segmentRCU := 0.0

			var lastEvaluatedKey map[string]types.AttributeValue

			for {
				input := &dynamodb.ScanInput{
					TableName:              aws.String("FinancialTransactions"),
					Segment:                aws.Int32(int32(seg)),
					TotalSegments:          aws.Int32(int32(totalSegments)),
					ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
				}

				if lastEvaluatedKey != nil {
					input.ExclusiveStartKey = lastEvaluatedKey
				}

				output, err := client.Scan(ctx, input)
				if err != nil {
					results <- segmentResult{err: err}
					return
				}

				segmentItems += len(output.Items)

				if output.ConsumedCapacity != nil {
					segmentRCU += *output.ConsumedCapacity.CapacityUnits
				}

				// Limit to 2500 items per segment
				if segmentItems >= 2500 || output.LastEvaluatedKey == nil {
					break
				}

				lastEvaluatedKey = output.LastEvaluatedKey
			}

			results <- segmentResult{items: segmentItems, rcu: segmentRCU, err: nil}
		}(segment)
	}

	// Collect results from all segments
	errorCount := 0
	for i := 0; i < totalSegments; i++ {
		result := <-results
		if result.err != nil {
			errorCount++
			log.Printf("Segment error: %v", result.err)
		} else {
			itemsScanned += result.items
			totalRCU += result.rcu
		}
	}

	totalDuration := time.Since(start)

	log.Printf("  Scanned %d items across %d segments in %v", itemsScanned, totalSegments, totalDuration)
	log.Printf("  Total RCU: %.2f (%.2f RCU per segment)", totalRCU, totalRCU/float64(totalSegments))
	log.Printf("  ✅ Parallel scans are faster but still consume same RCU as sequential")

	return BenchmarkResult{
		TestName:         testName,
		Database:         "DynamoDB",
		NumOperations:    totalSegments,
		TotalDuration:    totalDuration,
		AverageDuration:  totalDuration / time.Duration(totalSegments),
		OperationsPerSec: float64(totalSegments) / totalDuration.Seconds(),
		ConsumedRCU:      totalRCU,
		ItemsScanned:     itemsScanned,
		ItemsReturned:    itemsScanned,
		FilterEfficiency: 100.0,
		SuccessCount:     totalSegments - errorCount,
		ErrorCount:       errorCount,
		Timestamp:        time.Now(),
	}
}

func benchmarkScanVsQueryComparison() BenchmarkResult {
	testName := "Scan vs Query Performance Comparison"
	log.Printf("Benchmarking %s...", testName)

	// First: Scan approach
	log.Println("\n  Approach 1: SCAN with FilterExpression")
	scanStart := time.Now()
	scanItems := 0
	scanRCU := 0.0

	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		output, err := client.Scan(ctx, &dynamodb.ScanInput{
			TableName:        aws.String("FinancialTransactions"),
			FilterExpression: aws.String("GSI1PK = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "STATUS#completed"},
			},
			ExclusiveStartKey:      lastEvaluatedKey,
			Limit:                  aws.Int32(100),
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		})

		if err != nil {
			log.Printf("Scan error: %v", err)
			break
		}

		scanItems += len(output.Items)
		if output.ConsumedCapacity != nil {
			scanRCU += *output.ConsumedCapacity.CapacityUnits
		}

		if len(output.Items) >= 100 || output.LastEvaluatedKey == nil {
			break
		}

		lastEvaluatedKey = output.LastEvaluatedKey
	}

	scanDuration := time.Since(scanStart)
	log.Printf("    Items: %d, Duration: %v, RCU: %.2f", scanItems, scanDuration, scanRCU)

	// Second: Query approach (proper way)
	log.Println("\n  Approach 2: QUERY on GSI1")
	queryStart := time.Now()

	output, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("FinancialTransactions"),
		IndexName:              aws.String("GSI1"),
		KeyConditionExpression: aws.String("GSI1PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "STATUS#completed"},
		},
		Limit:                  aws.Int32(100),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})

	queryDuration := time.Since(queryStart)
	queryItems := 0
	queryRCU := 0.0

	if err != nil {
		log.Printf("Query error: %v", err)
	} else {
		queryItems = len(output.Items)
		if output.ConsumedCapacity != nil {
			queryRCU = *output.ConsumedCapacity.CapacityUnits
		}
		log.Printf("    Items: %d, Duration: %v, RCU: %.2f", queryItems, queryDuration, queryRCU)
	}

	// Comparison
	log.Println("\n  📊 COMPARISON:")
	if queryDuration > 0 {
		speedup := float64(scanDuration) / float64(queryDuration)
		rcuSavings := ((scanRCU - queryRCU) / scanRCU) * 100
		log.Printf("    Speed: Query is %.1fx FASTER", speedup)
		log.Printf("    Cost: Query saves %.1f%% RCU", rcuSavings)
	}
	log.Println("    ✅ ALWAYS use Query instead of Scan when possible!")

	return BenchmarkResult{
		TestName:         testName,
		Database:         "DynamoDB",
		NumOperations:    2,
		TotalDuration:    scanDuration + queryDuration,
		AverageDuration:  (scanDuration + queryDuration) / 2,
		OperationsPerSec: 2.0 / (scanDuration + queryDuration).Seconds(),
		ConsumedRCU:      scanRCU + queryRCU,
		ItemsScanned:     scanItems + queryItems,
		ItemsReturned:    scanItems + queryItems,
		FilterEfficiency: 100.0,
		SuccessCount:     2,
		ErrorCount:       0,
		Timestamp:        time.Now(),
	}
}

func benchmarkCountScan() BenchmarkResult {
	testName := "Count Scan (Get total item count)"
	log.Printf("Benchmarking %s...", testName)

	start := time.Now()
	totalCount := 0
	totalRCU := 0.0

	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		output, err := client.Scan(ctx, &dynamodb.ScanInput{
			TableName:              aws.String("FinancialTransactions"),
			Select:                 types.SelectCount, // Only count, don't return items
			ExclusiveStartKey:      lastEvaluatedKey,
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		})

		if err != nil {
			log.Printf("Count scan error: %v", err)
			break
		}

		totalCount += int(output.Count)

		if output.ConsumedCapacity != nil {
			totalRCU += *output.ConsumedCapacity.CapacityUnits
		}

		if output.LastEvaluatedKey == nil {
			break
		}

		lastEvaluatedKey = output.LastEvaluatedKey
	}

	totalDuration := time.Since(start)

	log.Printf("  Total items: %d", totalCount)
	log.Printf("  Duration: %v, RCU: %.2f", totalDuration, totalRCU)
	log.Printf("  ⚠️  Count scans still consume RCU for every item!")
	log.Printf("  💡 TIP: Maintain a separate counter item for O(1) counts")

	return BenchmarkResult{
		TestName:         testName,
		Database:         "DynamoDB",
		NumOperations:    1,
		TotalDuration:    totalDuration,
		AverageDuration:  totalDuration,
		OperationsPerSec: 1.0 / totalDuration.Seconds(),
		ConsumedRCU:      totalRCU,
		ItemsScanned:     totalCount,
		ItemsReturned:    0, // Count doesn't return items
		FilterEfficiency: 0,
		SuccessCount:     1,
		ErrorCount:       0,
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
		fmt.Printf("  Items Scanned: %d, Returned: %d\n", result.ItemsScanned, result.ItemsReturned)
		if result.FilterEfficiency > 0 {
			fmt.Printf("  Filter Efficiency: %.1f%%\n", result.FilterEfficiency)
		}
		fmt.Printf("  Total RCU: %.2f\n", result.ConsumedRCU)
		fmt.Println()
	}
}

func printBestPractices() {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("DynamoDB SCAN BEST PRACTICES")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("\n❌ AVOID Scans when possible:")
	fmt.Println("  • Scans read EVERY item in the table (expensive)")
	fmt.Println("  • FilterExpression is applied AFTER reading (you still pay for scanned items)")
	fmt.Println("  • Scans are slow and unpredictable for large tables")
	fmt.Println("  • Scans consume RCU even for filtered-out items")

	fmt.Println("\n✅ USE Queries instead:")
	fmt.Println("  • Design partition keys for your access patterns")
	fmt.Println("  • Use Global Secondary Indexes (GSI) for alternate queries")
	fmt.Println("  • Queries are typically 10-100x faster than scans")
	fmt.Println("  • Queries only consume RCU for returned items")

	fmt.Println("\n💡 When Scans are acceptable:")
	fmt.Println("  • Batch/background jobs that process entire dataset")
	fmt.Println("  • Data exports or migrations")
	fmt.Println("  • Analytics on small tables (< 10,000 items)")
	fmt.Println("  • Use parallel scans for large tables (see benchmark above)")

	fmt.Println("\n🔧 Optimization techniques:")
	fmt.Println("  • Maintain aggregate/count items instead of scanning")
	fmt.Println("  • Use DynamoDB Streams + Lambda for real-time aggregation")
	fmt.Println("  • Export to S3 + Athena for complex analytics")
	fmt.Println("  • Consider PostgreSQL if your workload requires frequent scans")

	fmt.Println("\n💰 Cost comparison (example):")
	fmt.Println("  • Query 100 items: ~0.05 RCU = $0.000013")
	fmt.Println("  • Scan 100,000 items to find 100: ~50 RCU = $0.013")
	fmt.Println("  • Difference: 1000x more expensive!")

	fmt.Println("\n" + strings.Repeat("=", 80) + "\n")
}
