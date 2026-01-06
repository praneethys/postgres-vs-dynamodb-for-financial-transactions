package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
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

const (
	NumMerchants    = 1000
	NumAccounts     = 10000
	NumTransactions = 100000
	BatchSize       = 25 // DynamoDB batch write limit
)

var (
	merchantCategories = []string{"Restaurant", "Retail", "Gas Station", "Grocery", "Entertainment", "Travel", "Healthcare", "Utility"}
	accountTypes       = []string{"checking", "savings", "credit"}
	transactionTypes   = []string{"payment", "transfer", "refund", "fee"}
	currencies         = []string{"USD", "EUR", "GBP"}
)

type Merchant struct {
	PK        string    `dynamodbav:"PK"`
	SK        string    `dynamodbav:"SK"`
	Type      string    `dynamodbav:"Type"`
	ID        string    `dynamodbav:"ID"`
	Name      string    `dynamodbav:"Name"`
	Category  string    `dynamodbav:"Category"`
	CreatedAt time.Time `dynamodbav:"CreatedAt"`
}

type Account struct {
	PK          string          `dynamodbav:"PK"`
	SK          string          `dynamodbav:"SK"`
	GSI1PK      string          `dynamodbav:"GSI1PK"`
	GSI1SK      string          `dynamodbav:"GSI1SK"`
	Type        string          `dynamodbav:"Type"`
	ID          string          `dynamodbav:"ID"`
	UserID      string          `dynamodbav:"UserID"`
	AccountType string          `dynamodbav:"AccountType"`
	Currency    string          `dynamodbav:"Currency"`
	Balance     decimal.Decimal `dynamodbav:"Balance"`
	Status      string          `dynamodbav:"Status"`
	Version     int             `dynamodbav:"Version"`
	CreatedAt   time.Time       `dynamodbav:"CreatedAt"`
	UpdatedAt   time.Time       `dynamodbav:"UpdatedAt"`
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
	UpdatedAt       time.Time `dynamodbav:"UpdatedAt"`
	CompletedAt     time.Time `dynamodbav:"CompletedAt"`
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
	BalanceAfter  decimal.Decimal `dynamodbav:"BalanceAfter"`
	CreatedAt     time.Time       `dynamodbav:"CreatedAt"`
}

func main() {
	ctx := context.Background()

	// Configure AWS SDK for local DynamoDB
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

	client := dynamodb.NewFromConfig(cfg)

	log.Println("Connected to DynamoDB Local")

	// Seed data
	merchantIDs := seedMerchants(ctx, client)
	log.Printf("Created %d merchants", len(merchantIDs))

	accountIDs := seedAccounts(ctx, client)
	log.Printf("Created %d accounts", len(accountIDs))

	seedTransactions(ctx, client, accountIDs, merchantIDs)
	log.Printf("Created %d transactions", NumTransactions)

	log.Println("Seeding completed successfully!")
}

func seedMerchants(ctx context.Context, client *dynamodb.Client) []string {
	log.Println("Seeding merchants...")
	merchantIDs := make([]string, 0, NumMerchants)
	items := make([]types.WriteRequest, 0, BatchSize)

	for i := 0; i < NumMerchants; i++ {
		id := uuid.New().String()
		merchant := Merchant{
			PK:        fmt.Sprintf("MERCHANT#%s", id),
			SK:        "METADATA",
			Type:      "Merchant",
			ID:        id,
			Name:      fmt.Sprintf("Merchant_%d", i),
			Category:  merchantCategories[rand.Intn(len(merchantCategories))],
			CreatedAt: time.Now(),
		}

		item, err := attributevalue.MarshalMap(merchant)
		if err != nil {
			log.Printf("Failed to marshal merchant: %v", err)
			continue
		}

		items = append(items, types.WriteRequest{
			PutRequest: &types.PutRequest{Item: item},
		})

		merchantIDs = append(merchantIDs, id)

		if len(items) == BatchSize || i == NumMerchants-1 {
			_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"FinancialTransactions": items,
				},
			})
			if err != nil {
				log.Printf("Failed to batch write merchants: %v", err)
			}
			items = make([]types.WriteRequest, 0, BatchSize)

			if (i+1)%100 == 0 {
				log.Printf("Created %d merchants...", i+1)
			}
		}
	}

	return merchantIDs
}

func seedAccounts(ctx context.Context, client *dynamodb.Client) []string {
	log.Println("Seeding accounts...")
	accountIDs := make([]string, 0, NumAccounts)
	items := make([]types.WriteRequest, 0, BatchSize)

	for i := 0; i < NumAccounts; i++ {
		id := uuid.New().String()
		userID := uuid.New().String()
		account := Account{
			PK:          fmt.Sprintf("ACCOUNT#%s", id),
			SK:          "METADATA",
			GSI1PK:      fmt.Sprintf("USER#%s", userID),
			GSI1SK:      fmt.Sprintf("ACCOUNT#%s", id),
			Type:        "Account",
			ID:          id,
			UserID:      userID,
			AccountType: accountTypes[rand.Intn(len(accountTypes))],
			Currency:    currencies[rand.Intn(len(currencies))],
			Balance:     decimal.NewFromFloat(rand.Float64() * 10000),
			Status:      "active",
			Version:     0,
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		}

		item, err := attributevalue.MarshalMap(account)
		if err != nil {
			log.Printf("Failed to marshal account: %v", err)
			continue
		}

		items = append(items, types.WriteRequest{
			PutRequest: &types.PutRequest{Item: item},
		})

		accountIDs = append(accountIDs, id)

		if len(items) == BatchSize || i == NumAccounts-1 {
			_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"FinancialTransactions": items,
				},
			})
			if err != nil {
				log.Printf("Failed to batch write accounts: %v", err)
			}
			items = make([]types.WriteRequest, 0, BatchSize)

			if (i+1)%1000 == 0 {
				log.Printf("Created %d accounts...", i+1)
			}
		}
	}

	return accountIDs
}

func seedTransactions(ctx context.Context, client *dynamodb.Client, accountIDs, merchantIDs []string) {
	log.Println("Seeding transactions...")

	for i := 0; i < NumTransactions; i++ {
		txnID := uuid.New().String()
		idempotencyKey := uuid.New().String()
		merchantID := merchantIDs[rand.Intn(len(merchantIDs))]
		createdAt := time.Now().Add(-time.Duration(rand.Intn(90)) * 24 * time.Hour)

		// Create transaction header
		txn := Transaction{
			PK:              fmt.Sprintf("TXN#%s", txnID),
			SK:              "METADATA",
			GSI1PK:          "STATUS#completed",
			GSI1SK:          fmt.Sprintf("CREATED#%s", createdAt.Format(time.RFC3339Nano)),
			GSI2PK:          fmt.Sprintf("IDEMPOTENCY#%s", idempotencyKey),
			GSI2SK:          "TXN",
			Type:            "Transaction",
			ID:              txnID,
			IdempotencyKey:  idempotencyKey,
			TransactionType: transactionTypes[rand.Intn(len(transactionTypes))],
			Status:          "completed",
			MerchantID:      merchantID,
			Description:     fmt.Sprintf("Transaction %d", i),
			CreatedAt:       createdAt,
			UpdatedAt:       createdAt,
			CompletedAt:     createdAt,
		}

		// Create transaction legs
		amount := decimal.NewFromFloat(rand.Float64() * 1000)
		currency := currencies[rand.Intn(len(currencies))]
		debitAccountID := accountIDs[rand.Intn(len(accountIDs))]
		creditAccountID := accountIDs[rand.Intn(len(accountIDs))]

		debitLeg := TransactionLeg{
			PK:            fmt.Sprintf("TXN#%s", txnID),
			SK:            fmt.Sprintf("LEG#%s", uuid.New().String()),
			GSI1PK:        fmt.Sprintf("ACCOUNT#%s", debitAccountID),
			GSI1SK:        fmt.Sprintf("LEG#%s#%s", createdAt.Format(time.RFC3339Nano), txnID),
			Type:          "TransactionLeg",
			ID:            uuid.New().String(),
			TransactionID: txnID,
			AccountID:     debitAccountID,
			LegType:       "debit",
			Amount:        amount,
			Currency:      currency,
			CreatedAt:     createdAt,
		}

		creditLeg := TransactionLeg{
			PK:            fmt.Sprintf("TXN#%s", txnID),
			SK:            fmt.Sprintf("LEG#%s", uuid.New().String()),
			GSI1PK:        fmt.Sprintf("ACCOUNT#%s", creditAccountID),
			GSI1SK:        fmt.Sprintf("LEG#%s#%s", createdAt.Format(time.RFC3339Nano), txnID),
			Type:          "TransactionLeg",
			ID:            uuid.New().String(),
			TransactionID: txnID,
			AccountID:     creditAccountID,
			LegType:       "credit",
			Amount:        amount,
			Currency:      currency,
			CreatedAt:     createdAt,
		}

		// Batch write all items
		txnItem, _ := attributevalue.MarshalMap(txn)
		debitItem, _ := attributevalue.MarshalMap(debitLeg)
		creditItem, _ := attributevalue.MarshalMap(creditLeg)

		_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				"FinancialTransactions": {
					{PutRequest: &types.PutRequest{Item: txnItem}},
					{PutRequest: &types.PutRequest{Item: debitItem}},
					{PutRequest: &types.PutRequest{Item: creditItem}},
				},
			},
		})

		if err != nil {
			log.Printf("Failed to write transaction: %v", err)
			continue
		}

		if (i+1)%1000 == 0 {
			log.Printf("Created %d transactions...", i+1)
		}
	}
}
