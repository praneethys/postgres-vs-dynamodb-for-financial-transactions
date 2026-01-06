package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/shopspring/decimal"
)

const (
	NumMerchants = 1000
	NumAccounts  = 10000
	NumTransactions = 100000
)

var (
	merchantCategories = []string{"Restaurant", "Retail", "Gas Station", "Grocery", "Entertainment", "Travel", "Healthcare", "Utility"}
	accountTypes = []string{"checking", "savings", "credit"}
	transactionTypes = []string{"payment", "transfer", "refund", "fee"}
	currencies = []string{"USD", "EUR", "GBP"}
)

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

	// Seed in order due to foreign key constraints
	merchantIDs := seedMerchants(db)
	log.Printf("Created %d merchants", len(merchantIDs))

	accountIDs := seedAccounts(db)
	log.Printf("Created %d accounts", len(accountIDs))

	seedTransactions(db, accountIDs, merchantIDs)
	log.Printf("Created %d transactions", NumTransactions)

	log.Println("Seeding completed successfully!")
}

func seedMerchants(db *sql.DB) []uuid.UUID {
	log.Println("Seeding merchants...")
	merchantIDs := make([]uuid.UUID, 0, NumMerchants)

	stmt, err := db.Prepare(`
		INSERT INTO merchants (id, name, category)
		VALUES ($1, $2, $3)
	`)
	if err != nil {
		log.Fatal("Failed to prepare merchant insert:", err)
	}
	defer stmt.Close()

	for i := 0; i < NumMerchants; i++ {
		id := uuid.New()
		name := fmt.Sprintf("Merchant_%d", i)
		category := merchantCategories[rand.Intn(len(merchantCategories))]

		_, err := stmt.Exec(id, name, category)
		if err != nil {
			log.Printf("Failed to insert merchant: %v", err)
			continue
		}

		merchantIDs = append(merchantIDs, id)

		if (i+1)%100 == 0 {
			log.Printf("Created %d merchants...", i+1)
		}
	}

	return merchantIDs
}

func seedAccounts(db *sql.DB) []uuid.UUID {
	log.Println("Seeding accounts...")
	accountIDs := make([]uuid.UUID, 0, NumAccounts)

	stmt, err := db.Prepare(`
		INSERT INTO accounts (id, user_id, account_type, currency, balance, status)
		VALUES ($1, $2, $3, $4, $5, $6)
	`)
	if err != nil {
		log.Fatal("Failed to prepare account insert:", err)
	}
	defer stmt.Close()

	for i := 0; i < NumAccounts; i++ {
		id := uuid.New()
		userID := uuid.New()
		accountType := accountTypes[rand.Intn(len(accountTypes))]
		currency := currencies[rand.Intn(len(currencies))]
		balance := decimal.NewFromFloat(rand.Float64() * 10000)
		status := "active"

		_, err := stmt.Exec(id, userID, accountType, currency, balance, status)
		if err != nil {
			log.Printf("Failed to insert account: %v", err)
			continue
		}

		accountIDs = append(accountIDs, id)

		if (i+1)%1000 == 0 {
			log.Printf("Created %d accounts...", i+1)
		}
	}

	return accountIDs
}

func seedTransactions(db *sql.DB, accountIDs, merchantIDs []uuid.UUID) {
	log.Println("Seeding transactions...")

	for i := 0; i < NumTransactions; i++ {
		tx, err := db.Begin()
		if err != nil {
			log.Printf("Failed to begin transaction: %v", err)
			continue
		}

		// Create transaction header
		txnID := uuid.New()
		idempotencyKey := uuid.New().String()
		txnType := transactionTypes[rand.Intn(len(transactionTypes))]
		merchantID := merchantIDs[rand.Intn(len(merchantIDs))]
		status := "completed"
		description := fmt.Sprintf("Transaction %d", i)

		createdAt := time.Now().Add(-time.Duration(rand.Intn(90)) * 24 * time.Hour)

		_, err = tx.Exec(`
			INSERT INTO transactions (id, idempotency_key, transaction_type, status, merchant_id, description, created_at, completed_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, txnID, idempotencyKey, txnType, status, merchantID, description, createdAt, createdAt)

		if err != nil {
			tx.Rollback()
			log.Printf("Failed to insert transaction: %v", err)
			continue
		}

		// Create transaction legs (double-entry)
		amount := decimal.NewFromFloat(rand.Float64() * 1000)
		currency := currencies[rand.Intn(len(currencies))]

		// Debit leg
		debitAccount := accountIDs[rand.Intn(len(accountIDs))]
		_, err = tx.Exec(`
			INSERT INTO transaction_legs (transaction_id, account_id, leg_type, amount, currency, created_at)
			VALUES ($1, $2, 'debit', $3, $4, $5)
		`, txnID, debitAccount, amount, currency, createdAt)

		if err != nil {
			tx.Rollback()
			log.Printf("Failed to insert debit leg: %v", err)
			continue
		}

		// Credit leg
		creditAccount := accountIDs[rand.Intn(len(accountIDs))]
		_, err = tx.Exec(`
			INSERT INTO transaction_legs (transaction_id, account_id, leg_type, amount, currency, created_at)
			VALUES ($1, $2, 'credit', $3, $4, $5)
		`, txnID, creditAccount, amount, currency, createdAt)

		if err != nil {
			tx.Rollback()
			log.Printf("Failed to insert credit leg: %v", err)
			continue
		}

		if err := tx.Commit(); err != nil {
			log.Printf("Failed to commit transaction: %v", err)
			continue
		}

		if (i+1)%1000 == 0 {
			log.Printf("Created %d transactions...", i+1)
		}
	}
}
