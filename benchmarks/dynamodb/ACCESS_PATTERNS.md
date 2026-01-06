# DynamoDB Single-Table Design - Access Patterns

This document describes the access patterns for the `FinancialTransactions` table.

## Table Structure

**Table Name:** `FinancialTransactions`

**Primary Key:**
- PK (Partition Key) - String
- SK (Sort Key) - String

**Global Secondary Indexes:**
- **GSI1**: GSI1PK (partition), GSI1SK (sort)
- **GSI2**: GSI2PK (partition), GSI2SK (sort)

---

## Entity Patterns

### 1. Merchants

**Storage Pattern:**
```
PK: MERCHANT#<merchant_id>
SK: METADATA
```

**Access Pattern:**
- Get merchant details by ID

**Example:**
```json
{
  "PK": "MERCHANT#550e8400-e29b-41d4-a716-446655440000",
  "SK": "METADATA",
  "Type": "Merchant",
  "ID": "550e8400-e29b-41d4-a716-446655440000",
  "Name": "Coffee Shop Inc",
  "Category": "Restaurant"
}
```

**Query:**
```javascript
GetItem({
  PK: "MERCHANT#550e8400...",
  SK: "METADATA"
})
```

---

### 2. Accounts

**Storage Pattern:**
```
PK: ACCOUNT#<account_id>
SK: METADATA
GSI1PK: USER#<user_id>
GSI1SK: ACCOUNT#<account_id>
```

**Access Patterns:**
- Get account details by ID (primary key)
- Query all accounts for a user (via GSI1)

**Example:**
```json
{
  "PK": "ACCOUNT#660f9511-e29b-41d4-a716-446655440000",
  "SK": "METADATA",
  "GSI1PK": "USER#770g0622-e29b-41d4-a716-446655440000",
  "GSI1SK": "ACCOUNT#660f9511...",
  "Type": "Account",
  "ID": "660f9511...",
  "UserID": "770g0622...",
  "AccountType": "checking",
  "Balance": 1500.00,
  "Currency": "USD",
  "Status": "active"
}
```

**Queries:**
```javascript
// Get account by ID
GetItem({
  PK: "ACCOUNT#660f9511...",
  SK: "METADATA"
})

// Get all accounts for a user
Query({
  IndexName: "GSI1",
  KeyConditionExpression: "GSI1PK = :user",
  ExpressionAttributeValues: {
    ":user": "USER#770g0622..."
  }
})
```

---

### 3. Transactions

**Storage Pattern:**
```
PK: TXN#<transaction_id>
SK: METADATA
GSI1PK: STATUS#<status>
GSI1SK: CREATED#<timestamp>
GSI2PK: IDEMPOTENCY#<idempotency_key>
GSI2SK: TXN
```

**Access Patterns:**
- Get transaction details by ID (primary key)
- Query transactions by status and time (via GSI1)
- Check idempotency key (via GSI2)

**Example:**
```json
{
  "PK": "TXN#880h1733-e29b-41d4-a716-446655440000",
  "SK": "METADATA",
  "GSI1PK": "STATUS#completed",
  "GSI1SK": "CREATED#2026-01-02T10:30:00.123Z",
  "GSI2PK": "IDEMPOTENCY#abc123def456",
  "GSI2SK": "TXN",
  "Type": "Transaction",
  "ID": "880h1733...",
  "IdempotencyKey": "abc123def456",
  "TransactionType": "payment",
  "Status": "completed",
  "MerchantID": "550e8400...",
  "Description": "Coffee purchase",
  "Amount": 5.50,
  "CreatedAt": "2026-01-02T10:30:00.123Z"
}
```

**Queries:**
```javascript
// Get transaction by ID
GetItem({
  PK: "TXN#880h1733...",
  SK: "METADATA"
})

// Get completed transactions in last 24 hours
Query({
  IndexName: "GSI1",
  KeyConditionExpression: "GSI1PK = :status AND GSI1SK >= :since",
  ExpressionAttributeValues: {
    ":status": "STATUS#completed",
    ":since": "CREATED#2026-01-01T10:30:00.000Z"
  }
})

// Check if idempotency key exists
Query({
  IndexName: "GSI2",
  KeyConditionExpression: "GSI2PK = :key",
  ExpressionAttributeValues: {
    ":key": "IDEMPOTENCY#abc123def456"
  }
})
```

---

### 4. Transaction Legs (Double-Entry)

**Storage Pattern:**
```
PK: TXN#<transaction_id>
SK: LEG#<leg_id>
GSI1PK: ACCOUNT#<account_id>
GSI1SK: LEG#<timestamp>#<leg_id>
```

**Access Patterns:**
- Get all legs for a transaction (primary key query)
- Query legs by account (via GSI1)

**Example:**
```json
{
  "PK": "TXN#880h1733-e29b-41d4-a716-446655440000",
  "SK": "LEG#990i2844-e29b-41d4-a716-446655440000",
  "GSI1PK": "ACCOUNT#660f9511-e29b-41d4-a716-446655440000",
  "GSI1SK": "LEG#2026-01-02T10:30:00.123Z#990i2844...",
  "Type": "TransactionLeg",
  "ID": "990i2844...",
  "TransactionID": "880h1733...",
  "AccountID": "660f9511...",
  "LegType": "debit",
  "Amount": 5.50,
  "Currency": "USD",
  "CreatedAt": "2026-01-02T10:30:00.123Z"
}
```

**Queries:**
```javascript
// Get all legs for a transaction (item collection)
Query({
  KeyConditionExpression: "PK = :txn",
  ExpressionAttributeValues: {
    ":txn": "TXN#880h1733..."
  }
})

// Get transaction history for account
Query({
  IndexName: "GSI1",
  KeyConditionExpression: "GSI1PK = :account AND begins_with(GSI1SK, :prefix)",
  ExpressionAttributeValues: {
    ":account": "ACCOUNT#660f9511...",
    ":prefix": "LEG#"
  },
  ScanIndexForward: false,  // Descending order (newest first)
  Limit: 100
})
```

---

## Key Design Principles

### 1. Item Collections
Transaction legs are stored with the same partition key as their parent transaction:
- **PK:** `TXN#880h1733...`
- **SK:** `LEG#990i2844...` (child 1)
- **SK:** `LEG#aa1j3955...` (child 2)

This allows fetching a transaction and all its legs in a single Query operation.

### 2. Composite Sort Keys
Sort keys use compound values for flexible querying:
- `LEG#<timestamp>#<id>` - Enables time-based sorting
- `CREATED#<timestamp>` - Enables range queries on creation time

### 3. Denormalization
Store frequently accessed data together to minimize reads:
- Transaction amount stored in both transaction header and legs
- Account balance cached in account item (updated atomically)

### 4. Access Pattern Coverage
- **GSI1:** Time-series queries (status + timestamp, account history)
- **GSI2:** Idempotency key lookups
- **Base Table:** Entity lookups by ID, transaction + legs collection

---

## Example Workflows

### Payment Processing
```javascript
// 1. Check idempotency
const existing = await Query(GSI2, IDEMPOTENCY#key)
if (existing) return existing

// 2. Create transaction + legs atomically
await TransactWriteItems([
  { Put: transaction },  // TXN#id METADATA
  { Put: debitLeg },     // TXN#id LEG#leg1
  { Put: creditLeg },    // TXN#id LEG#leg2
  { Update: debitAccount },  // Decrement balance
  { Update: creditAccount }  // Increment balance
])

// 3. Query succeeds if all items written atomically
```

### Account Statement
```javascript
// Get last 100 transactions for account
const history = await Query({
  IndexName: "GSI1",
  KeyConditionExpression: "GSI1PK = :account AND begins_with(GSI1SK, :prefix)",
  ExpressionAttributeValues: {
    ":account": "ACCOUNT#660f9511...",
    ":prefix": "LEG#"
  },
  ScanIndexForward: false,
  Limit: 100
})
```

### Reconciliation Report
```javascript
// Get all completed transactions in date range
const transactions = await Query({
  IndexName: "GSI1",
  KeyConditionExpression: "GSI1PK = :status AND GSI1SK BETWEEN :start AND :end",
  ExpressionAttributeValues: {
    ":status": "STATUS#completed",
    ":start": "CREATED#2026-01-01T00:00:00.000Z",
    ":end": "CREATED#2026-01-31T23:59:59.999Z"
  }
})

// For each transaction, get legs
for (const txn of transactions) {
  const legs = await Query({
    KeyConditionExpression: "PK = :txn",
    ExpressionAttributeValues: {
      ":txn": txn.PK
    }
  })
  // Verify debits = credits
}
```

---

## Performance Characteristics

| Operation | Method | RCU/WCU | Latency |
|-----------|--------|---------|---------|
| Get transaction by ID | GetItem | 0.5-1 RCU | 1-3ms |
| Get transaction + legs | Query | 1-3 RCU | 3-8ms |
| Query by status (100 items) | Query GSI1 | 5-10 RCU | 10-30ms |
| Check idempotency | Query GSI2 | 0.5 RCU | 2-5ms |
| Atomic payment (3 items) | TransactWriteItems | 6 WCU | 15-50ms |
| Account history (100 items) | Query GSI1 | 5-10 RCU | 10-30ms |

**Notes:**
- RCU assumes 4KB items or less
- Strongly consistent reads cost 2x RCU
- TransactWriteItems costs 2x WCU per item
- GSI queries have eventual consistency by default

---

## Comparison with PostgreSQL

| Feature | DynamoDB | PostgreSQL |
|---------|----------|------------|
| Point lookup | Very fast (1-3ms) | Fast (2-5ms) |
| Range queries | Fast if partition key known | Very fast with indexes |
| JOINs | Not supported (denormalize) | Native support |
| Aggregations | Not supported (use Streams) | Native SUM, COUNT, etc. |
| Transactions | Up to 100 items, 4MB | Unlimited rows |
| Scalability | Automatic, unlimited | Manual sharding |
| ACID | Item-level, conditional writes | Full ACID across tables |

**When to Use DynamoDB:**
- Known access patterns
- High throughput (millions of ops/sec)
- Global distribution required
- Low operational overhead desired

**When to Use PostgreSQL:**
- Complex queries and JOINs
- Ad-hoc reporting
- Strong consistency across multiple tables
- Traditional banking/financial systems
