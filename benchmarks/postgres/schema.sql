-- PostgreSQL Schema for Financial Transactions Benchmark

-- Drop existing tables if they exist
DROP TABLE IF EXISTS transaction_legs CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS merchants CASCADE;

-- Merchants table
CREATE TABLE merchants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Accounts table
CREATE TABLE accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    account_type VARCHAR(50) NOT NULL, -- checking, savings, credit, etc.
    currency VARCHAR(3) DEFAULT 'USD',
    balance DECIMAL(19, 4) NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 0, -- Optimistic locking
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Transactions table (header)
CREATE TABLE transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key VARCHAR(255) UNIQUE NOT NULL,
    transaction_type VARCHAR(50) NOT NULL, -- payment, transfer, refund, etc.
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- pending, completed, failed, reversed
    merchant_id UUID REFERENCES merchants(id),
    description TEXT,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE
);

-- Transaction legs table (double-entry bookkeeping)
CREATE TABLE transaction_legs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    account_id UUID NOT NULL REFERENCES accounts(id),
    leg_type VARCHAR(10) NOT NULL CHECK (leg_type IN ('debit', 'credit')),
    amount DECIMAL(19, 4) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) DEFAULT 'USD',
    balance_after DECIMAL(19, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_accounts_user_id ON accounts(user_id);
CREATE INDEX idx_accounts_status ON accounts(status);
CREATE INDEX idx_accounts_updated_at ON accounts(updated_at);

CREATE INDEX idx_transactions_status ON transactions(status);
CREATE INDEX idx_transactions_created_at ON transactions(created_at DESC);
CREATE INDEX idx_transactions_merchant_id ON transactions(merchant_id);
CREATE INDEX idx_transactions_idempotency_key ON transactions(idempotency_key);

CREATE INDEX idx_transaction_legs_transaction_id ON transaction_legs(transaction_id);
CREATE INDEX idx_transaction_legs_account_id ON transaction_legs(account_id);
CREATE INDEX idx_transaction_legs_created_at ON transaction_legs(created_at DESC);
CREATE INDEX idx_transaction_legs_account_created ON transaction_legs(account_id, created_at DESC);

-- Composite index for common queries
CREATE INDEX idx_transactions_status_created ON transactions(status, created_at DESC);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers to auto-update timestamps
CREATE TRIGGER update_accounts_updated_at BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_transactions_updated_at BEFORE UPDATE ON transactions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- View for account balances with recent activity
CREATE OR REPLACE VIEW account_balances AS
SELECT
    a.id,
    a.user_id,
    a.account_type,
    a.balance,
    a.currency,
    a.status,
    COUNT(tl.id) as transaction_count,
    MAX(tl.created_at) as last_transaction_at
FROM accounts a
LEFT JOIN transaction_legs tl ON a.id = tl.account_id
GROUP BY a.id, a.user_id, a.account_type, a.balance, a.currency, a.status;
