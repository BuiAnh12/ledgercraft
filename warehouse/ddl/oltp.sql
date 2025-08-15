-- ---------- Extensions ----------
CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS citext;    -- case-insensitive text (email)

-- ---------- Enums (use CHECK constraints if you prefer easier migrations) ----------
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'account_status') THEN
    CREATE TYPE account_status AS ENUM ('ACTIVE','SUSPENDED','CLOSED');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'transaction_status') THEN
    CREATE TYPE transaction_status AS ENUM ('PENDING','SETTLED','DECLINED','REVERSED','CHARGEBACK');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'transaction_type') THEN
    CREATE TYPE transaction_type AS ENUM ('PAYMENT','TRANSFER_IN','TRANSFER_OUT','REFUND','FEE');
  END IF;
END$$;

-- ---------- Utility trigger to keep updated_at fresh ----------
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ---------- Customers ----------
CREATE TABLE IF NOT EXISTS customers (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email         CITEXT NOT NULL UNIQUE,
  country       CHAR(2) NOT NULL CHECK (country ~ '^[A-Z]{2}$'),
  kyc_level     SMALLINT NOT NULL DEFAULT 0 CHECK (kyc_level BETWEEN 0 AND 3),
  risk_band     TEXT NOT NULL DEFAULT 'LOW' CHECK (risk_band IN ('LOW','MEDIUM','HIGH')),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TRIGGER trg_customers_updated_at
BEFORE UPDATE ON customers
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------- Accounts ----------
CREATE TABLE IF NOT EXISTS accounts (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id   UUID NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
  currency      CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  country       CHAR(2) NOT NULL DEFAULT 'VN' CHECK (country ~ '^[A-Z]{2}$'),
  status        account_status NOT NULL DEFAULT 'ACTIVE',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_status_valid CHECK (status IN ('ACTIVE','SUSPENDED','CLOSED'))
);
CREATE TRIGGER trg_accounts_updated_at
BEFORE UPDATE ON accounts
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- At most one *open* account per customer per currency (allows multiple CLOSED history)
CREATE UNIQUE INDEX IF NOT EXISTS uix_accounts_customer_currency_open
ON accounts(customer_id, currency)
WHERE status <> 'CLOSED';

CREATE INDEX IF NOT EXISTS idx_accounts_customer ON accounts(customer_id);
CREATE INDEX IF NOT EXISTS idx_accounts_status   ON accounts(status);

-- ---------- Transactions ----------
CREATE TABLE IF NOT EXISTS transactions (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  account_id        UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  type              transaction_type NOT NULL,
  status            transaction_status NOT NULL DEFAULT 'PENDING',
  amount            NUMERIC(20,6) NOT NULL CHECK (amount > 0),
  currency          CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  merchant_name     TEXT NOT NULL,
  merchant_category TEXT,
  description       TEXT,
  country           CHAR(2) NOT NULL CHECK (country ~ '^[A-Z]{2}$'),
  metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  settled_at        TIMESTAMPTZ
);
-- Hot-path indexes for account timelines and settled lookups
CREATE INDEX IF NOT EXISTS idx_txns_account_created_desc
  ON transactions (account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_txns_settled_account_created
  ON transactions (account_id, created_at)
  WHERE status = 'SETTLED';

-- ---------- Outbox (for CDC with Debezium) ----------
CREATE TABLE IF NOT EXISTS outbox (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregate_type TEXT NOT NULL,     -- e.g., 'transaction', 'account'
  aggregate_id   UUID NOT NULL,
  event_type     TEXT NOT NULL,     -- e.g., 'TransactionCreated'
  payload_json   JSONB NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_outbox_created ON outbox(created_at);

-- ---------- (Prep) Idempotency (used in Lesson 4) ----------
CREATE TABLE IF NOT EXISTS idempotency (
  id             BIGSERIAL PRIMARY KEY,
  endpoint       TEXT NOT NULL,
  idem_key       TEXT NOT NULL,
  request_hash   TEXT NOT NULL,
  response_code  INT,
  response_body  JSONB,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(endpoint, idem_key)
);
