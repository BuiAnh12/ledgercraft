-- ========= Schema =========
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS cur;

-- ========= Utility =========
CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- for UUID helpers if needed
CREATE EXTENSION IF NOT EXISTS citext;    -- case-insensitive text (email)

-- ========= DIMENSIONS (SCD2) =========
-- Conventions:
--   *_sk          : surrogate key (int)
--   *_bk          : business key (UUID / natural)
--   is_current    : whether this row is the current version
--   valid_from/to : SCD2 temporal bounds (half-open: [from, to))

-- ----- dim_customer -----
CREATE TABLE IF NOT EXISTS cur.dim_customer (
  customer_sk   BIGSERIAL PRIMARY KEY,
  customer_bk   UUID        NOT NULL,   -- OLTP customers.id
  email         CITEXT      NOT NULL,
  country       CHAR(2)     NOT NULL,
  kyc_level     SMALLINT    NOT NULL,
  risk_band     TEXT        NOT NULL CHECK (risk_band IN ('LOW','MEDIUM','HIGH')),

  is_current    BOOLEAN     NOT NULL DEFAULT TRUE,
  valid_from    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  valid_to      TIMESTAMPTZ,
  version       INTEGER     NOT NULL DEFAULT 1
);
-- One "current" row per BK
CREATE UNIQUE INDEX IF NOT EXISTS uix_dim_customer_current
  ON cur.dim_customer(customer_bk)
  WHERE is_current;

CREATE INDEX IF NOT EXISTS idx_dim_customer_bk ON cur.dim_customer(customer_bk);
CREATE INDEX IF NOT EXISTS idx_dim_customer_valid ON cur.dim_customer(customer_bk, valid_from DESC);

-- ----- dim_account -----
CREATE TABLE IF NOT EXISTS cur.dim_account (
  account_sk    BIGSERIAL PRIMARY KEY,
  account_bk    UUID        NOT NULL,   -- OLTP accounts.id
  customer_bk   UUID        NOT NULL,   -- denorm BK keeps SCD simple
  currency      CHAR(3)     NOT NULL,
  country       CHAR(2)     NOT NULL,
  status        TEXT        NOT NULL CHECK (status IN ('ACTIVE','SUSPENDED','CLOSED')),

  is_current    BOOLEAN     NOT NULL DEFAULT TRUE,
  valid_from    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  valid_to      TIMESTAMPTZ,
  version       INTEGER     NOT NULL DEFAULT 1
);
CREATE UNIQUE INDEX IF NOT EXISTS uix_dim_account_current
  ON cur.dim_account(account_bk)
  WHERE is_current;

CREATE INDEX IF NOT EXISTS idx_dim_account_bk ON cur.dim_account(account_bk);
CREATE INDEX IF NOT EXISTS idx_dim_account_valid ON cur.dim_account(account_bk, valid_from DESC);

-- ----- dim_merchant -----
-- Derived from transaction attributes (name/category/country).
CREATE TABLE IF NOT EXISTS cur.dim_merchant (
  merchant_sk        BIGSERIAL PRIMARY KEY,
  merchant_name_norm TEXT        NOT NULL,   -- LOWER(TRIM(name))
  merchant_category  TEXT,
  country            CHAR(2),

  is_current    BOOLEAN     NOT NULL DEFAULT TRUE,
  valid_from    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  valid_to      TIMESTAMPTZ,
  version       INTEGER     NOT NULL DEFAULT 1
);
-- Ensure one current row per normalized merchant "identity"
CREATE UNIQUE INDEX IF NOT EXISTS uix_dim_merchant_current
  ON cur.dim_merchant(merchant_name_norm, COALESCE(merchant_category,''), COALESCE(country,'')) WHERE is_current;

CREATE INDEX IF NOT EXISTS idx_dim_merchant_name ON cur.dim_merchant(merchant_name_norm);

-- ========= FACTS =========
-- Partitioned by created_at (UTC date) via a plain column kept in sync by trigger
CREATE TABLE IF NOT EXISTS cur.fact_transactions (
  transaction_bk   UUID           NOT NULL,                 -- OLTP transactions.id (natural)
  customer_sk      BIGINT         NOT NULL,
  account_sk       BIGINT         NOT NULL,
  merchant_sk      BIGINT         NOT NULL,

  type             TEXT           NOT NULL CHECK (type IN ('PAYMENT','TRANSFER_IN','TRANSFER_OUT','REFUND','FEE')),
  status           TEXT           NOT NULL CHECK (status IN ('PENDING','SETTLED','DECLINED','REVERSED','CHARGEBACK')),
  amount           NUMERIC(20,6)  NOT NULL CHECK (amount > 0),
  currency         CHAR(3)        NOT NULL,
  country          CHAR(2)        NOT NULL,

  created_at       TIMESTAMPTZ    NOT NULL,
  settled_at       TIMESTAMPTZ,

  -- NOTE: plain column (NOT generated). We enforce equality to UTC date of created_at.
  created_date     DATE           NOT NULL,
  CONSTRAINT chk_created_date_matches
    CHECK (created_date = (created_at AT TIME ZONE 'UTC')::date),

  PRIMARY KEY (transaction_bk, created_date)
) PARTITION BY RANGE (created_date);

-- Keep created_date in sync automatically
CREATE OR REPLACE FUNCTION cur.sync_created_date()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.created_date := (NEW.created_at AT TIME ZONE 'UTC')::date;
  RETURN NEW;
END$$;

DROP TRIGGER IF EXISTS trg_fact_txn_set_created_date ON cur.fact_transactions;
CREATE TRIGGER trg_fact_txn_set_created_date
BEFORE INSERT OR UPDATE OF created_at ON cur.fact_transactions
FOR EACH ROW EXECUTE FUNCTION cur.sync_created_date();


-- Helpful indexes on each partition will be added automatically via template (see function below)

-- ========= Partition helper: ensure month partition exists =========
CREATE OR REPLACE FUNCTION cur.ensure_fact_partition(p_date date)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  start_date date := date_trunc('month', p_date)::date;
  end_date   date := (date_trunc('month', p_date) + interval '1 month')::date;
  part_name  text := format('fact_transactions_y%sm%s', to_char(start_date,'YYYY'), to_char(start_date,'MM'));
  ddl        text;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                 WHERE c.relkind = 'r' AND n.nspname='cur' AND c.relname=part_name) THEN
    ddl := format($f$
      CREATE TABLE cur.%1$s PARTITION OF cur.fact_transactions
      FOR VALUES FROM ('%2$s') TO ('%3$s');
      CREATE INDEX %1$s_account_sk_idx ON cur.%1$s (account_sk, created_at);
      CREATE INDEX %1$s_customer_sk_idx ON cur.%1$s (customer_sk, created_at);
      CREATE INDEX %1$s_currency_date_idx ON cur.%1$s (currency, created_date);
    $f$, part_name, start_date, end_date);
    EXECUTE ddl;
  END IF;
END$$;

-- Create partitions for last month, current month, next month (handy for dev)
SELECT cur.ensure_fact_partition( (CURRENT_DATE - INTERVAL '1 month')::date );
SELECT cur.ensure_fact_partition( CURRENT_DATE );
SELECT cur.ensure_fact_partition( (CURRENT_DATE + INTERVAL '1 month')::date );
