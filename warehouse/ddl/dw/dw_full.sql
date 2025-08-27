CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS cur;

-- Watermarks for incremental loads
CREATE TABLE IF NOT EXISTS stg.etl_watermarks (
  table_name      TEXT PRIMARY KEY,
  last_loaded_at  TIMESTAMPTZ NOT NULL DEFAULT TIMESTAMPTZ 'epoch'
);

-- Staging tables (append-only, dedupe by PK)
CREATE TABLE IF NOT EXISTS stg.customers (
  id           UUID PRIMARY KEY,
  email        CITEXT NOT NULL,
  country      CHAR(2) NOT NULL,
  kyc_level    SMALLINT NOT NULL,
  risk_band    TEXT NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL,
  loaded_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.accounts (
  id           UUID PRIMARY KEY,
  customer_id  UUID NOT NULL,
  currency     CHAR(3) NOT NULL,
  country      CHAR(2) NOT NULL,
  status       TEXT NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL,
  loaded_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.transactions (
  id                UUID PRIMARY KEY,
  account_id        UUID NOT NULL,
  type              TEXT NOT NULL,
  status            TEXT NOT NULL,
  amount            NUMERIC(20,6) NOT NULL,
  currency          CHAR(3) NOT NULL,
  merchant_name     TEXT NOT NULL,
  merchant_category TEXT,
  description       TEXT,
  country           CHAR(2) NOT NULL,
  metadata          JSONB NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL,
  settled_at        TIMESTAMPTZ,
  loaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.outbox (
  id             UUID PRIMARY KEY,
  aggregate_type TEXT NOT NULL,
  aggregate_id   UUID NOT NULL,
  event_type     TEXT NOT NULL,
  payload_json   JSONB NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL,
  loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


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

-- ========== SCD2 helper functions in cur schema ==========

-- DIM CUSTOMER (BK = customer_bk)
CREATE OR REPLACE FUNCTION cur.upsert_dim_customer(
  p_customer_bk uuid,
  p_email citext,
  p_country char(2),
  p_kyc_level smallint,
  p_risk_band text,
  p_as_of timestamptz
) RETURNS bigint AS $$
DECLARE cur_row cur.dim_customer%ROWTYPE;
BEGIN
  SELECT * INTO cur_row
  FROM cur.dim_customer
  WHERE customer_bk = p_customer_bk AND is_current
  ORDER BY valid_from DESC
  LIMIT 1;

  IF cur_row.customer_sk IS NULL THEN
    INSERT INTO cur.dim_customer (customer_bk,email,country,kyc_level,risk_band,is_current,valid_from,version)
    VALUES (p_customer_bk,p_email,p_country,p_kyc_level,p_risk_band,TRUE,COALESCE(p_as_of,NOW()),1)
    RETURNING customer_sk INTO cur_row.customer_sk;
    RETURN cur_row.customer_sk;
  END IF;

  IF cur_row.email IS DISTINCT FROM p_email
     OR cur_row.country IS DISTINCT FROM p_country
     OR cur_row.kyc_level IS DISTINCT FROM p_kyc_level
     OR cur_row.risk_band IS DISTINCT FROM p_risk_band THEN
    UPDATE cur.dim_customer
      SET is_current=FALSE, valid_to=COALESCE(p_as_of,NOW())
      WHERE customer_sk = cur_row.customer_sk;

    INSERT INTO cur.dim_customer (customer_bk,email,country,kyc_level,risk_band,is_current,valid_from,version)
    VALUES (p_customer_bk,p_email,p_country,p_kyc_level,p_risk_band,TRUE,COALESCE(p_as_of,NOW()),cur_row.version+1)
    RETURNING customer_sk INTO cur_row.customer_sk;
  END IF;

  RETURN cur_row.customer_sk;
END$$ LANGUAGE plpgsql;

-- DIM ACCOUNT (BK = account_bk)
CREATE OR REPLACE FUNCTION cur.upsert_dim_account(
  p_account_bk uuid,
  p_customer_bk uuid,
  p_currency char(3),
  p_country char(2),
  p_status text,
  p_as_of timestamptz
) RETURNS bigint AS $$
DECLARE cur_row cur.dim_account%ROWTYPE;
BEGIN
  SELECT * INTO cur_row
  FROM cur.dim_account
  WHERE account_bk = p_account_bk AND is_current
  ORDER BY valid_from DESC
  LIMIT 1;

  IF cur_row.account_sk IS NULL THEN
    INSERT INTO cur.dim_account (account_bk,customer_bk,currency,country,status,is_current,valid_from,version)
    VALUES (p_account_bk,p_customer_bk,p_currency,p_country,p_status,TRUE,COALESCE(p_as_of,NOW()),1)
    RETURNING account_sk INTO cur_row.account_sk;
    RETURN cur_row.account_sk;
  END IF;

  IF cur_row.customer_bk IS DISTINCT FROM p_customer_bk
     OR cur_row.currency   IS DISTINCT FROM p_currency
     OR cur_row.country    IS DISTINCT FROM p_country
     OR cur_row.status     IS DISTINCT FROM p_status THEN
    UPDATE cur.dim_account
      SET is_current=FALSE, valid_to=COALESCE(p_as_of,NOW())
      WHERE account_sk = cur_row.account_sk;

    INSERT INTO cur.dim_account (account_bk,customer_bk,currency,country,status,is_current,valid_from,version)
    VALUES (p_account_bk,p_customer_bk,p_currency,p_country,p_status,TRUE,COALESCE(p_as_of,NOW()),cur_row.version+1)
    RETURNING account_sk INTO cur_row.account_sk;
  END IF;

  RETURN cur_row.account_sk;
END$$ LANGUAGE plpgsql;

-- DIM MERCHANT (BK = merchant_name_norm; category/country are versioned attrs)
CREATE OR REPLACE FUNCTION cur.upsert_dim_merchant(
  p_name text,
  p_category text,
  p_country char(2),
  p_as_of timestamptz
) RETURNS bigint AS $$
DECLARE cur_row cur.dim_merchant%ROWTYPE;
DECLARE p_norm text := lower(btrim(p_name));
BEGIN
  SELECT * INTO cur_row
  FROM cur.dim_merchant
  WHERE merchant_name_norm = p_norm AND is_current
  ORDER BY valid_from DESC
  LIMIT 1;

  IF cur_row.merchant_sk IS NULL THEN
    INSERT INTO cur.dim_merchant (merchant_name_norm, merchant_category, country, is_current, valid_from, version)
    VALUES (p_norm, p_category, p_country, TRUE, COALESCE(p_as_of,NOW()), 1)
    RETURNING merchant_sk INTO cur_row.merchant_sk;
    RETURN cur_row.merchant_sk;
  END IF;

  IF COALESCE(cur_row.merchant_category,'') IS DISTINCT FROM COALESCE(p_category,'')
     OR COALESCE(cur_row.country,'') IS DISTINCT FROM COALESCE(p_country,'') THEN
    UPDATE cur.dim_merchant
      SET is_current=FALSE, valid_to=COALESCE(p_as_of,NOW())
      WHERE merchant_sk = cur_row.merchant_sk;

    INSERT INTO cur.dim_merchant (merchant_name_norm, merchant_category, country, is_current, valid_from, version)
    VALUES (p_norm, p_category, p_country, TRUE, COALESCE(p_as_of,NOW()), cur_row.version+1)
    RETURNING merchant_sk INTO cur_row.merchant_sk;
  END IF;

  RETURN cur_row.merchant_sk;
END$$ LANGUAGE plpgsql;
-- Daily GMV (gross merchandise volume) per currency
CREATE MATERIALIZED VIEW IF NOT EXISTS cur.mv_gmv_daily_currency AS
SELECT
  (created_at AT TIME ZONE 'UTC')::date AS day_utc,
  currency,
  COUNT(*) AS tx_count,
  SUM(amount) AS total_amount
FROM cur.fact_transactions
GROUP BY 1,2;

CREATE INDEX IF NOT EXISTS mv_gmv_daily_currency_day_idx
  ON cur.mv_gmv_daily_currency(day_utc, currency);

-- Top merchants (7d) by amount
CREATE MATERIALIZED VIEW IF NOT EXISTS cur.mv_top_merchants_7d AS
WITH recent AS (
  SELECT * FROM cur.fact_transactions
  WHERE created_at >= NOW() - INTERVAL '7 days'
)
SELECT
  m.merchant_name_norm,
  COUNT(*)     AS tx_count,
  SUM(r.amount) AS total_amount
FROM recent r
JOIN cur.dim_merchant m ON m.merchant_sk = r.merchant_sk AND m.is_current
GROUP BY 1
ORDER BY total_amount DESC
LIMIT 50;
