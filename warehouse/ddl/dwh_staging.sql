CREATE SCHEMA IF NOT EXISTS stg;
CREATE EXTENSION IF NOT EXISTS citext;    -- case-insensitive text (email)

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
