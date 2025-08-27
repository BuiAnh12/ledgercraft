#!/usr/bin/env bash
set -euo pipefail

CONNECTOR_PORT=8083   # adjust if needed

OLTP_SCHEMA="public"
DWH_STG_SCHEMA="stg"
DWH_CUR_SCHEMA="cur"

REQUIRED_TABLES_OLTP=("accounts" "outbox" "customers" "idempotency" "transactions" "risk_flags")
REQUIRED_TABLES_DW_STG=("accounts" "outbox" "customers" "transactions" "etl_watermarks")
REQUIRED_TABLES_DW_CUR=("dim_account" "dim_customer" "dim_merchant" "fact_transactions")

echo "=== 1. Bringing up docker compose stack ==="
make up

echo "Waiting for service to roll up and initialization for 10s"
sleep 10

echo "=== 2. Checking OLTP schema in Postgres ==="
missing_tables_oltp=()
for table in "${REQUIRED_TABLES_OLTP[@]}"; do
  if ! make psql-oltp-nonint SQL="SELECT to_regclass('$OLTP_SCHEMA.$table');" | grep -q "$table"; then
    missing_tables_oltp+=("$table")
  fi
done

if [ ${#missing_tables_oltp[@]} -gt 0 ]; then
  echo "Missing tables: ${missing_tables_oltp[*]}"
  echo "=== 2.1. Running OLTP migrations ==="
  make migrate-oltp
else
  echo "All required tables exist. Skipping migration."
fi

echo "=== 3. Checking DW_STG schema in Postgres ==="
missing_tables_dw_stg=()
for table in "${REQUIRED_TABLES_DW_STG[@]}"; do
  if ! make psql-dw-nonint SQL="SELECT to_regclass('$DWH_STG_SCHEMA.$table');" | grep -q "$table"; then
    missing_tables_dw_stg+=("$table")
  fi
done

if [ ${#missing_tables_dw_stg[@]} -gt 0 ]; then
  echo "Missing tables: ${missing_tables_dw_stg[*]}"
  echo "=== 3.1. Running DW_STG migrations ==="
  make migrate-dwh-stg
else
  echo "All required tables exist. Skipping migration."
fi  

echo "=== 4. Checking DW_CUR schema in Postgres ==="
missing_tables_dw_cur=()
for table in "${REQUIRED_TABLES_DW_CUR[@]}"; do
  if ! make psql-dw-nonint SQL="SELECT to_regclass('$DWH_CUR_SCHEMA.$table');" | grep -q "$table"; then
    missing_tables_dw_cur+=("$table")
  fi
done

if [ ${#missing_tables_dw_cur[@]} -gt 0 ]; then
  echo "Missing tables: ${missing_tables_dw_cur[*]}"
  echo "=== 4.1. Running DW_CUR migrations ==="
  make migrate-dwh-cur
else
  echo "All required tables exist. Skipping migration."
fi  

echo "=== 5. Creating Kafka risk.flags topic ==="
make topic-create-flag || true

echo "=== 6. Checking if Debezium connector exists ==="
status=$(make connect-status P=$CONNECTOR_PORT | jq -r '.connector.state' 2>/dev/null || echo "MISSING")

if [ "$status" == "RUNNING" ]; then
  echo "Connector already exists and is RUNNING ✅"
else
  echo "Connector not present or not healthy. Registering..."
  make connect-register P=$CONNECTOR_PORT || true

  echo "Waiting for connector to become RUNNING 3s..."
  sleep 3
  status=$(make connect-status P=$CONNECTOR_PORT | jq -r '.connector.state')
  if [ "$status" == "RUNNING" ]; then
    echo "Connector is RUNNING ✅"
  else
    echo "Connector failed to become RUNNING within 60s ❌"
    exit 1
  fi
fi

echo "=== 7. Starting ingest service ==="
make ingest
