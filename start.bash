#!/usr/bin/env bash
set -euo pipefail

CONNECTOR_PORT=8083   # adjust if needed
REQUIRED_TABLES=("accounts" "outbox" "customers" "idempotency" "transactions" "risk_flags")

echo "=== 1. Bringing up docker compose stack ==="
make up

echo "=== 2. Checking OLTP schema in Postgres ==="
missing_tables=()
for table in "${REQUIRED_TABLES[@]}"; do
  if ! make psql-oltp-nonint SQL="SELECT to_regclass('public.$table');" | grep -q "$table"; then
    missing_tables+=("$table")
  fi
done

if [ ${#missing_tables[@]} -gt 0 ]; then
  echo "Missing tables: ${missing_tables[*]}"
  echo "=== 3. Running OLTP migrations ==="
  make migrate-oltp
else
  echo "All required tables exist. Skipping migration."
fi

echo "=== 4. Creating Kafka risk.flags topic ==="
make topic-create-flag || true

echo "=== 5. Checking if Debezium connector exists ==="
status=$(make connect-status P=$CONNECTOR_PORT | jq -r '.connector.state' 2>/dev/null || echo "MISSING")

if [ "$status" == "RUNNING" ]; then
  echo "Connector already exists and is RUNNING ✅"
else
  echo "Connector not present or not healthy. Registering..."
  make connect-register P=$CONNECTOR_PORT || true
  status=$(make connect-status P=$CONNECTOR_PORT | jq -r '.connector.state')
  if [ "$status" == "RUNNING" ]; then
    echo "Connector is RUNNING ✅"
  else
    echo "Connector status is $status ❌"
    echo "Fix connector before starting ingest."
    exit 1
  fi
fi

echo "=== 6. Starting ingest service ==="
make ingest
