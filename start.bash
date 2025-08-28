#!/usr/bin/env bash
set -euo pipefail

CONNECTOR_PORT=8083   # adjust if needed
REQUIRED_TABLES=("accounts" "outbox" "customers" "idempotency" "transactions" "risk_flags")

echo "=== 1. Bringing up docker compose stack ==="
make up

echo "=== 4. Creating Kafka risk.flags topic ==="
make topic-create-flag || true

echo "=== 5. Checking if Debezium connector exists ==="
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

echo "=== 6. Starting ingest service ==="
make ingest
