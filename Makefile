# Path to your .env file
ENV ?= $(CURDIR)/.env

# Bring up the stack
up:
	docker compose --env-file $(ENV) -f infra/docker-compose.yml up -d

# Tear down the stack (keep volumes/data)
down:
	docker compose --env-file $(ENV) -f infra/docker-compose.yml down

# Tear down the stack and remove volumes
reset:
	docker compose --env-file $(ENV) -f infra/docker-compose.yml down -v

# Build the service
build:
	docker compose --env-file $(ENV) -f infra/docker-compose.yml build

# Show running containers
ps:
	docker compose --env-file $(ENV) -f infra/docker-compose.yml ps

# Tail logs
logs:
	docker compose --env-file $(ENV) -f infra/docker-compose.yml logs -f --tail=200

# Connect to OLTP Postgres
psql-oltp:
	docker exec -it lc-oltp-postgres \
	psql -U $(shell grep OLTP_USER $(ENV) | cut -d= -f2) \
	     -d $(shell grep OLTP_DB $(ENV) | cut -d= -f2)

# Connect to OLTP Posgres and run script
psql-oltp-nonint:
	@docker exec -i lc-oltp-postgres \
	psql -U $(shell grep OLTP_USER $(ENV) | cut -d= -f2) \
	     -d $(shell grep OLTP_DB $(ENV) | cut -d= -f2) -tAc "$$SQL"


# Connect to OLTP Posgres and run script
psql-dw-nonint:
	@docker exec -i lc-dwh-postgres \
	psql -U $(shell grep DWH_USER $(ENV) | cut -d= -f2) \
	     -d $(shell grep DWH_DB $(ENV) | cut -d= -f2) -tAc "$$SQL"


		 
# Connect to DWH Postgres
psql-dwh:
	docker exec -it lc-dwh-postgres \
	psql -U $(shell grep DWH_USER $(ENV) | cut -d= -f2) \
	     -d $(shell grep DWH_DB $(ENV) | cut -d= -f2)

# migrate-oltp:
# 	docker exec -it lc-oltp-postgres \
# 	psql -U $(shell grep OLTP_USER $(ENV) | cut -d= -f2) \
# 	     -d $(shell grep OLTP_DB $(ENV) | cut -d= -f2)
# 	     -v ON_ERROR_STOP=1 \
# 	     -f /app/warehouse/ddl/oltp.sql

# Migrate schema for oltp data
migrate-oltp:
	@set -a; [ -f $(ENV) ] && . $(ENV); set +a; \
	docker exec -i lc-oltp-postgres \
	psql -U $$OLTP_USER -d $$OLTP_DB -v ON_ERROR_STOP=1 \
	-f /app/warehouse/ddl/oltp.sql
# Migrate risk schema
migrate-oltp-risk:
	@set -a; [ -f $(ENV) ] && . $(ENV); set +a; \
	docker exec -i lc-oltp-postgres psql -U $$OLTP_USER -d $$OLTP_DB -v ON_ERROR_STOP=1 -f /app/warehouse/ddl/oltp_risk_flags.sql
# Migrate schema for data warehouse
migrate-dwh-cur:
	@set -a; [ -f $(ENV) ] && . $(ENV); set +a; \
	docker exec -i lc-dwh-postgres psql -U $$DWH_USER -d $$DWH_DB -v ON_ERROR_STOP=1 -f /app/warehouse/ddl/dwh_curated.sql
# Migrate schema for stage
migrate-dwh-stg:
	@set -a; [ -f $(ENV) ] && . $(ENV); set +a; \
	docker exec -i lc-dwh-postgres psql -U $$DWH_USER -d $$DWH_DB -v ON_ERROR_STOP=1 -f /app/warehouse/ddl/dwh_staging.sql
migrate-dwh-cur-funcs:
	@set -a; [ -f $(ENV) ] && . $(ENV); set +a; \
	docker exec -i lc-dwh-postgres psql -U $$DWH_USER -d $$DWH_DB -v ON_ERROR_STOP=1 -f /app/warehouse/ddl/dwh_curated_functions.sql
migrate-dwh-mviews:
	@set -a; [ -f $(ENV) ] && . $(ENV); set +a; \
	docker exec -i lc-dwh-postgres psql -U $$DWH_USER -d $$DWH_DB -v ON_ERROR_STOP=1 -f /app/warehouse/ddl/dwh_mviews.sql

# refresh-mv:
# 	@set -a; [ -f $(ENV) ] && . $(ENV); set +a; \
# 	docker exec -i lc-dwh-postgres psql -U $$DWH_USER -d $$DWH_DB -c "REFRESH MATERIALIZED VIEW CONCURRENTLY cur.mv_gmv_daily_currency; REFRESH MATERIALIZED VIEW cur.mv_top_merchants_7d;"
# REMOVE CONCURENTLY
refresh-mv:
	@set -a; [ -f $(ENV) ] && . $(ENV); set +a; \
	docker exec -i lc-dwh-postgres psql -U $$DWH_USER -d $$DWH_DB -c "REFRESH MATERIALIZED VIEW cur.mv_gmv_daily_currency; REFRESH MATERIALIZED VIEW cur.mv_top_merchants_7d;"

# Migrate full db service
migrate-oltp-full:
	@set -a; [ -f $(ENV) ] && . $(ENV); set +a; \
	docker exec -i lc-oltp-postgres \
	psql -U $$OLTP_USER -d $$OLTP_DB -v ON_ERROR_STOP=1 \
	-f /app/warehouse/ddl/oltp/oltp_full.sql
migrate-dwh-cur:
	@set -a; [ -f $(ENV) ] && . $(ENV); set +a; \
	docker exec -i lc-dwh-postgres psql -U $$DWH_USER -d $$DWH_DB -v ON_ERROR_STOP=1 \
	-f /app/warehouse/ddl/dw/dw_full.sql

# Create Boostrap Data
dwh-dev-bootstrap:
	@set -a; [ -f $(ENV) ] && . $(ENV); set +a; \
	docker exec -i lc-dwh-postgres psql -U $$DWH_USER -d $$DWH_DB -v ON_ERROR_STOP=1 -f /app/warehouse/ddl/dwh_dev_bootstrap.sql
# Get partition data of month
dwh-partition-month:
	@set -a; [ -f $(ENV) ] && . $(ENV); set +a; \
	docker exec -i lc-dwh-postgres psql -U $$DWH_USER -d $$DWH_DB -v ON_ERROR_STOP=1 -c "SELECT cur.ensure_fact_partition(CURRENT_DATE);"

# Create seed user and account
seed-data:
	python3 seed/generator.py --customers 200 --max-accounts-per-customer 2
# Run the API Backend which recieve request of create txn and ect
ingest:
	cd services/ingest_api && uvicorn app.main:app --reload --port 8001
# Open CLI for kafka
kafka-shell:
	docker exec -it lc-kafka bash
# List kafka topics
topics:
	docker exec -it lc-kafka kafka-topics --bootstrap-server kafka:9092 --list
# Create demo topic
topic-create:
	docker exec -it lc-kafka kafka-topics \
		--bootstrap-server kafka:9092 --create \--topic $(T) \
		--partitions 1 --replication-factor 1
# Create flag topic ( only one )
topic-create-flag:
	docker exec -it lc-kafka kafka-topics \
	  --bootstrap-server kafka:9092 \
	  --create --topic risk.flags \
	  --partitions 1 --replication-factor 1 || true
# Produce topic
produce:
	@echo "Type messages, Ctrl+C to stop"
	docker exec -it lc-kafka bash -lc 'kafka-console-producer --broker-list kafka:9092 --topic $(T)'
# Consume toppic
consume:
	docker exec -it lc-kafka bash -lc 'kafka-console-consumer --bootstrap-server kafka:9092 --topic $(T) --from-beginning --timeout-ms 0'
# Check connector 
connectors:
	curl -s http://localhost:${P}/connectors | jq
# Register connecter
connect-register:
	curl -s -X POST -H "Content-Type: application/json" \
	  --data @infra/kafka-connect/debezium-postgres-source.json \
	  http://localhost:${P}/connectors | jq
# Check debezium connecter status
connect-status:
	@resp=$$(curl -s -w "%{http_code}" -o /tmp/connect_status.json http://localhost:${P}/connectors/debezium-postgres-source/status || true); \
	if [ "$$resp" -ne 200 ]; then \
	  echo "{}"; \
	else \
	  cat /tmp/connect_status.json | jq; \
	fi
# Delete debezium connecter
connect-delete:
	curl -s -X DELETE http://localhost:${P}/connectors/debezium-postgres-source | jq
# This checker for scipts
connect-check:
	@status=$$(curl -s http://localhost:${P}/connectors/debezium-postgres-source/status | jq -r '.connector.state'); \
	task=$$(curl -s http://localhost:${P}/connectors/debezium-postgres-source/status | jq -r '.tasks[0].state'); \
	echo "Connector: $$status, Task: $$task"; \
	if [ "$$status" != "RUNNING" ] || [ "$$task" != "RUNNING" ]; then \
	  echo "❌ Debezium connector not healthy"; \
	  exit 1; \
	else \
	  echo "✅ Debezium connector is healthy"; \
	fi
