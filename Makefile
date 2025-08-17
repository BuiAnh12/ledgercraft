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

# Migrate schema from /app/warehouse/ddl/oltp.sql
migrate-oltp:
	@set -a; [ -f $(ENV) ] && . $(ENV); set +a; \
	docker exec -i lc-oltp-postgres \
	psql -U $$OLTP_USER -d $$OLTP_DB -v ON_ERROR_STOP=1 \
	-f /app/warehouse/ddl/oltp.sql

# Create seed user and account
seed-data:
	python3 seed/generator.py --customers 200 --max-accounts-per-customer 2
# Create tester user and account
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
	docker exec -it lc-kafka kafka-topics --bootstrap-server kafka:9092 --create --topic $(T) --partitions 1 --replication-factor 1
# Produce demo topic
produce:
	@echo "Type messages, Ctrl+C to stop"
	docker exec -it lc-kafka bash -lc 'kafka-console-producer --broker-list kafka:9092 --topic $(T)'
# Consume demo toppic
consume:
	docker exec -it lc-kafka bash -lc 'kafka-console-consumer --bootstrap-server kafka:9092 --topic $(T) --from-beginning --timeout-ms 0'
# Check connector 
connectors:
	curl -s http://localhost:${KAFKA_CONNECT_HOST_PORT}/connectors | jq
# Register connecter
connect-register:
	curl -s -X POST -H "Content-Type: application/json" \
	  --data @infra/kafka-connect/debezium-postgres-source.json \
	  http://localhost:${KAFKA_CONNECT_HOST_PORT}/connectors | jq
# Check debezium connecter status
connect-status:
	curl -s http://localhost:${KAFKA_CONNECT_HOST_PORT}/connectors/debezium-postgres-source/status | jq
# Delete debezium connecter
connect-delete:
	curl -s -X DELETE http://localhost:${KAFKA_CONNECT_HOST_PORT}/connectors/debezium-postgres-source | jq
