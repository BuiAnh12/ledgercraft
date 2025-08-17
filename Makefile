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

migrate-oltp:
	@set -a; [ -f $(ENV) ] && . $(ENV); set +a; \
	docker exec -i lc-oltp-postgres \
	psql -U $$OLTP_USER -d $$OLTP_DB -v ON_ERROR_STOP=1 \
	-f /app/warehouse/ddl/oltp.sql

seed-data:
	python3 seed/generator.py --customers 200 --max-accounts-per-customer 2

ingest:
	cd services/ingest_api && uvicorn app.main:app --reload --port 8001

