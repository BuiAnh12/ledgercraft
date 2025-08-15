# Path to your .env file
ENV ?= $(CURDIR)/.env

# Bring up the stack
up:
	docker compose --env-file $(ENV) -f infra/docker-compose.yml up -d

# Tear down the stack and remove volumes
down:
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
