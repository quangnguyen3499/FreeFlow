.PHONY: help up down test lint format-check sync watch init logs ps restart clean shell

COMPOSE = docker compose

help:
	@echo "Airflow ETL Pipeline Boilerplate Commands:"
	@echo "  1. make watch [DAG=..]  Run watcher ON HOST to sync DAGs into enabled_dags/"
	@echo "  2. make sync [DAG=..]   Sync DAGs ON HOST into enabled_dags/ once"
	@echo "  3. make up              Start Docker Compose cluster (webserver, scheduler, mock-api, seaweedfs)"
	@echo "  4. make down            Stop Docker Compose containers"
	@echo "  5. make test            Run unit & integration tests inside Docker container"
	@echo "  6. make lint            Run Ruff against source and test files"
	@echo "  7. make format-check    Check Black formatting"
	@echo "  8. make init            Run database migration & environment seeding"
	@echo "  9. make logs            Follow logs from Docker containers"
	@echo "  10. make ps             Display running Docker services"
	@echo "  11. make shell          Open interactive bash shell inside Airflow container"
	@echo "  12. make clean          Stop containers, wipe volumes, and clean local temp files"

watch:
	@if [ -z "$(DAG)" ]; then \
		./watcher; \
	else \
		./watcher $(DAG); \
	fi

sync:
	@if [ -z "$(DAG)" ]; then \
		./sync --list; \
	else \
		./sync $(DAG); \
	fi

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

test:
	$(COMPOSE) run --rm test

lint:
	ruff check dags plugins tools tests

format-check:
	black --check dags plugins tools tests

init:
	$(COMPOSE) run --rm airflow-init

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

shell:
	$(COMPOSE) exec airflow-webserver bash

restart:
	$(COMPOSE) restart

clean:
	$(COMPOSE) down -v
	rm -rf staging/active_dags staging/finance_process_partner_billing enabled_dags staging/storage_remote
	find . -type d -name "__pycache__" -exec rm -rf {} +
