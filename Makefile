.DEFAULT_GOAL := help
SHELL := /bin/bash

COMPOSE := docker compose

.PHONY: lake dbt-build dbt-docs site airflow airflow-validate help install lint fmt typecheck test test-integration ci up down logs ps clean demo lambdas claims

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies and git hooks
	uv sync --all-extras --all-groups
	uv run pre-commit install

lint: ## Lint and check formatting
	uv run ruff check .
	uv run ruff format --check .

fmt: ## Format code and apply safe lint fixes
	uv run ruff check --fix .
	uv run ruff format .

typecheck: ## Static type check
	uv run mypy src tests

test: ## Unit tests with coverage
	uv run pytest --cov --cov-report=term --cov-report=xml

test-integration: ## Integration tests against the running stack (make up first)
	uv run pytest -m integration

ci: lint typecheck test ## Everything CI runs before integration tests

up: ## Start the local stack and wait until it is healthy
	$(COMPOSE) up -d --wait kafka localstack
	$(COMPOSE) run --rm kafka-init

down: ## Stop the local stack
	$(COMPOSE) down

logs: ## Follow logs of the local stack
	$(COMPOSE) logs -f

ps: ## Show stack status
	$(COMPOSE) ps

clean: ## Stop the stack and drop its volumes
	$(COMPOSE) down -v --remove-orphans

PIPELINE := $(COMPOSE) --profile pipeline
DEMO_SPEED := 720

demo: up ## Replay one day through ingestion, bronze and silver, then summarise the lake
	$(PIPELINE) build
	$(PIPELINE) run --rm -e SKYPULSE_INGESTION__REPLAY_START=2026-10-14T10:30:00Z producer-states states --cycles 1
	$(PIPELINE) run --rm -e SKYPULSE_INGESTION__REPLAY_SPEED=$(DEMO_SPEED) producer-schedules schedules --cycles 19 --interval 5
	$(PIPELINE) run --rm -e SKYPULSE_INGESTION__REPLAY_START=2026-10-14T23:00:00Z producer-metar metar --cycles 1
	$(PIPELINE) run --rm spark-bronze bronze --available-now --master "local[2]"
	$(PIPELINE) run --rm spark-silver silver --available-now --master "local[2]"
	$(MAKE) lambdas
	$(PIPELINE) run --rm spark-silver detector --available-now --master "local[2]"
	$(PIPELINE) run --rm --entrypoint python spark-silver -m skypulse.streaming.summary
	@echo "Waiting for the calculator Lambda to drain the queue..."
	@sleep 20
	$(MAKE) claims

lambdas: ## Package the Lambda handlers and deploy them to LocalStack
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test uv run python -m skypulse.deployment.lambdas

claims: ## List EU261 claims stored in DynamoDB (LocalStack)
	$(COMPOSE) exec -T localstack awslocal dynamodb scan --table-name eu261-claims \
		--query 'Items[].[flight_number.S, kind.S, verdict.S, amount_eur.N, extraordinary_circumstances_likely.BOOL]' \
		--output table

LAKE_ROOT := $(CURDIR)/data/lake
LAKE_ENV := SKYPULSE_LAKE__ROOT_URI=file://$(LAKE_ROOT) SKYPULSE_INGESTION__REPLAY=true \
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
SPARK_LOCAL := --master "local[2]"

lake: up ## Build a local lake in data/lake from the replay fixtures (no containers)
	$(LAKE_ENV) uv run python -m skypulse.batch reference $(SPARK_LOCAL)
	$(LAKE_ENV) SKYPULSE_INGESTION__REPLAY_START=2026-10-14T10:30:00Z uv run python -m skypulse.ingestion states --cycles 1
	$(LAKE_ENV) SKYPULSE_INGESTION__REPLAY_SPEED=7200 uv run python -m skypulse.ingestion schedules --cycles 10 --interval 1
	$(LAKE_ENV) SKYPULSE_INGESTION__REPLAY_START=2026-10-14T23:00:00Z uv run python -m skypulse.ingestion metar --cycles 1
	$(LAKE_ENV) uv run python -m skypulse.streaming bronze --available-now $(SPARK_LOCAL)
	$(LAKE_ENV) uv run python -m skypulse.streaming silver --available-now $(SPARK_LOCAL)
	$(LAKE_ENV) uv run python -m skypulse.streaming detector --available-now $(SPARK_LOCAL)
	$(LAKE_ENV) uv run python -m skypulse.batch assessments $(SPARK_LOCAL)

DBT := cd dbt && SKYPULSE_SILVER_ROOT=$(LAKE_ROOT)/silver uv run dbt

dbt-build: ## Build and test the gold models on the local lake
	$(DBT) deps
	$(DBT) build

dbt-docs: ## Generate dbt docs into dbt/target
	$(DBT) docs generate

site: ## Build the static site (landing, insights, dbt docs) into public/
	uv run python -m skypulse.site.build --gold dbt/target/skypulse.duckdb --dbt-target dbt/target --out public

ORCHESTRATION := $(COMPOSE) --profile orchestration --profile pipeline

airflow: up ## Build the images and start Airflow (UI on http://localhost:8088)
	$(ORCHESTRATION) build
	$(COMPOSE) --profile orchestration up -d --wait airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer

airflow-validate: ## Check the DAGs inside the Airflow image (imports, schedule, retries, order)
	$(COMPOSE) --profile orchestration build airflow-init
	docker run --rm -v $(CURDIR)/airflow:/opt/airflow/project:ro \
		-v $(CURDIR)/airflow/dags:/opt/airflow/dags:ro \
		skypulse-airflow:local python /opt/airflow/project/tests/validate_dags.py
