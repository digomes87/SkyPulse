.DEFAULT_GOAL := help
SHELL := /bin/bash

COMPOSE := docker compose

.PHONY: help install lint fmt typecheck test test-integration ci up down logs ps clean

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies and git hooks
	uv sync
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
