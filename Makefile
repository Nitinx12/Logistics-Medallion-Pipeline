# FreightLake Makefile
# Every target is a thin wrapper around a script in scripts/ or a uv
# command; the logic lives in one place. `make help` lists everything.
# Windows users without make: use the paired scripts under
# scripts/powershell/ or `uv run python main.py --help`.

.DEFAULT_GOAL := help
SHELL := /bin/bash

UV := uv
DBT_DIR := dbt

.PHONY: help setup docker-up docker-down health bronze silver gold gx publish \
        pipeline dbt-docs lint lint-fix fmt fmt-check test test-cov ci verify \
        hooks polish clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

# --- setup and stack ---

setup: ## First time setup: uv sync + build the Docker image
	./scripts/bash/bootstrap.sh

docker-up: ## Start Postgres, MongoDB and Airflow (waits for health)
	./scripts/bash/start_stack.sh

docker-down: ## Stop the stack
	./scripts/bash/stop_stack.sh

health: ## One-shot health check of every service
	./scripts/bash/health_check_all.sh

hooks: ## Install git hooks from .githooks
	./scripts/bash/setup_hooks.sh

# --- pipeline stages ---

bronze: ## Run both bronze extraction jobs (Postgres + MongoDB)
	./scripts/bash/run_bronze.sh

silver: ## Build and test the silver layer
	./scripts/bash/run_silver.sh

gold: ## Build and test the gold layer
	./scripts/bash/run_gold.sh

gx: ## Run the Great Expectations quality gate
	./scripts/bash/run_gx.sh

publish: ## Publish gold to the Postgres serving mart
	./scripts/bash/publish_mart.sh

pipeline: ## Full local run: docker, bronze, silver, gold, publish (ARGS="--skip-docker" etc.)
	$(UV) run python main.py $(ARGS)

dbt-docs: ## Generate the dbt docs site
	cd $(DBT_DIR) && $(UV) run dbt docs generate

# --- quality ---

lint: ## ruff check + ruff format check + sqlfluff lint
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run sqlfluff lint --exclude-rules LT09,LT12 || true

lint-fix: ## Auto fix ruff issues
	$(UV) run ruff check . --fix
	$(UV) run ruff format .

fmt: ## Format code with ruff
	$(UV) run ruff format .

fmt-check: ## Check formatting without changing files
	$(UV) run ruff format --check . --diff

test: ## pytest unit and suite tests
	$(UV) run pytest

test-cov: ## pytest with coverage
	$(UV) run pytest --cov=src --cov-report=term-missing --cov-report=xml

verify: lint test ## Fast local verify, same as CI lint and unit tests
	$(UV) run python gx/run_validations.py --demo

ci: verify ## Everything CI runs locally (lint, test, gx demo). Full gate is run_all_tests.sh

polish: lint-fix fmt ## Polish code style across the repo
	@echo "polish done. Review git diff before committing."

clean: ## Stop the stack and remove local build artifacts
	./scripts/bash/stop_stack.sh || true
	rm -rf spark-warehouse dbt/target htmlcov .coverage coverage.xml
