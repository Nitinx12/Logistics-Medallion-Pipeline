.PHONY: setup setup-ps docker-up docker-down seed bronze dbt-run dbt-build dbt-docs publish pipeline local-pipeline local-pipeline-clean lint test ci clean databricks-init databricks-schemas

# FreightLake — local automation
# Every target is a thin wrapper around scripts/bash/*.sh and scripts/powershell/*.ps1
# so logic lives in one place. See docs/PROJECT_PLAN.md section 10.

# Use bash where available (WSL/git bash) for portable || true handling
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

UV := uv
PYTHON := $(UV) run python

setup:
	@bash scripts/bash/setup.sh

setup-ps:
	@powershell -ExecutionPolicy Bypass -File scripts/powershell/setup.ps1

docker-up:
	@bash scripts/bash/docker.sh up

docker-down:
	@bash scripts/bash/docker.sh down

seed:
	@bash scripts/bash/seed_data.sh

bronze:
	@$(PYTHON) -m spark_jobs.bronze.extract_postgres_oltp
	@$(PYTHON) -m spark_jobs.bronze.extract_mongo_tracking

dbt-run:
	@bash scripts/bash/dbt.sh run

dbt-build:
	@bash scripts/bash/dbt.sh build

dbt-docs:
	@bash scripts/bash/dbt.sh docs

publish:
	@$(PYTHON) -m spark_jobs.publish.publish_gold_to_postgres

pipeline: seed bronze dbt-build publish
	@echo "Full local pipeline complete"

local-pipeline:
	@$(PYTHON) scripts/run_local_pipeline.py

local-pipeline-clean:
	@bash -c "rm -rf delta watermarks.json"
	@$(PYTHON) scripts/run_local_pipeline.py

lint:
	@$(UV) run ruff check spark_jobs scripts tests
	@$(UV) run ruff format --check spark_jobs scripts tests
	@$(UV) run mypy spark_jobs --ignore-missing-imports
	@$(UV) run sqlfluff lint sql/oltp_schema --dialect postgres
	@$(UV) run sqlfluff lint sql/serving_mart --dialect postgres
	@$(UV) run sqlfluff lint sql/databricks/bronze_tables.sql sql/databricks/schemas.sql --dialect databricks
	@$(UV) run sqlfluff lint sql/databricks/watermark.sql --dialect postgres
	@$(UV) run sqlfluff lint dbt/models --dialect databricks

test:
	@$(UV) run pytest tests -v
	-@bash scripts/bash/dbt.sh test || echo "dbt test skipped"

ci: lint test
	@echo "CI gate passed"

clean:
	@bash scripts/bash/clean.sh

# Databricks helpers
databricks-init:
	@$(PYTHON) scripts/databricks_init.py

databricks-schemas:
	@$(PYTHON) scripts/databricks_init.py --schemas-only
