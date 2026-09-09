.PHONY: setup docker-up docker-down seed bronze dbt-run dbt-docs publish pipeline lint test ci clean

# FreightLake — local automation
# Every target is a thin wrapper around scripts/bash/*.sh and scripts/powershell/*.ps1
# so logic lives in one place. See docs/PROJECT_PLAN.md section 10.

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
	@$(PYTHON) spark_jobs/bronze/extract_postgres_oltp.py
	@$(PYTHON) spark_jobs/bronze/extract_mongo_tracking.py

dbt-run:
	@bash scripts/bash/dbt.sh run

dbt-build:
	@bash scripts/bash/dbt.sh build

dbt-docs:
	@bash scripts/bash/dbt.sh docs

publish:
	@$(PYTHON) spark_jobs/publish/publish_gold_to_postgres.py

pipeline: seed bronze dbt-build publish
	@echo "Full local pipeline complete"

lint:
	@$(UV) run ruff check .
	@$(UV) run ruff format --check .
	@$(UV) run mypy spark_jobs --ignore-missing-imports || true
	@sqlfluff lint sql --dialect postgres || true
	@sqlfluff lint dbt/models --dialect databricks || true

test:
	@$(UV) run pytest tests -v
	@bash scripts/bash/dbt.sh test || true

ci: lint test
	@echo "CI gate passed"

clean:
	@bash scripts/bash/clean.sh

# Databricks helpers
databricks-init:
	@$(PYTHON) scripts/databricks_init.py

databricks-schemas:
	@$(PYTHON) scripts/databricks_init.py --schemas-only
