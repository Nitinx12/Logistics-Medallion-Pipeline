# docker/Dockerfile - Airflow 3.x + dbt + Spark + GX
FROM apache/airflow:3.0.6-python3.11

USER airflow
COPY --chown=airflow:root pyproject.toml uv.lock ./
COPY --chown=airflow:root jars/ ./jars/
COPY --chown=airflow:root config/ ./config/

# Install uv and project deps
USER root
RUN pip install uv && uv sync --group dev --frozen || pip install -e .[dev]

USER airflow
WORKDIR /opt/airflow
