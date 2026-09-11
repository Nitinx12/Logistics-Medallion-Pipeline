# docker/Dockerfile - Airflow 3.x + dbt + Spark + GX
FROM apache/airflow:3.0.6-python3.11

USER airflow
COPY --chown=airflow:root pyproject.toml uv.lock docker/requirements.txt ./requirements.txt
COPY --chown=airflow:root jars/ ./jars/
COPY --chown=airflow:root config/ ./config/
COPY --chown=airflow:root docker/entrypoint.sh /entrypoint.sh

# Install uv and project deps, prefer requirements.txt for pip fallback
USER root
RUN chmod +x /entrypoint.sh && pip install uv && uv sync --group dev --frozen || pip install -r requirements.txt || pip install -e .[dev]

USER airflow
WORKDIR /opt/airflow
ENTRYPOINT ["/entrypoint.sh"]
