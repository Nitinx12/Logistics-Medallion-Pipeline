# docker/Dockerfile - Airflow 3.x + dbt + Spark + GX
FROM apache/airflow:3.0.6-python3.11

# The base image ships Python 3.11, but pyproject.toml requires >= 3.13 and
# uv.lock is resolved for 3.13, so a plain `uv sync --frozen` fails here.
# uv downloads its own managed CPython 3.13 into UV_PYTHON_INSTALL_DIR,
# pre-created and owned by the airflow user so DAG tasks can `uv run`.
# No pip fallback on purpose: a failed sync must fail the build loudly
# instead of silently shipping a divergent environment.
USER root
ENV UV_PYTHON_INSTALL_DIR=/opt/uv-python
RUN pip install uv && mkdir -p /opt/uv-python && chown airflow:root /opt/uv-python

USER airflow
WORKDIR /opt/airflow
COPY --chown=airflow:root pyproject.toml uv.lock ./
COPY --chown=airflow:root docker/requirements.txt ./requirements.txt
COPY --chown=airflow:root jars/ ./jars/
COPY --chown=airflow:root config/ ./config/
COPY --chown=airflow:root docker/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh && uv sync --group dev --frozen --python 3.13

ENTRYPOINT ["/entrypoint.sh"]
