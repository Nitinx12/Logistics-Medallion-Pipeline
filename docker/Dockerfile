FROM apache/airflow:2.10.4-python3.11

USER airflow

COPY --chown=airflow:root docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY --chown=airflow:root pyproject.toml uv.lock /opt/airflow/
COPY --chown=airflow:root spark_jobs /opt/airflow/spark_jobs
COPY --chown=airflow:root dbt /opt/airflow/dbt

ENV PYTHONPATH=/opt/airflow:${PYTHONPATH}
