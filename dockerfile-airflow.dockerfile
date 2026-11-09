ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Re-declare build arguments used after FROM.
ARG AIRFLOW_VERSION

USER airflow

# Pin Airflow while adding providers so pip cannot change the base image version.
# fab: username/password login (FabAuthManager). snowflake: the COPY and preflight tasks.
# python-dotenv: scripts/reconcile.py loads snowflake/.env (only transitively present otherwise).
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      apache-airflow-providers-fab \
      apache-airflow-providers-snowflake \
      python-dotenv

ENV DBT_VENV_PATH=/opt/airflow/dbt_venv

# Keep dbt isolated from Airflow's Python dependencies. Same minor version as CI
# (.github/workflows/ci.yml) so the emulator build exercises the parser that runs here.
RUN python -m venv "${DBT_VENV_PATH}" \
    && "${DBT_VENV_PATH}/bin/pip" install --no-cache-dir \
      "dbt-core==1.10.*" \
      "dbt-snowflake==1.10.*"

# Snowflake connection from snowflake/.env (see docker/airflow/entrypoint.sh).
COPY --chown=airflow:root docker/dags/banking_pipeline/connections.py /opt/airflow/banking/snowflake_conn.py
COPY --chown=airflow:root --chmod=0755 docker/airflow/entrypoint.sh /opt/airflow/banking/entrypoint.sh
ENTRYPOINT ["/opt/airflow/banking/entrypoint.sh"]
