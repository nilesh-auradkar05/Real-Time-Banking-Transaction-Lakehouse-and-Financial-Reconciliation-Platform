#!/usr/bin/env bash
# Wraps the official Airflow entrypoint. When snowflake/.env provides SNOWFLAKE_* variables,
# export them as the `snowflake_banking` connection that the DAGs use.
set -euo pipefail

if [[ -z "${AIRFLOW_CONN_SNOWFLAKE_BANKING:-}" && -n "${SNOWFLAKE_ACCOUNT:-}" ]]; then
  if conn="$(python3 /opt/airflow/banking/snowflake_conn.py)"; then
    export AIRFLOW_CONN_SNOWFLAKE_BANKING="${conn}"
  fi
fi

exec /usr/bin/dumb-init -- /entrypoint "$@"
