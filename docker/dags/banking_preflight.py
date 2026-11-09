"""Manual preflight for the warehouse path. Run it once after `docker compose --profile airflow up`
and after any credential or IAM change, before enabling banking_s3_snowflake.

Each task checks one dependency, so a failure names the broken piece:
mounts -> dbt profile and connection -> Snowflake objects and S3 access through the stage.
"""
from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import dag, task

log = logging.getLogger(__name__)

SNOWFLAKE_CONN_ID = "snowflake_banking"
SQL_HOME = Path(os.getenv("SNOWFLAKE_SQL_HOME", "/opt/airflow/snowflake"))
SCRIPTS_HOME = Path(os.getenv("BANKING_SCRIPTS_HOME", "/opt/airflow/scripts"))
DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", "/opt/airflow/banking_dbt"))
DBT_BIN = os.getenv("DBT_BIN", "/opt/airflow/dbt_venv/bin/dbt")

REQUIRED_FILES = [
    SQL_HOME / "03_copy_into_raw.sql",
    SQL_HOME / "07_copy_source_windows.sql",
    SQL_HOME / "07b_copy_dead_letters.sql",
    SQL_HOME / "08_reconciliation.sql",
    SCRIPTS_HOME / "reconcile.py",
    DBT_PROJECT_DIR / "dbt_project.yml",
    DBT_PROJECT_DIR / "profiles" / "profiles.yml",
]

# Cheap statements that fail with a specific Snowflake error when a grant or object is missing.
# LIST with a pattern that matches nothing still authenticates to S3 through the integration,
# so it catches a broken IAM trust policy without scanning the bucket.
SNOWFLAKE_CHECKS = [
    ("session", "SELECT CURRENT_ACCOUNT(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()"),
    ("raw table", "SELECT COUNT(*) FROM BANKING_LAKEHOUSE.RAW.CDC_EVENTS"),
    ("audit table", "SELECT COUNT(*) FROM BANKING_LAKEHOUSE.AUDIT.LOAD_BATCH"),
    ("cdc stage -> S3", "LIST @BANKING_LAKEHOUSE.RAW.BANKING_CDC_STAGE PATTERN = '.*__preflight__.*'"),
]


def dbt_command(*args: str) -> list[str]:
    return [DBT_BIN, *args, "--project-dir", str(DBT_PROJECT_DIR),
            "--profiles-dir", str(DBT_PROJECT_DIR / "profiles")]


@dag(dag_id="banking_preflight", start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
     schedule=None, catchup=False, max_active_runs=1, tags=["banking", "preflight"])
def banking_preflight():
    @task
    def check_mounts():
        absent = [str(p) for p in REQUIRED_FILES if not p.exists()]
        if absent:
            raise FileNotFoundError(f"missing mounted files: {absent}")
        version = subprocess.run([DBT_BIN, "--version"], check=True, capture_output=True,
                                 text=True, timeout=120).stdout
        log.info("mounts ok; %s", version.splitlines()[0] if version else "dbt found")

    @task
    def check_dbt_connection():
        # Writes nothing into the (read-only) project: dbt debug only reads profile and project.
        subprocess.run(dbt_command("debug"), check=True, timeout=300,
                       env={**os.environ, "DBT_LOG_PATH": "/tmp/dbt/logs"})

    @task
    def check_snowflake_objects():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        failures = []
        for name, sql in SNOWFLAKE_CHECKS:
            try:
                rows = hook.get_records(sql)
                log.info("ok   %-16s %s", name, rows[:1])
            except Exception as exc:  # report every broken check, not just the first
                failures.append(f"{name}: {str(exc).splitlines()[0]}")
                log.error("FAIL %-16s %s", name, exc)
        if failures:
            raise RuntimeError("; ".join(failures))

    check_mounts() >> check_dbt_connection() >> check_snowflake_objects()


banking_preflight()
