"""Airflow 3: S3 landing -> Snowflake RAW, audit feeds, dbt build, optional reconciliation.

Each COPY writes one AUDIT.LOAD_BATCH row whether it succeeds or fails, so load history
is queryable next to the data it produced.
"""
from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import dag, get_current_context, task
from banking_pipeline.snowflake_copy import read_copy_statement, summarize_copy

log = logging.getLogger(__name__)

SNOWFLAKE_CONN_ID = "snowflake_banking"
# docker-compose.snowflake.yml mounts ./snowflake here (and ./scripts under it).
SQL_HOME = Path(os.getenv("SNOWFLAKE_SQL_HOME", "/opt/airflow/snowflake"))
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/banking_dbt")
DBT_BIN = os.getenv("DBT_BIN", "/opt/airflow/dbt_venv/bin/dbt")

LOAD_BATCH_INSERT = """
INSERT INTO AUDIT.LOAD_BATCH (airflow_dag_id, airflow_run_id, target_table, window_start,
    window_end, files_loaded, rows_loaded, rows_rejected, status, error_message,
    started_at, finished_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
"""


def _run_copy(cur, file_name: str, target_table: str) -> None:
    """Run one COPY script, record it in AUDIT.LOAD_BATCH, and raise if any file failed."""
    ctx = get_current_context()
    started = datetime.now(timezone.utc)
    window_start = ctx.get("data_interval_start") or started
    window_end = ctx.get("data_interval_end") or started
    try:
        cur.execute(read_copy_statement(SQL_HOME, file_name))
        summary = summarize_copy(cur.fetchall())
        status = "SUCCESS" if summary.ok else "FAILED"
        error = summary.first_error
    except Exception as exc:  # recorded, then re-raised below
        log.exception("COPY %s failed", file_name)
        summary, status, error = None, "FAILED", str(exc)[:1024]
    cur.execute(LOAD_BATCH_INSERT, (
        ctx["dag"].dag_id, ctx["run_id"], target_table, window_start, window_end,
        summary.files_loaded if summary else 0, summary.rows_loaded if summary else 0,
        summary.rows_rejected if summary else 0, status, error, started))
    if status == "FAILED":
        raise RuntimeError(f"{target_table} COPY failed: {error}")
    log.info("%s: files=%s rows=%s rejected=%s", target_table, summary.files_loaded,
             summary.rows_loaded, summary.rows_rejected)


def _snowflake_cursor_session(copies: list[tuple[str, str]]) -> None:
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("USE WAREHOUSE BANKING_ETL_WH")
            cur.execute("USE DATABASE BANKING_LAKEHOUSE")
            try:
                for file_name, target in copies:
                    _run_copy(cur, file_name, target)
            finally:
                conn.commit()  # keep LOAD_BATCH rows for failed loads too
    finally:
        conn.close()


@dag(dag_id="banking_s3_snowflake", start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
     schedule="*/15 * * * *", catchup=False, max_active_runs=1,
     tags=["banking", "cdc", "snowflake", "dbt", "reconciliation"])
def banking_s3_snowflake():
    @task(retries=2)
    def load_raw():
        _snowflake_cursor_session([("03_copy_into_raw.sql", "RAW.CDC_EVENTS")])

    @task(retries=2)
    def load_audit_feeds():
        _snowflake_cursor_session([
            ("07_copy_source_windows.sql", "AUDIT.SOURCE_ACCOUNT_WINDOW"),
            ("07b_copy_dead_letters.sql", "AUDIT.DEAD_LETTER_EVENT"),
        ])

    @task(retries=1)
    def build_models():
        env = os.environ.copy()
        env.setdefault("DBT_TARGET", "prod")
        # The project may be mounted read-only, so artifacts and logs go to /tmp.
        subprocess.run(
            [DBT_BIN, "build", "--project-dir", DBT_PROJECT_DIR,
             "--profiles-dir", f"{DBT_PROJECT_DIR}/profiles",
             "--target-path", "/tmp/dbt/target", "--log-path", "/tmp/dbt/logs"],
            check=True, env=env, timeout=1800)

    @task(retries=0)
    def optional_reconciliation():
        snapshot_id = os.getenv("SOURCE_SNAPSHOT_ID", "").strip()
        if not snapshot_id:
            log.info("No SOURCE_SNAPSHOT_ID; no source-to-target reconciliation is claimed")
            return
        if os.getenv("RECON_WATERMARK_VERIFIED", "false").lower() != "true":
            raise RuntimeError("Set RECON_WATERMARK_VERIFIED=true only after verifying "
                               "source/warehouse CDC watermarks")
        subprocess.run(["python3", str(SQL_HOME / "scripts" / "reconcile.py"),
                        "--snapshot-run-id", snapshot_id, "--source-caught-up"],
                       check=True, env=os.environ.copy(), timeout=900)

    load_raw() >> load_audit_feeds() >> build_models() >> optional_reconciliation()


banking_s3_snowflake()
