"""
Airflow 3: Snowflake COPY landing, audit loads, dbt build, optional reconcillation.
"""

from __future__ import annotations
from datetime import datetime, timezone
import os
from pathlib import Path
from loguru import logger
from airflow.sdk import dag, task, get_current_context

SQL_HOME=Path('/opt/airflow/snowflake/sql')

def _copy_with_counts(cur, file_name: str):
    text=(SQL_HOME/file_name).read_text()
    sql=text[text.index('COPY INTO'):].strip()
    cur.execute(sql)
    data=cur.fetchall()

    # Snowflake COPY result: file,status,rows_parsed,rows_loaded,error_limit,errors_seen,...
    files=sum(1 for row in data if row[1] in ('LOADED','PARTIALLY LOADED'))
    loaded=sum(int(row[3] or 0) for row in data if row[1] in ('LOADED','PARTIALLY LOADED'))
    rejected=sum(int(row[5] or 0) for row in data if row[1] in ('LOADED','PARTIALLY LOADED'))
    return files,loaded,rejected

@dag(dag_id='banking_s3_snowflake', start_date=datetime(2026,1,1,tzinfo=timezone.utc),
    schedule='*/15 * * * *',catchup=False,max_active_runs=1,
    tags=['banking','cdc','snowflake','dbt','reconciliation'])
def banking_s3_snowflake():
    @task(retries=2)
    def load_raw():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        ctx=get_current_context()
        hook=SnowflakeHook(snowflake_conn_id='snowflake_banking')
        started=datetime.now(timezone.utc)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('USE WAREHOUSE BANKING_ETL_WH')
                cur.execute('USE DATABASE BANKING_LAKEHOUSE')
                try:
                    files,rows,rejected=_copy_with_counts(cur,'03_copy_into_raw.sql')
                    status='SUCCESS';error_message=None
                except Exception as e:
                    files=rows=rejected=0;status='FAILED';error_message=str(e)[:1024]
                    logger.exception('RAW COPY failed')
                cur.execute("""
                INSERT INTO AUDIT.LOAD_BATCH (airflow_dag_id,airlfow_run_id,target_table,window_start,window_end,files_loaded,rows_loaded,rows_rejected,status,error_message,started_at,finished_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP())""",
                (ctx['dag'].dag_id,ctx['run_id'],'RAW.CDC_EVENTS',started,
                datetime.now(timezone.utc),files,rows,rejected,status,error_message,started))
            conn.commit()
            if status=='FAILED':
                raise RuntimeError(error_message)
            logger.info(f'RAW load: files={files} rows={rows} rejected={rejected}')

    @task(retries=2)
    def load_audit_feeds():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook=SnowflakeHook(snowflake_conn_id='snowflake_banking')
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('USE DATABASE BANKING_LAKEHOUSE')
                for sql in ['07_copy_source_windows.sql','07b_copy_dead_letters.sql']:
                    files,rows,rejected=_copy_with_counts(cur,sql)
                    logger.info(f"{sql}: files={files} rows={rows} rejected={rejected}")
            conn.commit()

    @task(retries=1)
    def build_models():
        env=os.environ.copy()
        env['DNT_PROFILES_DIR']='/opt/airflow/banking_dbt/.dbt'
        subprocess.run(['/opt/airflow/dbt_venv/bin/dbt','build',
            '--project-dir','/opt/airflow/banking_dbt',
            '--profiles-dir',env['DBT_PROFILES_DIR']],check=True,env=env,timeout=1800)

    @task(retries=0)
    def optional_reconciliation():
        snapshot_id=os.getenv('SOURCE_SNAPSHOT_ID','').strip()
        if not snapshot_id:
            logger.info('No SOURCE_SNAPSHOT_ID; no source-to-target reconciliation will be claimed')
            return
        if os.getenv('RECON_WATERMARK_VERIFIED','false').lower() != 'true':
            raise RuntimeError('Set RECON_WATERMARK_VERIFIED=true only after verifying source/warehouse CDC watermarks')
        env=os.environ.copy()
        subprocess.run(['python3','/opt/airflow/snowflake/scripts/reconcile.py',
            '--snapshot-run-id',snapshot_id,'--source-caught-up'],
            check=True,env=env,timeout=900)

    raw = load_raw()
    audit_trans = load_audit_feeds()
    models = build_models()

    raw >> audit_trans >> models >> optional_reconciliation()

banking_s3_snowflake()