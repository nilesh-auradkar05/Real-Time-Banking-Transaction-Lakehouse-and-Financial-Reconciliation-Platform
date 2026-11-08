"""Persist a source-vs-warehouse current-state account/window reconciliation.

Run only after source and warehouse have reached the same CDC high watermark, or
pause the source at a controlled window boundary. Otherwise results can be false
mismatches even when all systems are working as designed.
"""
from __future__ import annotations
import argparse
import os
from pathlib import Path
import snowflake.connector


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--snapshot-run-id', required=True)
    parser.add_argument('--tolerance', type=float, default=0.02)
    parser.add_argument('--source-caught-up', action='store_true',
                        help='Acknowledge verified common source/warehouse CDC watermark')
    args = parser.parse_args()
    if not args.source_caught_up:
        parser.error('Verify common CDC watermark before using --source-caught-up')
    conn = snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'BANKING_ETL_WH'),
        database='BANKING_LAKEHOUSE', schema='AUDIT', autocommit=False,
    )
    try:
        cur = conn.cursor()
        cur.execute("""SELECT MIN(window_start), MAX(window_end), COUNT(*)
                    FROM AUDIT.SOURCE_ACCOUNT_WINDOW WHERE snapshot_run_id=%s""",
                    (args.snapshot_run_id,))
        start, end, count = cur.fetchone()
        if not count:
            raise RuntimeError('Source snapshot is empty; not a successful reconciliation')
        cur.execute("""INSERT INTO AUDIT.RECON_RUN
                    (snapshot_run_id,window_start,window_end,amount_tolerance,status,started_at)
                    VALUES (%s,%s,%s,%s,'RUNNING',CURRENT_TIMESTAMP())""",
                    (args.snapshot_run_id, start, end, args.tolerance))
        cur.execute("""SELECT MAX(recon_run_id) FROM AUDIT.RECON_RUN
                    WHERE snapshot_run_id=%s""", (args.snapshot_run_id,))
        run_id = cur.fetchone()[0]
        sql_root = Path(__file__).resolve().parents[1]
        sql_path = (sql_root / 'sql/07_reconciliation.sql') if (sql_root / 'sql/07_reconciliation.sql').exists() else (sql_root / 'snowflake/sql/07_reconciliation.sql')
        sql = sql_path.read_text()
        sql = sql[sql.index('INSERT INTO'):]
        cur.execute(sql, (args.snapshot_run_id,args.snapshot_run_id,run_id,args.tolerance))
        cur.execute("""INSERT INTO AUDIT.RECON_EXCEPTION
            (recon_run_id,account_id,exception_type,detail,detected_at)
            SELECT recon_run_id,account_id,'COUNT_OR_AMOUNT_MISMATCH',
                   OBJECT_CONSTRUCT('count_diff',count_diff,'amount_diff',amount_diff)::varchar,
                   CURRENT_TIMESTAMP()
            FROM AUDIT.RECON_ACCOUNT_WINDOW
            WHERE recon_run_id=%s AND status='MISMATCH'""",(run_id,))
        cur.execute("""UPDATE AUDIT.RECON_RUN SET
              accounts_checked=(SELECT COUNT(*) FROM AUDIT.RECON_ACCOUNT_WINDOW WHERE recon_run_id=%s),
              accounts_mismatched=(SELECT COUNT(*) FROM AUDIT.RECON_ACCOUNT_WINDOW
                                   WHERE recon_run_id=%s AND status='MISMATCH'),
              status=IFF((SELECT COUNT(*) FROM AUDIT.RECON_ACCOUNT_WINDOW
                          WHERE recon_run_id=%s AND status='MISMATCH')=0,'MATCH','MISMATCH'),
              finished_at=CURRENT_TIMESTAMP()
              WHERE recon_run_id=%s""",(run_id,run_id,run_id,run_id))
        conn.commit()
        print('recon_run_id=' + str(run_id))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__=='__main__':
    main()
