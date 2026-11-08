"""Publish a PostgreSQL current-state account/window snapshot to S3 for reconciliation.

Requires: pip install 'psycopg[binary]' boto3; PG_DSN environment/secret, AWS IAM auth.
Choose window on BUSINESS DATE, not the date CDC events were ingested.
Requires quiescent source/warehouse or an agreed CDC watermark for reliable comparisons.
"""
import argparse
import json
import os
import uuid
from datetime import datetime, timezone

import boto3
import psycopg

SQL = """
SELECT account_id,lower(coalesce(data_origin,'unknown')) as data_origin,
       count(*)::bigint as source_count,round(sum(amount)::numeric,2) as source_amount
FROM public.fin_trans
WHERE trans_date >= %s AND trans_date <= %s
GROUP BY account_id,lower(coalesce(data_origin,'unknown'))
ORDER BY account_id,data_origin
"""

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--start',required=True,help='YYYY-MM-DD')
    parser.add_argument('--end',required=True,help='YYYY-MM-DD')
    parser.add_argument('--bucket',required=True)
    parser.add_argument('--prefix',default='landing/cdc')
    parser.add_argument('--snapshot-run-id',default=None)
    args=parser.parse_args()
    if args.start>args.end: raise SystemExit('start must be <= end')
    run_id=args.snapshot_run_id or uuid.uuid4().hex
    recorded_at=datetime.now(timezone.utc).isoformat()
    with psycopg.connect(os.environ['PG_DSN']) as conn:
        # REPEATABLE READ keeps source counts internally consistent during the scan.
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        rows=conn.execute(SQL,(args.start,args.end)).fetchall()
    jsonl=''.join(json.dumps(dict(snapshot_run_id=run_id,window_start=args.start,
        window_end=args.end,account_id=account_id,data_origin=origin,
        source_count=count,source_amount=str(amount),source_snapshot_at=recorded_at))+'\n'
        for account_id,origin,count,amount in rows)
    if not rows: raise SystemExit('No rows in source window; refusing empty snapshot')
    key=f"{args.prefix.strip('/')}/_audit/source_windows/run={run_id}.jsonl"
    boto3.client('s3').put_object(Bucket=args.bucket,Key=key,Body=jsonl.encode(),
         ContentType='application/x-ndjson',ServerSideEncryption='AES256',IfNoneMatch='*')
    print(f'snapshot_run_id={run_id} rows={len(rows)} s3://{args.bucket}/{key}')
if __name__=='__main__': main()
