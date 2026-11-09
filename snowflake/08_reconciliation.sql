-- Run by scripts/reconcile.py, which binds SNAPSHOT_RUN_ID, TOLERANCE and AUDIT.RECON_RUN_ID.
-- This is a CURRENT-STATE source-vs-target comparison over business-date windows.
-- CDC replay-event reconciliation requires replicated replay_run/replay_event and is not inferred from this query.

INSERT INTO AUDIT.RECON_ACCOUNT_WINDOW (
    recon_run_id,account_id,data_origin,source_count,target_count,count_diff,
    source_amount,target_amount,amount_diff,status
)
WITH source_group AS (
    SELECT account_id,lower(coalesce(data_origin,'unknown')) AS data_origin,
           sum(source_count) as source_count, sum(source_amount) as source_amount
    FROM AUDIT.SOURCE_ACCOUNT_WINDOW
    WHERE snapshot_run_id = %s GROUP BY 1,2
), target_group AS (
    SELECT t.account_id,lower(coalesce(t.data_origin,'unknown')) as data_origin,
           count(*) as target_count,sum(t.amount)::number(18,2) as target_amount
    FROM SILVER.FIN_TRANS t
    JOIN (SELECT min(window_start) as window_start,max(window_end) as window_end
          FROM AUDIT.SOURCE_ACCOUNT_WINDOW WHERE snapshot_run_id = %s) bounds
    ON t.trans_date >= bounds.window_start and t.trans_date <= bounds.window_end
    WHERE not t.is_deleted
    GROUP BY 1,2
), compared AS (
    SELECT coalesce(s.account_id,t.account_id) as account_id,
           coalesce(s.data_origin,t.data_origin) as data_origin,
           coalesce(s.source_count,0) as source_count,coalesce(t.target_count,0) as target_count,
           coalesce(s.source_amount,0)::number(18,2) as source_amount,
           coalesce(t.target_amount,0)::number(18,2) as target_amount
    FROM source_group s FULL OUTER JOIN target_group t
    ON s.account_id=t.account_id and s.data_origin=t.data_origin
)
SELECT %s,account_id,data_origin,source_count,target_count,
    source_count-target_count,source_amount,target_amount,
    source_amount-target_amount,
    IFF(source_count=target_count AND abs(source_amount-target_amount)<=%s,'MATCH','MISMATCH')
FROM compared;