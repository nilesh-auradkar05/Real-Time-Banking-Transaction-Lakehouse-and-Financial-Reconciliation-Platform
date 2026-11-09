{{ config(materialized='table') }}

with latest as (
{{ latest_cdc('fin_loan', 'loan_id') }}
)
select
    source_pk::number(38,0) as loan_id,
    try_cast(row_json:account_id::varchar as number(38,0)) as account_id,
    {{ cdc_date("row_json:granted_date") }} as granted_date,
    try_cast(row_json:amount::varchar as number(18,2)) as amount,
    try_cast(row_json:duration::varchar as number(38,0)) as duration,
    try_cast(row_json:payments::varchar as number(18,2)) as payments,
    nullif(trim(row_json:status::varchar), '') as status,
    nullif(trim(row_json:data_origin::varchar), '') as data_origin,
    try_cast(row_json:last_replay_event_id::varchar as number(38,0)) as last_replay_event_id,
    {{ cdc_timestamp("row_json:updated_at") }} as updated_at,
    op = 'd' as is_deleted,
    event_id as last_event_id,
    event_ts_ms as source_event_ts_ms,
    raw_loaded_at as warehouse_loaded_at,
    first_event_ts_ms as source_first_committed_ts_ms,
    first_raw_loaded_at as warehouse_first_loaded_at,
    snapshot_read_count,
    kafka_offset as last_kafka_offset
from latest