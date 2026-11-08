{{ config(materialized='table') }}

with latest as (
{{ latest_cdc('fin_disp', 'disp_id') }}
)
select
    source_pk::number(38,0) as disp_id,
    try_cast(row_json:client_id::varchar as number(38,0)) as client_id,
    try_cast(row_json:account_id::varchar as number(38,0)) as account_id,
    nullif(trim(row_json:disp_type::varchar), '') as disp_type,
    try_cast(row_json:synthetic_batch_id::varchar as number(38,0)) as synthetic_batch_id,
    nullif(trim(row_json:data_origin::varchar), '') as data_origin,
    try_cast(row_json:last_replay_event_id::varchar as number(38,0)) as last_replay_event_id,
    {{ cdc_timestamp("row_json:updated_at") }} as updated_at,
    op = 'd' as is_deleted,
    event_id as last_event_id,
    event_ts_ms as source_event_ts_ms,
    raw_loaded_at as warehouse_loaded_at,
    kafka_offset as last_kafka_offset
from latest