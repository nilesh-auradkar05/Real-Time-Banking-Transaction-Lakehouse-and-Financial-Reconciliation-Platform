{{ config(materialized='table') }}
with events as (
    select coalesce(try_to_number(record_pk),try_to_number(after_json:client_id::varchar),
        try_to_number(before_json:client_id::varchar),
        try_to_number(key_json:client_id::varchar)) as client_id,
        coalesce(after_json,before_json) as state_json, op, event_id,
        topic,kafka_partition,kafka_offset,source_lsn,event_ts_ms,debezium_ts_ms,
        kafka_timestamp_ms,raw_loaded_at
    from {{ ref('stg_cdc_events') }} where source_table='fin_client' and op in ('r','c','u','d')
), ordered as (
    select *,
        lag(to_json(state_json)) over (partition by client_id order by try_to_number(source_lsn) asc nulls first,
        event_ts_ms asc nulls first,
        debezium_ts_ms asc nulls first, topic,kafka_partition,kafka_offset) as prev_state,
        lag(op='d') over (partition by client_id order by try_to_number(source_lsn) asc nulls first,
        event_ts_ms asc nulls first,
        debezium_ts_ms asc nulls first, topic,kafka_partition,kafka_offset) as prev_deleted,
        row_number() over (partition by client_id order by try_to_number(source_lsn) asc nulls first,
        event_ts_ms asc nulls first,
        debezium_ts_ms asc nulls first, topic,kafka_partition,kafka_offset) as event_seq
    from events where client_id is not null
), changed as (
    select 
        *,
        dateadd('microsecond', event_seq - 1,
        coalesce(to_timestamp_ntz(event_ts_ms,3),raw_loaded_at::timestamp_ntz)) as version_at
    from ordered
    where prev_state is null or prev_deleted is distinct from (op='d')
        or prev_state is distinct from to_json(state_json)
), versions as (
    select 
        *, 
        lead(version_at) over (partition by client_id order by event_seq) as valid_to
    from changed
)
select client_id, state_json, op='d' as is_deleted,event_id,
       version_at as valid_from,valid_to,valid_to is null as is_current,
       abs(hash(client_id,event_id))::number(38,0) as version_key,
       source_lsn,event_ts_ms,kafka_partition,kafka_offset
from versions