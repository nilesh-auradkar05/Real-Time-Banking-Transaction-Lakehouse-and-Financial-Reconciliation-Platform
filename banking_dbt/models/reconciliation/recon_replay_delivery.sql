-- Replay-delivery reconciliation: did every change the simulator committed reach the warehouse
-- exactly once, and how long did each hop take?
--
-- Expected side: AUDIT.REPLAY_EVENT, synced from PostgreSQL by Airflow outside Debezium/Kafka.
-- Delivered side: stg_cdc_events (one row per Kafka coordinate).
--   c / u  match on the replay id in the after-image (last_replay_event_id).
--   d      matches on (table, record key): a delete's before-image carries the PREVIOUS id.
--   r      re-snapshot reads re-send existing state; they are never deliveries. A read whose
--          after-image carries this change's replay id proves the change's STATE arrived even
--          though its change event did not (RECOVERED_BY_SNAPSHOT).
--
-- delivery_status
--   DELIVERED           exactly one delivery
--   DUPLICATE_DELIVERY  same change at more than one Kafka coordinate (at-least-once re-send)
--   RECOVERED_BY_SNAPSHOT  change event lost, but a later snapshot read carries its exact state
--                       (insert/update only; a snapshot cannot show a delete). History gap: no
--                       commit-time event, so SCD2 misses that version and latency is unknown.
--   MISSING             committed before as_of - grace and neither delivered nor recovered.
--                       is_acknowledged marks reviewed losses (AUDIT.RECON_ACKNOWLEDGEMENT).
--   PENDING             committed within the grace window, or planned in a still-running replay
--   NOT_COMMITTED       planned by a replay that ended without committing it
--   UNEXPECTED_CHANGE   warehouse change that no replay event explains (judged only for commits
--                       older than the last sync, which must already have seen their replay row)
{% set grace = var('replay_grace_minutes') %}
with as_of as (
    select
        {% if var('recon_as_of', none) -%}
        '{{ var("recon_as_of") }}'::timestamp_ntz
        {%- else -%}
        convert_timezone('UTC', current_timestamp())::timestamp_ntz
        {%- endif %} as as_of_at,
        (select max(convert_timezone('UTC', synced_at)::timestamp_ntz)
           from {{ source('audit', 'replay_event') }}) as last_synced_at
), expected as (
    select e.replay_event_id, e.replay_run_id, r.status as run_status,
           lower(e.source_table) as source_table, e.record_pk, lower(e.operation) as operation,
           e.data_origin, e.business_date, e.amount,
           convert_timezone('UTC', e.intended_commit_at)::timestamp_ntz as intended_commit_at,
           convert_timezone('UTC', e.actual_commit_at)::timestamp_ntz as actual_commit_at,
           e.injected_delay_seconds, e.injected_fault
    from {{ source('audit', 'replay_event') }} e
    left join {{ source('audit', 'replay_run') }} r on r.replay_run_id = e.replay_run_id
), cdc as (
    select replay_event_id, source_table, record_pk, op, event_id, data_origin,
           to_timestamp_ntz(event_ts_ms, 3) as source_committed_at,
           to_timestamp_ntz(kafka_timestamp_ms, 3) as kafka_at,
           {{ cdc_timestamp('landing_ingested_at') }} as landed_at,
           convert_timezone('UTC', raw_loaded_at)::timestamp_ntz as loaded_at,
           topic, kafka_partition, kafka_offset
    from {{ ref('stg_cdc_events') }}
    where op in ('c', 'u', 'd')
), snapshot_reads as (
    select replay_event_id, source_table, record_pk, event_id,
           convert_timezone('UTC', raw_loaded_at)::timestamp_ntz as loaded_at
    from {{ ref('stg_cdc_events') }}
    where op = 'r' and replay_event_id is not null
    qualify row_number() over (partition by replay_event_id, source_table, record_pk
                               order by raw_loaded_at, event_id) = 1
), acks as (
    select e.replay_event_id, a.reason as acknowledgement_reason,
           convert_timezone('UTC', a.acknowledged_at)::timestamp_ntz as acknowledged_at
    from expected e
    join {{ source('audit', 'recon_acknowledgement') }} a
      on (a.replay_event_id = e.replay_event_id
          or (a.replay_event_id is null and a.replay_run_id = e.replay_run_id))
     -- an acknowledgement covers only what had been committed when it was given
     and e.actual_commit_at <= convert_timezone('UTC', a.acknowledged_at)::timestamp_ntz
    qualify row_number() over (partition by e.replay_event_id
                               order by iff(a.replay_event_id is null, 1, 0), a.acknowledged_at) = 1
), by_replay_id as (
    select *, count(*) over (partition by replay_event_id) as delivery_count
    from cdc
    where op in ('c', 'u') and replay_event_id is not null
    qualify row_number() over (partition by replay_event_id
                               order by kafka_at, topic, kafka_partition, kafka_offset) = 1
), by_delete_key as (
    select *, count(*) over (partition by source_table, record_pk) as delivery_count
    from cdc
    where op = 'd'
    qualify row_number() over (partition by source_table, record_pk
                               order by kafka_at, topic, kafka_partition, kafka_offset) = 1
), matched as (
    select e.*,
           coalesce(i.event_id, d.event_id) as first_event_id,
           coalesce(i.delivery_count, d.delivery_count, 0) as delivery_count,
           coalesce(i.source_committed_at, d.source_committed_at) as source_committed_at,
           coalesce(i.kafka_at, d.kafka_at) as kafka_at,
           coalesce(i.landed_at, d.landed_at) as landed_at,
           coalesce(i.loaded_at, d.loaded_at) as loaded_at,
           r.event_id as recovered_by_event_id,
           r.loaded_at as recovered_loaded_at,
           k.acknowledgement_reason, k.acknowledged_at
    from expected e
    left join by_replay_id i
      on e.operation in ('c', 'u') and i.replay_event_id = e.replay_event_id
    left join by_delete_key d
      on e.operation = 'd' and d.source_table = e.source_table and d.record_pk = e.record_pk
    left join snapshot_reads r
      on e.operation in ('c', 'u') and r.replay_event_id = e.replay_event_id
     and r.source_table = e.source_table and r.record_pk = e.record_pk
    left join acks k on k.replay_event_id = e.replay_event_id
), classified as (
    select m.*,
        case
            when m.actual_commit_at is null and coalesce(m.run_status, 'running') = 'running' then 'PENDING'
            when m.actual_commit_at is null then 'NOT_COMMITTED'
            when m.delivery_count > 1 then 'DUPLICATE_DELIVERY'
            when m.delivery_count = 1 then 'DELIVERED'
            when m.recovered_by_event_id is not null then 'RECOVERED_BY_SNAPSHOT'
            when m.actual_commit_at >= dateadd('minute', -{{ grace }}, a.as_of_at) then 'PENDING'
            else 'MISSING'
        end as delivery_status
    from matched m
    cross join as_of a
), expected_ids as (
    select distinct replay_event_id from expected
), expected_deletes as (
    select distinct source_table, record_pk from expected where operation = 'd'
), unexpected as (
    select c.*
    from cdc c
    cross join as_of a
    left join expected_ids x on x.replay_event_id = c.replay_event_id
    left join expected_deletes xd
      on xd.source_table = c.source_table and xd.record_pk = c.record_pk
    where c.source_committed_at < a.last_synced_at
      and (
            -- a replay id that the expected set does not contain
            (c.op in ('c', 'u') and c.replay_event_id is not null and x.replay_event_id is null)
            -- the simulator is the only writer that updates or deletes
         or (c.op = 'u' and c.replay_event_id is null)
         or (c.op = 'd' and xd.record_pk is null)
      )
)
select
    replay_event_id, replay_run_id, run_status, source_table, record_pk, operation,
    data_origin, business_date, amount, injected_fault, injected_delay_seconds,
    delivery_status, delivery_count, first_event_id,
    delivery_status = 'MISSING' and acknowledgement_reason is not null as is_acknowledged,
    acknowledgement_reason, acknowledged_at, recovered_by_event_id, recovered_loaded_at,
    intended_commit_at, actual_commit_at, source_committed_at, kafka_at, landed_at, loaded_at,
    datediff('millisecond', intended_commit_at, actual_commit_at) / 1000.0 as commit_delay_seconds,
    datediff('millisecond', source_committed_at, kafka_at) / 1000.0 as capture_seconds,
    datediff('millisecond', kafka_at, landed_at) / 1000.0 as landing_seconds,
    datediff('millisecond', landed_at, loaded_at) / 1000.0 as load_seconds,
    datediff('millisecond', actual_commit_at, loaded_at) / 1000.0 as end_to_end_seconds
from classified
union all
select
    replay_event_id, null, null, source_table, record_pk, op,
    data_origin, null, null, null, null,
    'UNEXPECTED_CHANGE', 1, event_id,
    false, null, null, null, null,
    null, null, source_committed_at, kafka_at, landed_at, loaded_at,
    null, datediff('millisecond', source_committed_at, kafka_at) / 1000.0,
    datediff('millisecond', kafka_at, landed_at) / 1000.0,
    datediff('millisecond', landed_at, loaded_at) / 1000.0, null
from unexpected