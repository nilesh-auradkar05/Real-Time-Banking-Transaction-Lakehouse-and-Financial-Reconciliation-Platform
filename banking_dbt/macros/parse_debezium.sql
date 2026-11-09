-- With JSON converter schemas disabled, PostgreSQL DATE often arrives as epoch DAYS.
-- Some sources use ISO strings. Support both without parsing date-only values as timestamps.
{% macro cdc_date(field) -%}
  (case when try_to_number({{ field }}::varchar) is not null
     then dateadd(day, try_to_number({{ field }}::varchar)::int, '1970-01-01'::date)
     else try_to_date({{ field }}::varchar) end)
{%- endmacro %}

-- Debezium timestamp without timezone is typically microseconds since epoch.
-- Timestamp-with-timezone may be epoch milliseconds or ISO string; the source
-- date carries no timezone, so cast as NTZ for reporting.
{% macro cdc_timestamp(field) -%}
  (case when try_to_number({{ field }}::varchar) is not null then
     case when abs(try_to_number({{ field }}::varchar)) >= 100000000000000
       then to_timestamp_ntz(try_to_number({{ field }}::varchar), 6)
       when abs(try_to_number({{ field }}::varchar)) >= 100000000000
       then to_timestamp_ntz(try_to_number({{ field }}::varchar), 3)
       else to_timestamp_ntz(try_to_number({{ field }}::varchar)) end
     else try_to_timestamp_ntz({{ field }}::varchar) end)
{%- endmacro %}

-- PostgreSQL LSN is ordered at source; Kafka offset is meaningful only inside one partition.
-- This assumes a single PostgreSQL connector/stream and key-stable Kafka partitioning.
-- A cross-partition re-key without WAL ordering is not safe to silently resolve.
{% macro cdc_order_desc(alias='') -%}
  try_to_number({{ alias }}source_lsn) desc nulls last,
  {{ alias }}event_ts_ms desc nulls last,
  {{ alias }}debezium_ts_ms desc nulls last,
  {{ alias }}kafka_timestamp_ms desc nulls last,
  {{ alias }}topic desc, {{ alias }}kafka_partition desc, {{ alias }}kafka_offset desc
{%- endmacro %}

{% macro latest_cdc(table_name, pk) -%}
  with changes as (
    select coalesce(try_to_number(record_pk), try_to_number(after_json:{{ pk }}::varchar),
                    try_to_number(before_json:{{ pk }}::varchar),
                    try_to_number(key_json:{{ pk }}::varchar)) as source_pk,
      coalesce(after_json, before_json) as row_json,
      after_json, before_json, op, event_id, topic, kafka_partition, kafka_offset,
      source_lsn, event_ts_ms, debezium_ts_ms, kafka_timestamp_ms, raw_loaded_at
    from {{ ref('stg_cdc_events') }}
    where source_table = '{{ table_name }}' and op in ('r','c','u','d')
  ), ranked as (
    select *, row_number() over (
      partition by source_pk order by {{ cdc_order_desc() }}
    ) as rn from changes where source_pk is not null
  )
  select * from ranked where rn=1
{%- endmacro %}

-- SCD2 history for one captured table. One version per change in row state or delete flag.
-- Ordering follows WAL position, then commit time, then Kafka coordinates (cdc_order_desc, ascending).
-- valid_from runs on the pipeline clock (source commit time), never the business date.
-- The first version of each key starts at 1900-01-01 so facts committed before the
-- snapshot timestamp still find a version (docs/data_model.md, "SCD2 first version").
{% macro cdc_scd2_history(table_name, pk) -%}
{%- set order_asc -%}
  try_to_number(source_lsn) asc nulls first, event_ts_ms asc nulls first,
  debezium_ts_ms asc nulls first, topic, kafka_partition, kafka_offset
{%- endset -%}
with events as (
    select
        coalesce(try_to_number(record_pk), try_to_number(after_json:{{ pk }}::varchar),
                 try_to_number(before_json:{{ pk }}::varchar),
                 try_to_number(key_json:{{ pk }}::varchar)) as {{ pk }},
        coalesce(after_json, before_json) as state_json,
        op, event_id, topic, kafka_partition, kafka_offset, source_lsn,
        event_ts_ms, debezium_ts_ms, kafka_timestamp_ms, raw_loaded_at
    from {{ ref('stg_cdc_events') }}
    where source_table = '{{ table_name }}' and op in ('r','c','u','d')
), ordered as (
    select *,
        lag(to_json(state_json)) over (partition by {{ pk }} order by {{ order_asc }}) as prev_state,
        lag(op = 'd') over (partition by {{ pk }} order by {{ order_asc }}) as prev_deleted,
        row_number() over (partition by {{ pk }} order by {{ order_asc }}) as event_seq
    from events
    where {{ pk }} is not null
), changed as (
    -- event_seq microseconds keep versions committed in the same millisecond strictly ordered.
    select *,
        dateadd('microsecond', event_seq - 1,
                coalesce(to_timestamp_ntz(event_ts_ms, 3),
                         convert_timezone('UTC', raw_loaded_at)::timestamp_ntz)) as version_at
    from ordered
    where prev_state is null
       or prev_deleted is distinct from (op = 'd')
       or prev_state is distinct from to_json(state_json)
), versions as (
    select *,
        row_number() over (partition by {{ pk }} order by event_seq) as version_number,
        lead(version_at) over (partition by {{ pk }} order by event_seq) as valid_to
    from changed
)
select
    {{ pk }},
    state_json,
    op = 'd' as is_deleted,
    event_id,
    version_number,
    case when version_number = 1 then '1900-01-01'::timestamp_ntz else version_at end as valid_from,
    valid_to,
    valid_to is null as is_current,
    {{ hash_key(pk ~ ", event_id") }} as version_key,
    source_lsn, event_ts_ms, kafka_partition, kafka_offset
from versions
{%- endmacro %}

-- Source writes 'czech_original' or 'faker_synthetic' (postgres/schema.sql CHECK constraints).
-- Anything else is surfaced as 'unknown' and fails assert_origin_known rather than being absorbed.
{% macro origin_code(column) -%}
  (case when {{ column }} in ('czech_original', 'faker_synthetic') then {{ column }} else 'unknown' end)
{%- endmacro %}
-- Deterministic surrogate key. Snowflake HASH is signed 64-bit, so ABS fits NUMBER(38,0).
-- The fakesnow emulator (target `ci`) maps HASH to DuckDB's unsigned 64-bit hash, which
-- overflows that cast; the modulo keeps CI keys in range without changing production SQL.
{% macro hash_key(args) -%}
  {%- if target.name == 'ci' -%}
  (hash({{ args }}) % 9223372036854775807)::number(38,0)
  {%- else -%}
  abs(hash({{ args }}))::number(38,0)
  {%- endif -%}
{%- endmacro %}

-- yyyymmdd integer key, computed arithmetically (no string formatting round trip).
{% macro date_key(column) -%}
  (year({{ column }}) * 10000 + month({{ column }}) * 100 + day({{ column }}))
{%- endmacro %}
