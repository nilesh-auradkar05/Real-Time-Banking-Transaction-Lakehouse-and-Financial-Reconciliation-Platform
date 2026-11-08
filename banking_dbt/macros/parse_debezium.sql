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