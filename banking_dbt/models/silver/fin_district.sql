{{ config(materialized='table') }}

with latest as (
{{ latest_cdc('fin_district', 'district_id') }}
)
select
    source_pk::number(38,0) as district_id,
    nullif(trim(row_json:district_name::varchar), '') as district_name,
    nullif(trim(row_json:region::varchar), '') as region,
    try_cast(row_json:num_inhabitants::varchar as number(38,0)) as num_inhabitants,
    try_to_number(row_json:num_municipalities_gt499::varchar, 38, 0) as num_municipalities_lt500,
    try_to_number(row_json:num_municipalities_500to1999::varchar, 38, 0) as num_municipalities_500to1999,
    try_to_number(row_json:num_municipalities_2000to9999::varchar, 38, 0) as num_municipalities_2000to9999,
    try_to_number(row_json:num_municipalities_gt10000::varchar, 38, 0) as num_municipalities_gt10000,
    try_cast(row_json:num_cities::varchar as number(38,0)) as num_cities,
    try_cast(row_json:ratio_urban::varchar as number(18,2)) as ratio_urban,
    try_cast(row_json:average_salary::varchar as number(18,2)) as average_salary,
    try_to_decimal(row_json:unemployment_rate95::varchar, 18, 2) as unemployment_rate95,
    try_to_decimal(row_json:unemployment_rate96::varchar, 18, 2) as unemployment_rate96,
    try_to_number(row_json:num_entrep_per1000::varchar, 38, 0) as num_entrep_per1000,
    try_to_number(row_json:num_crimes95::varchar, 38, 0) as num_crimes95,
    try_to_number(row_json:num_crimes96::varchar, 38, 0) as num_crimes96,
    op = 'd' as is_deleted,
    event_id as last_event_id,
    event_ts_ms as source_event_ts_ms,
    raw_loaded_at as warehouse_loaded_at,
    kafka_offset as last_kafka_offset
from latest