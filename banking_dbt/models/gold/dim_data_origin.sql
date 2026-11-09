{{ config(materialized='table') }}
-- origin_code values match postgres/schema.sql, so facts join on the raw source value.
select 1 as data_origin_key, 'czech_original' as origin_code, 'Czech PKDD''99 source' as origin_name, false as is_synthetic
union all select 2, 'faker_synthetic', 'Faker synthetic', true
union all select 3, 'unknown', 'Unknown / missing', false
