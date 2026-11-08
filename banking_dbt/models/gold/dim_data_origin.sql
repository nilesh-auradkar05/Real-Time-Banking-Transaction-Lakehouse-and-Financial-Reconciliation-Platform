{{ config(materialized='table') }}
select 1 as data_origin_key,'czech' as origin_code,'Czech source' as origin_name,false as is_synthetic
union all select 2,'faker','Faker synthetic',true
union all select 3,'unknown','Unknown / missing',false