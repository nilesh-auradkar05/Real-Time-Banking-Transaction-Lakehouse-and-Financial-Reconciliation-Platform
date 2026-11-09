{{ config(materialized='table') }}
select district_id as district_key, district_id, district_name, region, num_inhabitants,
       num_municipalities_lt500, num_municipalities_500to1999,
       num_municipalities_2000to9999, num_municipalities_gt10000 as num_municipalities_gte10000,
       num_cities, ratio_urban, average_salary, unemployment_rate95, unemployment_rate96,
       num_entrep_per1000, num_crimes95, num_crimes96
from {{ ref('fin_district') }} where not is_deleted
