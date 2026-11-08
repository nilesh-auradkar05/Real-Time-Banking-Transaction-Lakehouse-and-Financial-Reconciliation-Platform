{{ config(materialized='table') }}
select h.version_key as client_key,h.client_id,
       h.state_json:gender::varchar as gender,
       {{ cdc_date('h.state_json:birth_date') }} as birth_date,
       try_to_number(h.state_json:district_id::varchar) as home_district_id,
       d.district_name as home_district_name,d.region as home_region,
       h.valid_from,h.valid_to,h.is_current,h.is_deleted,
       h.event_id as version_event_id
from {{ ref('fin_client_history') }} h
left join {{ ref('fin_district') }} d
  on d.district_id=try_to_number(h.state_json:district_id::varchar) and not d.is_deleted