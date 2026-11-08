{{ config(materialized='table') }}
select h.version_key as account_key,h.account_id,
       h.state_json:frequency::varchar as statement_frequency,
       {{ cdc_date('h.state_json:create_date') }} as create_date,
       try_to_number(h.state_json:district_id::varchar) as branch_district_id,
       d.district_name as branch_district_name,d.region as branch_region,
       h.valid_from,h.valid_to,h.is_current,h.is_deleted,
       h.event_id as version_event_id
from {{ ref('fin_account_history') }} h
left join {{ ref('fin_district') }} d
  on d.district_id=try_to_number(h.state_json:district_id::varchar) and not d.is_deleted