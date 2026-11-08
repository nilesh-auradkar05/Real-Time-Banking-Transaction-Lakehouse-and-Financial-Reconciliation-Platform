{{ config(materialized='table') }}
select h.version_key as loan_key,h.loan_id,
       try_to_number(h.state_json:account_id::varchar) as account_id,
       h.state_json:status::varchar as status_code,
       case h.state_json:status::varchar when 'A' then 'FINISHED_OK'
          when 'B' then 'FINISHED_PROBLEM' when 'C' then 'RUNNING_OK'
          when 'D' then 'RUNNING_PROBLEM' else 'UNKNOWN' end as status_name,
       h.state_json:status::varchar in ('A','B') as is_finished,
       h.state_json:status::varchar in ('B','D') as is_problem,
       h.valid_from,h.valid_to,h.is_current,h.is_deleted,
       h.event_id as version_event_id
from {{ ref('fin_loan_history') }} h