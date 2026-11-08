-- CDC r/c/u must have an after-image; d must have a before-image.
-- Stop the build rather than silently allowing malformed records through.
select event_id, source_table, op
from {{ ref('stg_cdc_events') }}
where source_table in ('fin_account','fin_card','fin_client','fin_disp','fin_district','fin_loan','fin_order','fin_trans')
  and ((op in ('r','c','u') and after_json is null)
       or (op = 'd' and before_json is null))
