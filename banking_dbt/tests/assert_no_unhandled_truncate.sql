-- Truncation must not be silently treated as a no-op in current-state SILVER.
select event_id, source_table, op
from {{ ref('stg_cdc_events') }}
where source_table in ('fin_account','fin_card','fin_client','fin_disp',
 'fin_district','fin_loan','fin_order','fin_trans')
  and op in ('t','m')
