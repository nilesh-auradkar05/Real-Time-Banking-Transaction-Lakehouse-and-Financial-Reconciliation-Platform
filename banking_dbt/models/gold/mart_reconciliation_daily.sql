{{ config(materialized='table') }}
select business_date, count(*) as account_days,
  count_if(reconciliation_status='RECONCILED') as reconciled_account_days,
  count_if(reconciliation_status='VARIANCE') as variance_account_days,
  count_if(reconciliation_status='AMBIGUOUS_CLOSING') as ambiguous_account_days,
  count_if(reconciliation_status='NO_COMPARABLE_PREVIOUS_DAY') as no_baseline_account_days,
  sum(case when reconciliation_status='VARIANCE' then abs(balance_variance) else 0 end)::number(18,2)
    as total_absolute_variance,
  sum(net_amount)::number(18,2) as net_cash_movement
from {{ ref('fact_account_balance_daily') }}
group by 1