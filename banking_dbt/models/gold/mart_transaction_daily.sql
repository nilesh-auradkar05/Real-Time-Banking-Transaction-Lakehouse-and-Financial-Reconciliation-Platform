{{ config(materialized='table') }}
select business_date, data_origin,
  count(*) as transaction_count,
  count(distinct account_key) as active_accounts,
  sum(case when amount > 0 then amount else 0 end)::number(18,2) as credit_amount,
  sum(case when amount < 0 then amount else 0 end)::number(18,2) as debit_amount,
  sum(amount)::number(18,2) as net_amount
from {{ ref('fact_transaction') }} where not is_deleted
group by 1,2