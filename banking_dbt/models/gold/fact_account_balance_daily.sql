-- Czech transaction IDs are NOT chronological. Same-day rows lack a time-of-day.
-- A candidate closing row has no observed successor whose opening balance equals its balance.
-- Mark unresolved multi-row days ambiguous rather than selecting MAX(trans_id) or MAX(balance).
with tx as (
  select transaction_id, account_key, owner_client_key,branch_district_key,client_district_key,
         data_origin_key, business_date, amount, balance_after, data_origin
  from {{ ref('fact_transaction') }} where not is_deleted
), day_activity as (
  select account_key, business_date, count(*) as transaction_count,
    sum(case when amount >= 0 then amount else 0 end)::number(18,2) as credit_amount,
    sum(case when amount < 0 then amount else 0 end)::number(18,2) as debit_amount,
    sum(amount)::number(18,2) as net_amount,
    count(distinct data_origin) as origin_count,
    min(owner_client_key) as owner_client_key,min(branch_district_key) as branch_district_key,
    min(client_district_key) as client_district_key,
    min(data_origin_key) as data_origin_key
  from tx group by 1,2
), closing_candidates as (
  select x.account_key, x.business_date, x.transaction_id, x.balance_after
  from tx x
  where not exists (
    select 1 from tx successor
    where successor.account_key=x.account_key
      and successor.business_date=x.business_date
      and successor.transaction_id<>x.transaction_id
      and abs((successor.balance_after - successor.amount) - x.balance_after) <= 0.02
  )
), candidates as (
  select account_key,business_date,count(*) as candidate_count,
    max(balance_after)::number(18,2) as candidate_closing_balance
  from closing_candidates group by 1,2
), daily as (
  select d.*, coalesce(c.candidate_count,0) as candidate_count,
    case when c.candidate_count=1 then c.candidate_closing_balance end as closing_balance
  from day_activity d left join candidates c
    on c.account_key=d.account_key and c.business_date=d.business_date
), lagged as (
  select *,lag(closing_balance) over (partition by account_key order by business_date)
    as previous_closing_balance,
    row_number() over (partition by account_key order by business_date) as day_number
  from daily
), finished as (
  select account_key, business_date,owner_client_key,branch_district_key,client_district_key,data_origin_key,
    to_number(to_char(business_date,'YYYYMMDD')) as date_key,
    transaction_count, credit_amount, debit_amount, net_amount,
    origin_count, candidate_count, closing_balance,
    case when day_number=1 and closing_balance is not null then closing_balance-net_amount
      else previous_closing_balance end::number(18,2) as opening_balance,
    case when day_number>1 and previous_closing_balance is not null and closing_balance is not null
      then (closing_balance - (previous_closing_balance+net_amount))::number(18,2) end
      as balance_variance
  from lagged
)
select *,
  case when candidate_count != 1 then 'AMBIGUOUS_CLOSING'
       when balance_variance is null then 'NO_COMPARABLE_PREVIOUS_DAY'
       when abs(balance_variance)<=0.02 then 'RECONCILED'
       else 'VARIANCE' end as reconciliation_status,
  case when balance_variance is not null then abs(balance_variance)<=0.02 end as is_reconciled
from finished