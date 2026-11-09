{{ config(materialized='table') }}
-- Grain: one account per business day with activity.
-- The running balance is computed over the NATURAL account_id: account_key is an SCD2 version
-- key, so partitioning on it would restart the balance chain after every simulated change.
-- Czech transaction IDs are NOT chronological and same-day rows lack a time of day.
-- A closing candidate is a row whose balance is no other same-day row's opening balance
-- (balance_after - amount). Unresolved multi-row days are AMBIGUOUS_CLOSING, never guessed.
with tx as (
  select transaction_id, source_account_id as account_id, account_key, owner_client_key,
         branch_district_key, client_district_key, data_origin_key, data_origin,
         business_date, amount, balance_after, source_committed_at
  from {{ ref('fact_transaction') }}
  where not is_deleted and source_account_id is not null
), day_activity as (
  select account_id, business_date, count(*) as transaction_count,
    sum(case when amount >= 0 then amount else 0 end)::number(18,2) as credit_amount,
    sum(case when amount < 0 then amount else 0 end)::number(18,2) as debit_amount,
    sum(amount)::number(18,2) as net_amount,
    count(distinct data_origin) as origin_count,
    -- Dimension keys of the version in force at the day's last committed change.
    max_by(account_key, source_committed_at) as account_key,
    max_by(owner_client_key, source_committed_at) as owner_client_key,
    max_by(branch_district_key, source_committed_at) as branch_district_key,
    max_by(client_district_key, source_committed_at) as client_district_key,
    min(data_origin_key) as data_origin_key
  from tx group by 1, 2
), closing_candidates as (
  select x.account_id, x.business_date, x.transaction_id, x.balance_after
  -- Anti-join, not NOT EXISTS: Snowflake rejects correlated subqueries with non-equality predicates.
  from tx x
  left join tx successor
    on successor.account_id = x.account_id
   and successor.business_date = x.business_date
   and successor.transaction_id <> x.transaction_id
   and abs((successor.balance_after - successor.amount) - x.balance_after) <= 0.02
  where successor.transaction_id is null
), candidates as (
  select account_id, business_date, count(*) as candidate_count,
    max(balance_after)::number(18,2) as candidate_closing_balance
  from closing_candidates group by 1, 2
), daily as (
  select d.*, coalesce(c.candidate_count, 0) as candidate_count,
    case when c.candidate_count = 1 then c.candidate_closing_balance end as closing_balance
  from day_activity d
  left join candidates c on c.account_id = d.account_id and c.business_date = d.business_date
), lagged as (
  select *,
    lag(closing_balance) over (partition by account_id order by business_date) as previous_closing_balance,
    row_number() over (partition by account_id order by business_date) as day_number
  from daily
), finished as (
  select account_id, account_key, business_date, owner_client_key, branch_district_key,
    client_district_key, data_origin_key,
    {{ date_key('business_date') }} as date_key,
    transaction_count, credit_amount, debit_amount, net_amount,
    origin_count, candidate_count, closing_balance,
    (case when day_number = 1 and closing_balance is not null then closing_balance - net_amount
          else previous_closing_balance end)::number(18,2) as opening_balance,
    (case when day_number > 1 and previous_closing_balance is not null and closing_balance is not null
          then closing_balance - (previous_closing_balance + net_amount) end)::number(18,2)
      as balance_variance
  from lagged
)
select *,
  case when candidate_count != 1 then 'AMBIGUOUS_CLOSING'
       when balance_variance is null then 'NO_COMPARABLE_PREVIOUS_DAY'
       when abs(balance_variance) <= 0.02 then 'RECONCILED'
       else 'VARIANCE' end as reconciliation_status,
  case when balance_variance is not null then abs(balance_variance) <= 0.02 end as is_reconciled
from finished
