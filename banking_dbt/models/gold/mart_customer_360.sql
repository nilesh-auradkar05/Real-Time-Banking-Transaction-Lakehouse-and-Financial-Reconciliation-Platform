{{ config(materialized='table') }}
with current_clients as (
 select * from {{ ref('dim_client') }} where is_current and not is_deleted
), tx as (
 select dc.client_id, count(*) as transaction_count,
  sum(t.amount)::number(18,2) as lifetime_net_cash_movement,
  max(t.business_date) as last_transaction_date
 from {{ ref('fact_transaction') }} t
 join {{ ref('dim_client') }} dc on dc.client_key=t.owner_client_key
 where not t.is_deleted group by dc.client_id
), owned as (
 select d.client_key,count(distinct d.account_key) as owned_account_count
 from {{ ref('bridge_account_client') }} d where disposition_type='OWNER' group by 1
), loan as (
 select dc.client_id,count(*) as loan_count,
        sum(l.loan_amount)::number(18,2) as original_loan_exposure
 from {{ ref('fact_loan') }} l
 join {{ ref('dim_client') }} dc on dc.client_key=l.owner_client_key
 where not l.is_deleted group by 1
)
select c.client_key,c.client_id,c.home_district_id,c.home_region,c.gender,
 coalesce(o.owned_account_count,0) as owned_account_count,
 coalesce(t.transaction_count,0) as transaction_count,
 coalesce(t.lifetime_net_cash_movement,0)::number(18,2) as lifetime_net_cash_movement,
 t.last_transaction_date,coalesce(l.loan_count,0) as loan_count,
 coalesce(l.original_loan_exposure,0)::number(18,2) as original_loan_exposure
from current_clients c
left join owned o on o.client_key=c.client_key
left join tx t on t.client_id=c.client_id
left join loan l on l.client_id=c.client_id