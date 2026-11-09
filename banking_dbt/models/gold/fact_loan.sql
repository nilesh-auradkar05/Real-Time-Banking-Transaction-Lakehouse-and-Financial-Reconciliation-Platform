{{ config(materialized='table') }}
-- Grain: one loan contract. Points at the CURRENT dim_loan version (docs/data_model.md).
with owners as (
  select account_id, client_id from {{ ref('fin_disp') }} where disp_type = 'O' and not is_deleted
  qualify row_number() over (partition by account_id order by disp_id) = 1
), repayments as (
  select source_account_id as account_id, count(*) as payments_made,
         sum(abs(amount))::number(18,2) as amount_repaid
  from {{ ref('fact_transaction') }}
  where not is_deleted and category_code = 'LO' and amount < 0
  group by 1
), as_of as (
  -- Replayed history ends in 2018, so "months since grant" is measured to the data's as-of date.
  select max(trans_date) as as_of_date from {{ ref('fin_trans') }} where not is_deleted
), loan as (
  select l.*,
    coalesce(to_timestamp_ntz(l.source_first_committed_ts_ms, 3),
             to_timestamp_ntz(l.source_event_ts_ms, 3),
             convert_timezone('UTC', l.warehouse_loaded_at)::timestamp_ntz) as cdc_at
  from {{ ref('fin_loan') }} l
), account_version as (
  select l.loan_id, a.account_key, a.branch_district_id
  from loan l
  join {{ ref('dim_account') }} a
    on a.account_id = l.account_id and not a.is_deleted
   and l.cdc_at >= a.valid_from and (l.cdc_at < a.valid_to or a.valid_to is null)
  qualify row_number() over (partition by l.loan_id order by a.valid_from desc) = 1
), client_version as (
  select l.loan_id, c.client_key, c.home_district_id
  from loan l
  join owners o on o.account_id = l.account_id
  join {{ ref('dim_client') }} c
    on c.client_id = o.client_id and not c.is_deleted
   and l.cdc_at >= c.valid_from and (l.cdc_at < c.valid_to or c.valid_to is null)
  qualify row_number() over (partition by l.loan_id order by c.valid_from desc) = 1
)
select dl.loan_key,
       {{ date_key('l.granted_date') }} as granted_date_key,
       av.account_key, cv.client_key as owner_client_key,
       br.district_key as branch_district_key, home.district_key as client_district_key,
       origin.data_origin_key,
       l.amount::number(18,2) as loan_amount, l.duration as duration_months,
       l.payments::number(18,2) as monthly_payment,
       coalesce(r.payments_made, 0) as payments_made,
            -- Approximated from whole months between grant and the as-of date, capped at the term.
       least(l.duration, greatest(datediff('month', l.granted_date, ao.as_of_date), 0)) as payments_expected,
       coalesce(r.amount_repaid, 0)::number(18,2) as amount_repaid,
       greatest(l.amount - coalesce(r.amount_repaid, 0), 0)::number(18,2) as amount_outstanding,
       l.loan_id as source_loan_id, l.is_deleted
from loan l
left join {{ ref('dim_loan') }} dl
  on dl.loan_id = l.loan_id and dl.is_current and not dl.is_deleted
left join account_version av on av.loan_id = l.loan_id
left join client_version cv on cv.loan_id = l.loan_id
left join {{ ref('dim_district') }} br on br.district_id = av.branch_district_id
left join {{ ref('dim_district') }} home on home.district_id = cv.home_district_id
left join {{ ref('dim_data_origin') }} origin on origin.origin_code = {{ origin_code('l.data_origin') }}
left join repayments r on r.account_id = l.account_id
cross join as_of ao
