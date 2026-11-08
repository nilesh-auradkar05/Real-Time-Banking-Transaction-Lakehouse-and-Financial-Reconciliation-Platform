{{ config(materialized='table') }}
with owners as (
 select account_id,client_id from {{ ref('fin_disp') }} where disp_type='O' and not is_deleted
 qualify row_number() over(partition by account_id order by disp_id)=1
), repayments as (
 select source_account_id as account_id,count(*) as payments_made,
        sum(abs(amount))::number(18,2) as amount_repaid
 from {{ ref('fact_transaction') }} where not is_deleted and category_code='LO' and amount<0
 group by 1
), account_versions as (
 select *, row_number() over(partition by account_id order by valid_from,account_key)=1 as first_version
 from {{ ref('dim_account') }}
), client_versions as (
 select *, row_number() over(partition by client_id order by valid_from,client_key)=1 as first_version
 from {{ ref('dim_client') }}
), loan as (
 select l.*,coalesce(try_to_timestamp_ntz(l.source_event_ts_ms,3),l.warehouse_loaded_at::timestamp_ntz) as cdc_at
 from {{ ref('fin_loan') }} l
)
select dl.loan_key, to_number(to_char(l.granted_date,'YYYYMMDD')) as granted_date_key,
       da.account_key,dc.client_key as owner_client_key,
       br.district_key as branch_district_key,home.district_key as client_district_key,
       origin.data_origin_key,
       l.amount::number(18,2) as loan_amount,l.duration as duration_months,
       l.payments::number(18,2) as monthly_payment,
       coalesce(r.payments_made,0) as payments_made,
       null::number(38,0) as payments_expected,
       coalesce(r.amount_repaid,0)::number(18,2) as amount_repaid,
       null::number(18,2) as amount_outstanding,
       l.loan_id as source_loan_id, l.is_deleted
from loan l
left join {{ ref('dim_loan') }} dl on dl.loan_id=l.loan_id and dl.is_current and not dl.is_deleted
left join account_versions da on da.account_id=l.account_id and not da.is_deleted
    and (l.cdc_at>=da.valid_from or da.first_version)
    and (l.cdc_at<da.valid_to or da.valid_to is null)
left join owners o on o.account_id=l.account_id
left join client_versions dc on dc.client_id=o.client_id and not dc.is_deleted
    and (l.cdc_at>=dc.valid_from or dc.first_version)
    and (l.cdc_at<dc.valid_to or dc.valid_to is null)
left join {{ ref('dim_district') }} br on br.district_id=da.branch_district_id
left join {{ ref('dim_district') }} home on home.district_id=dc.home_district_id
left join {{ ref('dim_data_origin') }} origin on origin.origin_code=(case
 when lower(coalesce(l.data_origin,'')) in ('czech','original','source','pkdd99') then 'czech'
 when lower(coalesce(l.data_origin,'')) in ('faker','synthetic') then 'faker'
 else 'unknown' end)
left join repayments r on r.account_id=l.account_id
qualify row_number() over(partition by l.loan_id order by da.valid_from desc nulls last,dc.valid_from desc nulls last)=1