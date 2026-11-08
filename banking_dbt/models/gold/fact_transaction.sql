{{ config(materialized='table') }}
with owners as (
 select account_id, client_id from {{ ref('fin_disp') }}
 where not is_deleted and disp_type='O'
 qualify row_number() over (partition by account_id order by disp_id)=1
), unique_orders as (
 select * from {{ ref('dim_standing_order') }}
 qualify count(*) over(partition by account_id,partner_bank_code,partner_account_id,category_code)=1
), unique_loans as (
 select * from {{ ref('dim_loan') }} where is_current and not is_deleted
 qualify count(*) over(partition by account_id)=1
), account_versions as (
 select *, row_number() over(partition by account_id order by valid_from,account_key)=1 as first_version
 from {{ ref('dim_account') }}
), client_versions as (
 select *, row_number() over(partition by client_id order by valid_from,client_key)=1 as first_version
 from {{ ref('dim_client') }}
),tx as (
 select *,coalesce(try_to_timestamp_ntz(source_event_ts_ms,3),
                    warehouse_loaded_at::timestamp_ntz) as cdc_at
 from {{ ref('fin_trans') }}
)
select t.trans_id as transaction_id,to_number(to_char(t.trans_date,'YYYYMMDD')) as date_key,
       da.account_key,dc.client_key as owner_client_key,
       br.district_key as branch_district_key,home.district_key as client_district_key,
       tt.transaction_type_key,orders.order_key,lo.loan_key,null::number(38,0) as card_key,
       ori.data_origin_key,t.other_bank_id as partner_bank_code,
       t.other_account_id as partner_account_id,
       t.amount::number(18,2) as amount,t.balance::number(18,2) as balance_after,
       t.source_event_ts_ms as source_committed_at_ms,
       t.recorded_at as recorded_at,t.warehouse_loaded_at as landed_at,
       datediff('second',t.recorded_at,t.warehouse_loaded_at::timestamp_ntz) as ingestion_lag_seconds,
       t.is_deleted,t.last_event_id,t.account_id as source_account_id,
       t.trans_date as business_date,t.data_origin,
       t.trans_type,coalesce(t.operation,'UNKNOWN') as operation_code,
       coalesce(t.category,'UNKNOWN') as category_code
from tx t
left join owners o on o.account_id=t.account_id
left join account_versions da on da.account_id=t.account_id and not da.is_deleted
    and (t.cdc_at >= da.valid_from or da.first_version)
    and (t.cdc_at < da.valid_to or da.valid_to is null)
left join client_versions dc on dc.client_id=o.client_id and not dc.is_deleted
    and (t.cdc_at >= dc.valid_from or dc.first_version)
    and (t.cdc_at < dc.valid_to or dc.valid_to is null)
left join {{ ref('dim_district') }} br on br.district_id=da.branch_district_id
left join {{ ref('dim_district') }} home on home.district_id=dc.home_district_id
left join {{ ref('dim_transaction_type') }} tt on tt.type_code=coalesce(t.trans_type,'UNKNOWN')
    and tt.operation_code=coalesce(t.operation,'UNKNOWN') and tt.category_code=coalesce(t.category,'UNKNOWN')
left join unique_orders orders on orders.account_id=t.account_id
    and orders.partner_bank_code=t.other_bank_id and orders.partner_account_id=t.other_account_id
    and orders.category_code=coalesce(t.category,'UNKNOWN')
left join unique_loans lo on lo.account_id=t.account_id
    and t.category='LO'
left join {{ ref('dim_data_origin') }} ori on ori.origin_code=(case
 when lower(coalesce(t.data_origin,'')) in ('czech','original','source','pkdd99') then 'czech'
 when lower(coalesce(t.data_origin,'')) in ('faker','synthetic') then 'faker'
 else 'unknown' end)
qualify row_number() over (partition by t.trans_id order by
  da.valid_from desc nulls last,dc.valid_from desc nulls last,
  orders.order_key,lo.loan_key) = 1