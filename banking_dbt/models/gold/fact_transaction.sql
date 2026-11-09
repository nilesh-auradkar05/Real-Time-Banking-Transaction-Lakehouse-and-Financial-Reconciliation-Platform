{{ config(materialized='table') }}
-- Grain: one posted transaction (current CDC state, deletes kept with is_deleted).
-- Dimension versions are chosen by source commit time (pipeline clock), never by business date.
-- Conditional keys: -1 = not applicable, -2 = expected but no match (docs/data_model.md).
with tx as (
    select *,
        coalesce(to_timestamp_ntz(source_event_ts_ms, 3),
                 convert_timezone('UTC', warehouse_loaded_at)::timestamp_ntz) as cdc_at
    from {{ ref('fin_trans') }}
), owners as (
    select account_id, client_id from {{ ref('fin_disp') }}
    where not is_deleted and disp_type = 'O'
    qualify row_number() over (partition by account_id order by disp_id) = 1
), account_version as (
    select t.trans_id, a.account_key, a.branch_district_id
    from tx t
    join {{ ref('dim_account') }} a
      on a.account_id = t.account_id and not a.is_deleted
     and t.cdc_at >= a.valid_from and (t.cdc_at < a.valid_to or a.valid_to is null)
    qualify row_number() over (partition by t.trans_id order by a.valid_from desc) = 1
), client_version as (
    select t.trans_id, c.client_key, c.home_district_id
    from tx t
    join owners o on o.account_id = t.account_id
    join {{ ref('dim_client') }} c
      on c.client_id = o.client_id and not c.is_deleted
     and t.cdc_at >= c.valid_from and (t.cdc_at < c.valid_to or c.valid_to is null)
    qualify row_number() over (partition by t.trans_id order by c.valid_from desc) = 1
), order_match as (
    -- ROB: match account + partner bank + partner account; prefer the order with the same
    -- absolute amount when an account pays the same partner under several orders.
    select t.trans_id, o.order_key
    from tx t
    join {{ ref('dim_standing_order') }} o
      on o.order_key > 0 and o.account_id = t.account_id
     and o.partner_bank_code = t.other_bank_id and o.partner_account_id = t.other_account_id
    where t.operation = 'ROB'
    qualify row_number() over (partition by t.trans_id
        order by iff(abs(o.order_amount - abs(t.amount)) <= 0.05, 0, 1), o.order_key) = 1
), loan_match as (
    -- LO: the loan version in force when the repayment was committed (one loan per account).
    select t.trans_id, l.loan_key
    from tx t
    join {{ ref('dim_loan') }} l
      on l.loan_key > 0 and l.account_id = t.account_id and not l.is_deleted
     and t.cdc_at >= l.valid_from and (t.cdc_at < l.valid_to or l.valid_to is null)
    where t.category = 'LO'
    qualify row_number() over (partition by t.trans_id order by l.valid_from desc) = 1
), card_match as (
    select t.trans_id, c.card_key
    from tx t
    join {{ ref('dim_card') }} c on c.card_key > 0 and c.account_id = t.account_id
    where t.operation = 'CCW'
    qualify row_number() over (partition by t.trans_id order by c.card_key) = 1
)
select
    t.trans_id as transaction_id,
    {{ date_key('t.trans_date') }} as date_key,
    av.account_key,
    cv.client_key as owner_client_key,
    br.district_key as branch_district_key,
    home.district_key as client_district_key,
    tt.transaction_type_key,
    case when t.operation = 'ROB' then coalesce(om.order_key, -2) else -1 end as order_key,
    case when t.category = 'LO' then coalesce(lm.loan_key, -2) else -1 end as loan_key,
    case when t.operation = 'CCW' then coalesce(cm.card_key, -2) else -1 end as card_key,
    ori.data_origin_key,
    t.other_bank_id as partner_bank_code,
    t.other_account_id as partner_account_id,
    t.amount::number(18,2) as amount,
    t.balance::number(18,2) as balance_after,
    to_timestamp_ntz(t.source_event_ts_ms, 3) as source_committed_at,
    convert_timezone('UTC', t.warehouse_loaded_at)::timestamp_ntz as warehouse_loaded_at,
    datediff('second', to_timestamp_ntz(t.source_event_ts_ms, 3),
             convert_timezone('UTC', t.warehouse_loaded_at)::timestamp_ntz) as ingestion_lag_seconds,
    coalesce(datediff('second', to_timestamp_ntz(t.source_event_ts_ms, 3),
             convert_timezone('UTC', t.warehouse_loaded_at)::timestamp_ntz)
             > {{ var('late_arrival_threshold_seconds', 1800) }}, false) as is_late_arriving,
    t.is_deleted,
    -- Lineage and slicing columns used by downstream marts.
    t.last_event_id,
    t.account_id as source_account_id,
    t.trans_date as business_date,
    {{ origin_code('t.data_origin') }} as data_origin,
    t.trans_type,
    coalesce(t.operation, 'UNKNOWN') as operation_code,
    coalesce(t.category, 'UNKNOWN') as category_code,
    t.recorded_at
from tx t
left join account_version av on av.trans_id = t.trans_id
left join client_version cv on cv.trans_id = t.trans_id
left join {{ ref('dim_district') }} br on br.district_id = av.branch_district_id
left join {{ ref('dim_district') }} home on home.district_id = cv.home_district_id
left join {{ ref('dim_transaction_type') }} tt
  on tt.type_code = coalesce(t.trans_type, 'UNKNOWN')
 and tt.operation_code = coalesce(t.operation, 'UNKNOWN')
 and tt.category_code = coalesce(t.category, 'UNKNOWN')
left join order_match om on om.trans_id = t.trans_id
left join loan_match lm on lm.trans_id = t.trans_id
left join card_match cm on cm.trans_id = t.trans_id
left join {{ ref('dim_data_origin') }} ori on ori.origin_code = {{ origin_code('t.data_origin') }}
