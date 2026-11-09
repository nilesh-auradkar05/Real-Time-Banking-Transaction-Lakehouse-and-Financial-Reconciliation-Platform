{{ config(materialized='table') }}
-- -1 = not applicable (not a ROB payment), -2 = ROB payment with no matching order.
select order_id as order_key, order_id, account_id, bank_to as partner_bank_code,
       account_to as partner_account_id,
       coalesce(category, 'UNKNOWN') as category_code,
       coalesce(category, 'UNKNOWN') as category_name,
       amount::number(18,2) as order_amount
from {{ ref('fin_order') }} where not is_deleted
union all
select -1, null, null, null, null, 'N/A', 'Not applicable', null
union all
select -2, null, null, null, null, 'UNMATCHED', 'Expected but no match', null
