{{ config(materialized='table') }}
-- -1 = not applicable (not a CCW withdrawal), -2 = CCW withdrawal with no card on the account.
select c.card_id as card_key, c.card_id, c.disp_id, d.account_id, c.card_type,
       coalesce(c.card_type, 'UNKNOWN') as card_type_name, c.issued_date
from {{ ref('fin_card') }} c
left join {{ ref('fin_disp') }} d on d.disp_id = c.disp_id
where not c.is_deleted
union all
select -1, null, null, null, 'N/A', 'Not applicable', null
union all
select -2, null, null, null, 'UNMATCHED', 'Expected but no match', null
