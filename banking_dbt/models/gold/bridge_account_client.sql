{{ config(materialized='table') }}
select d.disp_id,a.account_key,c.client_key,
       case d.disp_type when 'O' then 'OWNER' when 'D' then 'DISPONENT'
            else d.disp_type end as disposition_type
from {{ ref('fin_disp') }} d
left join {{ ref('dim_account') }} a on a.account_id=d.account_id and a.is_current and not a.is_deleted
left join {{ ref('dim_client') }} c on c.client_id=d.client_id and c.is_current and not c.is_deleted
where not d.is_deleted