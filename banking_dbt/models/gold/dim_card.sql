{{ config(materialized='table') }}
select c.card_id as card_key,c.card_id,c.disp_id,c.card_type,
       coalesce(c.card_type,'UNKNOWN') as card_type_name,c.issued_date
from {{ ref('fin_card') }} c where not c.is_deleted