{{ config(materialized='table') }}
select distinct
  abs(hash(coalesce(trans_type,'UNKNOWN'),coalesce(operation,'UNKNOWN'),
           coalesce(category,'UNKNOWN')))::number(38,0) as transaction_type_key,
  coalesce(trans_type,'UNKNOWN') as type_code,
  case when trans_type in ('PRIJEM','C') then 'CREDIT'
       when trans_type in ('VYDAJ','VYBER','D','P') then 'DEBIT' else 'UNKNOWN' end as direction,
  coalesce(trans_type,'UNKNOWN') as type_name,
  coalesce(operation,'UNKNOWN') as operation_code,
  coalesce(operation,'UNKNOWN') as operation_name,
  coalesce(category,'UNKNOWN') as category_code,
  coalesce(category,'UNKNOWN') as category_name
from {{ ref('fin_trans') }}