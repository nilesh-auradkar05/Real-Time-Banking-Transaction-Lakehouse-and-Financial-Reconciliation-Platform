select account_key,business_date,count(*) as duplicates
from {{ ref('fact_account_balance_daily') }}
group by 1,2 having count(*) > 1
