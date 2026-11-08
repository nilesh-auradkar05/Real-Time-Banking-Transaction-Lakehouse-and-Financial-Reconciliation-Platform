{{ config(materialized='table') }}
with date_bounds as (
  select coalesce(min(trans_date), '1990-01-01'::date) as min_date,
         greatest(coalesce(max(trans_date), '1999-12-31'::date),current_date()) as max_date
  from {{ ref('fin_trans') }}
), dates as (
 select dateadd(day, seq.n, b.min_date)::date as full_date
 from date_bounds b cross join
   (select row_number() over(order by seq4())-1 as n from table(generator(rowcount=>20000))) seq
 where dateadd(day,seq.n,b.min_date)<=b.max_date
)
select to_number(to_char(full_date,'YYYYMMDD')) as date_key,full_date,
  year(full_date) as year, quarter(full_date) as quarter,
  month(full_date) as month,monthname(full_date) as month_name,
  dayofweekiso(full_date) as day_of_week,
  dayofweekiso(full_date) in (6,7) as is_weekend,
  full_date = last_day(full_date) as is_month_end
from dates