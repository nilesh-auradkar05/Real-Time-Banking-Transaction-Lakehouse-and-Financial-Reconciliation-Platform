select event_id,count(*) as duplicates from {{ ref('stg_cdc_events') }}
group by event_id having count(*) > 1
