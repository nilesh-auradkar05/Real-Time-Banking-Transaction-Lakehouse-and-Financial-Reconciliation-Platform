with ranked as (
    select
        *, row_number() over (
            partition by event_id
            order by raw_loaded_at desc, src_filename desc, src_file_row_number desc
        ) as _event_row_num
    from {{ source('raw','cdc_events')}}
    where event_id is not null
)

select * exclude (_event_row_num)
from ranked where _event_row_num = 1