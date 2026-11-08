-- Only applies to synthetic *replay* updates, not initial Faker snapshots.
select event_id from {{ ref('stg_cdc_events') }}
where data_origin='faker' and op in ('u','d')
  and replay_event_id is null and coalesce(after_json:last_replay_event_id,before_json:last_replay_event_id) is null
