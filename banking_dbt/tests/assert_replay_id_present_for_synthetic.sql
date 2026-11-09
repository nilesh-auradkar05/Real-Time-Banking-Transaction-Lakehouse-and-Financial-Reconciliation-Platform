-- Every simulated UPDATE is planned in replay_event first, so its after-image must carry
-- last_replay_event_id. Deletes are excluded: their before-image holds the previous value
-- and is matched on (table, record key, op) instead (docs/data_models/README.md).
select event_id from {{ ref('stg_cdc_events') }}
where op = 'u'
  and source_table in ('fin_client','fin_account','fin_loan','fin_order')
  and coalesce(replay_event_id, try_to_number(after_json:last_replay_event_id::varchar)) is null
