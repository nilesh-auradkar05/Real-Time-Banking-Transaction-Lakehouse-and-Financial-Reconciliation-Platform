{{ config(severity=var('replay_recon_severity')) }}
-- A change reached the warehouse that no replay event explains (or carries an unknown replay id).
-- Tested on the summary (a leaf); details are in recon_replay_delivery
-- where delivery_status = 'UNEXPECTED_CHANGE'.
select replay_run_id, unexpected_changes
from {{ ref('recon_replay_summary') }}
where unexpected_changes > 0
