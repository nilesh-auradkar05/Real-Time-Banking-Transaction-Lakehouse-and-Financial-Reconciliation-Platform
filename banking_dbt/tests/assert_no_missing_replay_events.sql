{{ config(severity=var('replay_recon_severity')) }}
-- A change committed in PostgreSQL longer ago than the grace window never reached the warehouse,
-- neither as a change event nor through a snapshot, and nobody has reviewed it.
-- Tested on the summary (a leaf) so a failure still leaves both reconciliation tables refreshed.
-- Details: recon_replay_delivery where delivery_status = 'MISSING' and not is_acknowledged.
-- After review, record the loss in AUDIT.RECON_ACKNOWLEDGEMENT (see README).
select replay_run_id, missing
from {{ ref('recon_replay_summary') }}
where missing > 0