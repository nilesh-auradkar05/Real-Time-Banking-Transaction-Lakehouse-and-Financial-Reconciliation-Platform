{{ config(severity='warn') }}
-- The change's state reached the warehouse only through a Debezium re-snapshot: current state is
-- right, but the change event (its commit time, SCD2 version and latency) was lost.
select replay_run_id, recovered_by_snapshot
from {{ ref('recon_replay_summary') }}
where recovered_by_snapshot > 0
