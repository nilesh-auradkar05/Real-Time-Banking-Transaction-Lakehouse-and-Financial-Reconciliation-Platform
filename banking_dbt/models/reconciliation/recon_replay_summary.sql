-- One row per replay run (plus a NULL-run row when unexpected changes exist).
-- delivered_pct is judged only over changes old enough to judge: pending ones are excluded.
select
    replay_run_id,
    max(run_status) as run_status,
    count_if(delivery_status <> 'UNEXPECTED_CHANGE') as planned_changes,
    count_if(actual_commit_at is not null) as committed_changes,
    count_if(delivery_status = 'DELIVERED') as delivered,
    count_if(delivery_status = 'DUPLICATE_DELIVERY') as duplicate_delivered,
    -- missing = unacknowledged losses (these fail the build); acknowledged ones are kept apart.
    count_if(delivery_status = 'MISSING' and not is_acknowledged) as missing,
    count_if(delivery_status = 'MISSING' and is_acknowledged) as missing_acknowledged,
    count_if(delivery_status = 'RECOVERED_BY_SNAPSHOT') as recovered_by_snapshot,
    count_if(delivery_status = 'PENDING') as pending,
    count_if(delivery_status = 'NOT_COMMITTED') as not_committed,
    count_if(delivery_status = 'UNEXPECTED_CHANGE') as unexpected_changes,
    count_if(injected_fault = 'late_arrival') as late_injected,
    count_if(injected_fault = 'late_arrival'
             and delivery_status in ('DELIVERED', 'DUPLICATE_DELIVERY')) as late_delivered,
    -- Over changes old enough to judge. delivered_pct: the change EVENT arrived.
    -- state_converged_pct: the change's state is in the warehouse (event or snapshot).
    round(100.0 * count_if(delivery_status in ('DELIVERED', 'DUPLICATE_DELIVERY'))
          / nullif(count_if(delivery_status in ('DELIVERED', 'DUPLICATE_DELIVERY',
                                                'RECOVERED_BY_SNAPSHOT', 'MISSING')), 0), 2)
        as delivered_pct,
    round(100.0 * count_if(delivery_status in ('DELIVERED', 'DUPLICATE_DELIVERY', 'RECOVERED_BY_SNAPSHOT'))
          / nullif(count_if(delivery_status in ('DELIVERED', 'DUPLICATE_DELIVERY',
                                                'RECOVERED_BY_SNAPSHOT', 'MISSING')), 0), 2)
        as state_converged_pct,
    percentile_cont(0.50) within group (order by end_to_end_seconds) as p50_end_to_end_seconds,
    percentile_cont(0.95) within group (order by end_to_end_seconds) as p95_end_to_end_seconds,
    percentile_cont(0.95) within group (order by capture_seconds) as p95_capture_seconds,
    percentile_cont(0.95) within group (order by landing_seconds) as p95_landing_seconds,
    percentile_cont(0.95) within group (order by load_seconds) as p95_load_seconds
from {{ ref('recon_replay_delivery') }}
group by replay_run_id