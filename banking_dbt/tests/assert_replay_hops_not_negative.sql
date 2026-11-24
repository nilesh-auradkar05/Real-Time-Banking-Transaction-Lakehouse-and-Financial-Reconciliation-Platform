{{ config(severity='warn') }}
-- A hop that finishes before it starts means two clocks disagree (for example, a landing
-- timestamp parsed in the wrong timezone). Tolerates 5 seconds of skew between hosts.
select replay_event_id, capture_seconds, landing_seconds, load_seconds
from {{ ref('recon_replay_delivery') }}
where capture_seconds < -5 or landing_seconds < -5 or load_seconds < -5
