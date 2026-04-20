{{ config(
    materialized='table',
    file_format='delta'
) }}

with route_stop_metrics as (
    select
        route_id,
        route_short_name,
        stop_id,
        max(stop_name) as stop_name,
        max(stop_display_name) as stop_display_name,
        max(stop_group_id) as stop_group_id,
        max(stop_group_name) as stop_group_name,
        max(boarding_point_id) as boarding_point_id,
        max(boarding_point_name) as boarding_point_name,
        max(boarding_point_sub_name) as boarding_point_sub_name,
        count(*) as observed_trip_count,
        avg(drift_minutes) as avg_drift_minutes,
        percentile_approx(drift_minutes, 0.5) as p50_drift_minutes,
        percentile_approx(drift_minutes, 0.9) as p90_drift_minutes,
        avg(update_count) as avg_update_count,
        avg(monotonicity_violation_count) as avg_monotonicity_violation_count
    from {{ ref('departure_drift_metrics') }}
    where route_short_name is not null
      and stop_id is not null
    group by route_id, route_short_name, stop_id
)

select
    route_id,
    route_short_name,
    stop_id,
    stop_name,
    stop_display_name,
    stop_group_id,
    stop_group_name,
    boarding_point_id,
    boarding_point_name,
    boarding_point_sub_name,
    observed_trip_count,
    avg_drift_minutes,
    p50_drift_minutes,
    p90_drift_minutes,
    avg_update_count,
    avg_monotonicity_violation_count,
    greatest(10, least(100, cast(round(100 - (p90_drift_minutes * 9), 0) as int))) as eta_trust_score,
    cast(ceil(p90_drift_minutes) as int) as suggested_buffer_minutes,
    case
        when p90_drift_minutes <= 2 then 'High trust'
        when p90_drift_minutes <= 5 then 'Medium trust'
        else 'Low trust'
    end as eta_trust_label,
    current_timestamp() as score_computed_at
from route_stop_metrics
