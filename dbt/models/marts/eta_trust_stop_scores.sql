{{ config(
    materialized='table',
    file_format='delta'
) }}

with stop_metrics as (
    select
        stop_id,
        max(stop_name) as stop_name,
        max(stop_display_name) as stop_display_name,
        max(stop_group_id) as stop_group_id,
        max(stop_group_name) as stop_group_name,
        max(boarding_point_id) as boarding_point_id,
        max(boarding_point_name) as boarding_point_name,
        max(boarding_point_sub_name) as boarding_point_sub_name,
        max(stop_code) as stop_code,
        max(stop_city) as stop_city,
        max(stop_latitude) as stop_latitude,
        max(stop_longitude) as stop_longitude,
        max(cast(is_station as int)) = 1 as is_station,
        count(*) as observed_trip_count,
        avg(drift_minutes) as avg_drift_minutes,
        percentile_approx(drift_minutes, 0.5) as p50_drift_minutes,
        percentile_approx(drift_minutes, 0.9) as p90_drift_minutes,
        percentile_approx(drift_minutes, 0.95) as p95_drift_minutes,
        avg(update_count) as avg_update_count,
        avg(monotonicity_violation_count) as avg_monotonicity_violation_count,
        avg(case when drift_minutes >= 2 then 1.0 else 0.0 end) as pct_trips_drift_ge_2_min,
        avg(case when drift_minutes >= 5 then 1.0 else 0.0 end) as pct_trips_drift_ge_5_min,
        avg(case when drift_minutes >= 10 then 1.0 else 0.0 end) as pct_trips_drift_ge_10_min
    from {{ ref('departure_drift_metrics') }}
    where stop_id is not null
    group by stop_id
),

scored as (
    select
        *,
        greatest(10, least(100, cast(round(100 - (p90_drift_minutes * 9), 0) as int))) as eta_trust_score,
        cast(ceil(p90_drift_minutes) as int) as suggested_buffer_minutes,
        case
            when p90_drift_minutes <= 2 then 'High trust'
            when p90_drift_minutes <= 5 then 'Medium trust'
            else 'Low trust'
        end as eta_trust_label,
        case
            when p90_drift_minutes <= 2 then 'Safe to trust'
            when p90_drift_minutes <= 5 then 'Keep checking'
            else 'Add buffer'
        end as student_guidance
    from stop_metrics
)

select
    stop_id,
    stop_name,
    stop_display_name,
    stop_group_id,
    stop_group_name,
    boarding_point_id,
    boarding_point_name,
    boarding_point_sub_name,
    stop_code,
    stop_city,
    stop_latitude,
    stop_longitude,
    is_station,
    observed_trip_count,
    avg_drift_minutes,
    p50_drift_minutes,
    p90_drift_minutes,
    p95_drift_minutes,
    avg_update_count,
    avg_monotonicity_violation_count,
    pct_trips_drift_ge_2_min,
    pct_trips_drift_ge_5_min,
    pct_trips_drift_ge_10_min,
    eta_trust_score,
    suggested_buffer_minutes,
    eta_trust_label,
    student_guidance,
    current_timestamp() as score_computed_at
from scored
