{{ config(
    materialized='table',
    file_format='delta'
) }}

with route_colors as (
    select distinct route_group_name, hex_color, text_hex_color
    from {{ ref('stg_route_colors') }}
),

route_metrics as (
    select
        route_id,
        route_short_name,
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
    where route_short_name is not null
    group by route_id, route_short_name
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
    from route_metrics
),

scored_with_colors as (
    select
        s.*,
        coalesce(rc.route_group_name, s.route_short_name) as route_group_name,
        coalesce(rc.hex_color, '808285') as hex_color,
        coalesce(rc.text_hex_color, 'ffffff') as text_hex_color
    from scored s
    left join route_colors rc
        on upper(s.route_short_name) like '%' || upper(rc.route_group_name) || '%'
    qualify row_number() over (
        partition by s.route_id
        order by length(coalesce(rc.route_group_name, '')) desc
    ) = 1
)

select
    route_id,
    route_short_name,
    route_group_name,
    hex_color,
    text_hex_color,
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
from scored_with_colors
