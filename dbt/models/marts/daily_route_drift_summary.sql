{{ config(
    materialized='table',
    file_format='delta'
) }}

with trip_metrics as (
    select
        first_ingestion_date as service_date,
        route_id,
        route_short_name,
        drift_minutes,
        update_count,
        monotonicity_violation_count
    from {{ ref('departure_drift_metrics') }}

),

route_colors as (
    select route_short_name, route_group_name, hex_color, text_hex_color
    from {{ ref('stg_route_colors') }}
)

select
    tm.service_date,
    tm.route_id,
    tm.route_short_name,
    coalesce(rc.route_group_name, tm.route_short_name) as route_group_name,
    coalesce(rc.hex_color, '808285') as hex_color,
    coalesce(rc.text_hex_color, 'ffffff') as text_hex_color,
    count(*) as observed_trip_count,
    avg(tm.drift_minutes) as avg_drift_minutes,
    percentile_approx(tm.drift_minutes, 0.5) as p50_drift_minutes,
    percentile_approx(tm.drift_minutes, 0.9) as p90_drift_minutes,
    percentile_approx(tm.drift_minutes, 0.95) as p95_drift_minutes,
    avg(tm.update_count) as avg_update_count,
    avg(tm.monotonicity_violation_count) as avg_monotonicity_violation_count,
    avg(case when tm.drift_minutes >= 2 then 1.0 else 0.0 end) as pct_trips_drift_ge_2_min,
    avg(case when tm.drift_minutes >= 5 then 1.0 else 0.0 end) as pct_trips_drift_ge_5_min,
    avg(case when tm.drift_minutes >= 10 then 1.0 else 0.0 end) as pct_trips_drift_ge_10_min
from trip_metrics tm
left join route_colors rc on tm.route_short_name = rc.route_short_name
group by tm.service_date, tm.route_id, tm.route_short_name, rc.route_group_name, rc.hex_color, rc.text_hex_color
