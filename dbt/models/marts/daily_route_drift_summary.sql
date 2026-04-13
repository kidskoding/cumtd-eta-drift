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
)

select
    service_date,
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
from trip_metrics
group by service_date, route_id, route_short_name
