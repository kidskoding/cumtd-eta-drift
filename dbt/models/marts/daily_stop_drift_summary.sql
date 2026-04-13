{{ config(
    materialized='table',
    file_format='delta'
) }}

with trip_metrics as (
    select
        first_ingestion_date as service_date,
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
        drift_minutes,
        update_count,
        monotonicity_violation_count
    from {{ ref('departure_drift_metrics') }}
)

select
    service_date,
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
from trip_metrics
group by service_date, stop_id
