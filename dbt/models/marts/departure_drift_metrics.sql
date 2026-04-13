{{ config(
    materialized='table',
    file_format='delta'
) }}

with updates as (
    select *
    from {{ ref('int_departure_eta_updates') }}
),

metrics as (
    select
        sha2(
            concat_ws(
                '||',
                cast(min(ingestion_date) as string),
                coalesce(stop_id, ''),
                coalesce(trip_id, ''),
                coalesce(route_id, ''),
                coalesce(route_short_name, '')
            ),
            256
        ) as metric_key,
        min(ingestion_date) as first_ingestion_date,
        max(ingestion_date) as last_ingestion_date,
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
        trip_id,
        route_id,
        route_short_name,
        first(first_scheduled_departure, true) as scheduled_departure,
        min(ingestion_timestamp) as first_ingestion_timestamp,
        max(ingestion_timestamp) as last_ingestion_timestamp,
        first(first_estimated_departure, true) as first_estimated_departure,
        first(last_estimated_departure, true) as last_estimated_departure,
        min(estimated_departure) as min_estimated_departure,
        max(estimated_departure) as max_estimated_departure,
        avg(estimate_offset_minutes) as avg_estimate_offset_minutes,
        min(estimate_offset_minutes) as min_estimate_offset_minutes,
        max(estimate_offset_minutes) as max_estimate_offset_minutes,
        (
            unix_timestamp(max(estimated_departure))
            - unix_timestamp(min(estimated_departure))
        ) / 60.0 as drift_minutes,
        (
            unix_timestamp(first(last_estimated_departure, true))
            - unix_timestamp(first(first_estimated_departure, true))
        ) / 60.0 as net_drift_minutes,
        count(*) as update_count,
        sum(monotonicity_violation) as monotonicity_violation_count,
        max(forward_jump_minutes) as max_forward_jump_minutes,
        max(backward_jump_minutes) as max_backward_jump_minutes,
        sum(abs(coalesce(eta_delta_minutes, 0.0))) as absolute_eta_movement_minutes,
        stddev_samp(estimate_offset_minutes) as prediction_volatility_minutes,
        avg(time_to_departure_minutes) as avg_time_to_departure_minutes,
        min(time_to_departure_minutes) as min_time_to_departure_minutes,
        max(time_to_departure_minutes) as max_time_to_departure_minutes,
        current_timestamp() as metric_computed_at
    from updates
    group by stop_id, trip_id, route_id, route_short_name
    having count(*) >= 2
)

select *
from metrics
