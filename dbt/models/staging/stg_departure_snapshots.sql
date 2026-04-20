with source as (
    select *
    from {{ source('cumtd_raw', 'raw_departure_snapshots') }}
),

renamed as (
    select
        sha2(
            concat_ws(
                '||',
                coalesce(run_id, ''),
                cast(ingestion_timestamp as string),
                coalesce(source_stop_id, ''),
                coalesce(stop_id, ''),
                coalesce(trip_id, ''),
                coalesce(cast(estimatedDeparture as string), '')
            ),
            256
        ) as snapshot_key,
        cast(run_id as string) as run_id,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        coalesce(cast(ingestion_date as date), cast(to_date(ingestion_timestamp) as date)) as ingestion_date,
        cast(source_stop_id as string) as source_stop_id,
        cast(stop_id as string) as stop_id,
        cast(stop_name as string) as stop_name,
        cast(stop_display_name as string) as stop_display_name,
        cast(stop_group_id as string) as stop_group_id,
        cast(stop_group_name as string) as stop_group_name,
        cast(boarding_point_id as string) as boarding_point_id,
        cast(boarding_point_name as string) as boarding_point_name,
        cast(boarding_point_sub_name as string) as boarding_point_sub_name,
        cast(stop_code as string) as stop_code,
        cast(stop_city as string) as stop_city,
        cast(stop_latitude as double) as stop_latitude,
        cast(stop_longitude as double) as stop_longitude,
        cast(is_station as boolean) as is_station,
        cast(trip_id as string) as trip_id,
        cast(route_id as string) as route_id,
        cast(route_short_name as string) as route_short_name,
        cast(scheduledDeparture as timestamp) as scheduled_departure,
        cast(estimatedDeparture as timestamp) as estimated_departure,
        cast(recordedTime as timestamp) as recorded_time,
        cast(minutesTillDeparture as double) as minutes_till_departure,
        cast(isRealTime as boolean) as is_real_time,
        cast(response_time as timestamp) as response_time,
        cast(raw_departure_json as string) as raw_departure_json,
        (
            unix_timestamp(cast(estimatedDeparture as timestamp))
            - unix_timestamp(cast(scheduledDeparture as timestamp))
        ) / 60.0 as estimate_offset_minutes,
        (
            unix_timestamp(cast(estimatedDeparture as timestamp))
            - unix_timestamp(cast(ingestion_timestamp as timestamp))
        ) / 60.0 as time_to_departure_minutes
    from source
)

select distinct *
from renamed
