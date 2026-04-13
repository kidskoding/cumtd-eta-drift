with snapshots as (
    select *
    from {{ ref('stg_departure_snapshots') }}
    where stop_id is not null
      and trip_id is not null
      and scheduled_departure is not null
      and estimated_departure is not null
),

updates as (
    select
        *,
        lag(estimated_departure) over (
            partition by stop_id, trip_id, route_id, route_short_name
            order by ingestion_timestamp
        ) as previous_estimated_departure,
        first_value(scheduled_departure) over (
            partition by stop_id, trip_id, route_id, route_short_name
            order by ingestion_timestamp
            rows between unbounded preceding and unbounded following
        ) as first_scheduled_departure,
        first_value(estimated_departure) over (
            partition by stop_id, trip_id, route_id, route_short_name
            order by ingestion_timestamp
            rows between unbounded preceding and unbounded following
        ) as first_estimated_departure,
        last_value(estimated_departure) over (
            partition by stop_id, trip_id, route_id, route_short_name
            order by ingestion_timestamp
            rows between unbounded preceding and unbounded following
        ) as last_estimated_departure
    from snapshots
),

with_deltas as (
    select
        *,
        (
            unix_timestamp(estimated_departure)
            - unix_timestamp(previous_estimated_departure)
        ) / 60.0 as eta_delta_minutes,
        case
            when previous_estimated_departure is not null
             and estimated_departure < previous_estimated_departure
            then 1
            else 0
        end as monotonicity_violation,
        case
            when previous_estimated_departure is not null
             and estimated_departure > previous_estimated_departure
            then (
                unix_timestamp(estimated_departure)
                - unix_timestamp(previous_estimated_departure)
            ) / 60.0
            else 0.0
        end as forward_jump_minutes,
        case
            when previous_estimated_departure is not null
             and estimated_departure < previous_estimated_departure
            then (
                unix_timestamp(previous_estimated_departure)
                - unix_timestamp(estimated_departure)
            ) / 60.0
            else 0.0
        end as backward_jump_minutes
    from updates
)

select *
from with_deltas
