{{ config(materialized='view') }}

select
    route_short_name,
    route_group_name,
    hex_color,
    text_hex_color
from {{ source('cumtd_raw', 'route_colors') }}
