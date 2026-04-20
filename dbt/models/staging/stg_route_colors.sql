{{ config(materialized='view') }}

select
    route_short_name,
    route_group_name,
    hex_color,
    text_hex_color
from {{ var('catalog') }}.{{ var('raw_schema') }}.route_colors
