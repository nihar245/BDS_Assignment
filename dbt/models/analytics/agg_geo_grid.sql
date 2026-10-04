{{ config(materialized='table') }}
select
    latitude_grid,
    longitude_grid,
    count(*)::bigint as trip_count,
    round(100.0 * count(*) / nullif(sum(count(*)) over (), 0), 2) as share_pct,
    rank() over (order by count(*) desc) as rank
from {{ ref('fct_uber_trips') }}
group by latitude_grid, longitude_grid
order by trip_count desc
