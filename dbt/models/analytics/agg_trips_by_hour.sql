{{ config(materialized='table') }}

with hourly as (
    select pickup_hour, count(*)::bigint as trip_count
    from {{ ref('fct_uber_trips') }}
    group by pickup_hour
)
select
    pickup_hour,
    trip_count,
    round(100.0 * trip_count / nullif(sum(trip_count) over (), 0), 2) as share_pct,
    rank() over (order by trip_count desc) as rank,
    case when pickup_hour between 7 and 10 or pickup_hour between 17 and 20 then true else false end as is_peak_hour
from hourly
order by pickup_hour
