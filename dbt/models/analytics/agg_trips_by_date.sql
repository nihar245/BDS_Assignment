{{ config(materialized='table') }}

with daily as (
    select trip_date, count(*)::bigint as trip_count
    from {{ ref('fct_uber_trips') }}
    group by trip_date
)
select
    trip_date,
    trip_count,
    round(100.0 * trip_count / nullif(sum(trip_count) over (), 0), 2) as share_pct,
    round(avg(trip_count) over (), 2) as avg_daily_trips,
    round(avg(trip_count) over (order by trip_date rows between 6 preceding and current row), 2) as rolling_7d_avg,
    round(100.0 * (trip_count - lag(trip_count) over (order by trip_date))
        / nullif(lag(trip_count) over (order by trip_date), 0), 2) as pct_change
from daily
order by trip_date
