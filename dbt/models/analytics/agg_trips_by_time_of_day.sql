{{ config(materialized='table') }}
select
    time_of_day,
    count(*)::bigint as trip_count,
    round(100.0 * count(*) / nullif(sum(count(*)) over (), 0), 2) as share_pct
from {{ ref('fct_uber_trips') }}
group by time_of_day
order by trip_count desc
