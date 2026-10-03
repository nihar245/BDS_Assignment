{{ config(materialized='table') }}
select
    base,
    pickup_hour,
    count(*)::bigint as trip_count
from {{ ref('fct_uber_trips') }}
group by base, pickup_hour
order by base, pickup_hour
