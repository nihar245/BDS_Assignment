{{ config(materialized='table') }}
select
    day_of_week,
    day_name,
    pickup_hour,
    count(*)::bigint as trip_count
from {{ ref('fct_uber_trips') }}
group by day_of_week, day_name, pickup_hour
order by day_of_week, pickup_hour
