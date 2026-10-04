{{ config(materialized='table') }}

select
    trip_id,
    event_timestamp,
    trip_date,
    pickup_hour,
    day_of_week,
    day_name,
    is_weekend,
    time_of_day,
    is_peak_hour,
    latitude_grid,
    longitude_grid,
    pickup_latitude,
    pickup_longitude,
    base
from {{ ref('stg_uber_trips') }}
