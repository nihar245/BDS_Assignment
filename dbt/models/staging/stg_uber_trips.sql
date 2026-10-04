{{ config(materialized='view') }}

select
    trip_id,
    event_timestamp::timestamp with time zone as event_timestamp,
    pickup_latitude::double precision as pickup_latitude,
    pickup_longitude::double precision as pickup_longitude,
    base,
    event_timestamp::date as trip_date,
    extract(hour from event_timestamp)::integer as pickup_hour,
    extract(isodow from event_timestamp)::integer as day_of_week,
    to_char(event_timestamp, 'Dy') as day_name,
    case when extract(isodow from event_timestamp) in (6, 7) then true else false end as is_weekend,
    case
        when extract(hour from event_timestamp) between 5 and 11 then 'Morning'
        when extract(hour from event_timestamp) between 12 and 16 then 'Afternoon'
        when extract(hour from event_timestamp) between 17 and 20 then 'Evening'
        else 'Night'
    end as time_of_day,
    case
        when extract(hour from event_timestamp) between 7 and 10
          or extract(hour from event_timestamp) between 17 and 20
        then true else false
    end as is_peak_hour,
    round(pickup_latitude::numeric, 2) as latitude_grid,
    round(pickup_longitude::numeric, 2) as longitude_grid
from {{ source('uber', 'uber_demo_trips') }}
where trip_id is not null
  and event_timestamp is not null
  and pickup_latitude between -90 and 90
  and pickup_longitude between -180 and 180
  and base is not null
  and base <> ''
