select *
from {{ ref('fct_uber_trips') }}
where trip_date is null
   or event_timestamp::date <> trip_date
