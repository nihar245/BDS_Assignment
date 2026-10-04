select *
from {{ ref('fct_uber_trips') }}
where pickup_hour not between 0 and 23
