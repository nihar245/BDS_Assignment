select *
from {{ ref('stg_uber_trips') }}
where pickup_latitude not between -90 and 90
   or pickup_longitude not between -180 and 180
