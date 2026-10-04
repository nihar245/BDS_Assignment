{{ config(materialized='table') }}
select
    case when is_weekend then 'Weekend' else 'Weekday' end as day_type,
    count(*)::bigint as trip_count,
    round(100.0 * count(*) / nullif(sum(count(*)) over (), 0), 2) as share_pct
from {{ ref('fct_uber_trips') }}
group by 1
order by trip_count desc
