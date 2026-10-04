{{ config(materialized='table') }}

with base_counts as (
    select base, count(*)::bigint as trip_count
    from {{ ref('fct_uber_trips') }}
    group by base
)
select
    base,
    trip_count,
    round(100.0 * trip_count / nullif(sum(trip_count) over (), 0), 2) as share_pct,
    rank() over (order by trip_count desc) as rank
from base_counts
order by trip_count desc
