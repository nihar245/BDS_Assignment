{{ config(materialized='table') }}
with base as (select * from {{ ref('fct_uber_trips') }}),
daily as (select trip_date, count(*) as trip_count from base group by trip_date),
hourly as (select pickup_hour, count(*) as trip_count from base group by pickup_hour),
bases as (select base as base_name, count(*) as trip_count from base group by base)
select
    (select count(*) from base)::bigint as total_trips,
    (select count(distinct base_name) from bases)::bigint as active_bases,
    (select count(*) from daily)::bigint as unique_dates,
    (select count(*) from hourly)::bigint as unique_hours,
    (select round(avg(trip_count), 2) from daily) as avg_daily_trips,
    (select percentile_cont(0.5) within group (order by trip_count) from daily) as median_daily_trips,
    (select pickup_hour from hourly order by trip_count desc, pickup_hour limit 1)::integer as busiest_hour,
    (select pickup_hour from hourly order by trip_count asc, pickup_hour limit 1)::integer as quietest_hour,
    (select base_name from bases order by trip_count desc, base_name limit 1) as top_base,
    (select trip_count from bases order by trip_count desc, base_name limit 1)::bigint as top_base_trips,
    (select count(*) from base where is_peak_hour)::bigint as peak_trips,
    (select count(*) from base where is_weekend)::bigint as weekend_trips
