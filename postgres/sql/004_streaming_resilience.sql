-- Safe migration for an existing project database.
create table if not exists staging.uber_trips_ingest (
    trip_id varchar(32) not null,
    event_timestamp timestamptz not null,
    pickup_latitude double precision not null,
    pickup_longitude double precision not null,
    base varchar(32) not null,
    trip_date date not null,
    pickup_hour integer not null,
    day_of_week integer not null
);

create schema if not exists analytics;

create table if not exists analytics.streaming_batches (
    batch_id bigint primary key,
    records_received bigint not null,
    processed_at timestamptz not null
);
