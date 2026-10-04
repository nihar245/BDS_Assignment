create table if not exists staging.uber_trips (
    trip_id varchar(32) primary key,
    event_timestamp timestamptz not null,
    pickup_latitude double precision not null check (pickup_latitude between -90 and 90),
    pickup_longitude double precision not null check (pickup_longitude between -180 and 180),
    base varchar(32) not null check (base <> ''),
    trip_date date not null,
    pickup_hour integer not null check (pickup_hour between 0 and 23),
    day_of_week integer not null check (day_of_week between 1 and 7)
);

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

create index if not exists ix_uber_trips_trip_date on staging.uber_trips (trip_date);
create index if not exists ix_uber_trips_base on staging.uber_trips (base);
