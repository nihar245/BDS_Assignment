create schema if not exists analytics;

create table if not exists analytics.streaming_batches (
    batch_id bigint primary key,
    records_received bigint not null,
    processed_at timestamptz not null
);
