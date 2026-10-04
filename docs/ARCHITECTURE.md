# Architecture

```mermaid
flowchart TD
  R[Raw Uber CSV] -->|read and hash| P[Producer]
  P -->|JSON| T[Kafka topic uber_trips]
  T -->|Structured Streaming| V[Parse and validate]
  V --> E[Add trip_date pickup_hour day_of_week]
  E -->|checkpointed append| HDFS[HDFS Parquet]
  E -->|separate foreachBatch checkpoint| STG[PostgreSQL staging]
  STG --> DBT[dbt staging and analytics]
  DBT --> DASH[Streamlit]
```

The stream is the core. HDFS and PostgreSQL are independent sinks of the same transformed stream; Airflow is not required for ingestion. Only source-supported fields and documented time derivations are retained.
