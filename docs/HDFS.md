# HDFS

Processed data defaults to `hdfs://localhost:9000/data/uber/trips`. The HDFS sink checkpoint defaults to `hdfs://localhost:9000/data/uber/checkpoints/trips`; the PostgreSQL sink uses the separate `hdfs://localhost:9000/data/uber/checkpoints/postgres` checkpoint. Override these paths in `.env`. HDFS directory creation, permissions, replication, and Spark connector availability are infrastructure setup tasks outside this repository.
