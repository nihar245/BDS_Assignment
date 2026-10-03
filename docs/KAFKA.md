# Kafka

Kafka runs as one Dockerized KRaft broker for local development. There is no ZooKeeper, Schema Registry, Kafka Connect, or Control Center. Start it with `docker compose -f kafka/docker-compose.yml up -d`. The producer and host Spark connect to `localhost:9092`. The producer sends the trip ID as the message key and the canonical event as JSON value.

Create/check the topic from PowerShell with:

```powershell
docker compose -f kafka/docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic uber_trips --partitions 1 --replication-factor 1
```

`KAFKA_BOOTSTRAP_SERVERS` defaults to `localhost:9092` and `KAFKA_STARTING_OFFSETS` defaults to `earliest`.

Spark uses `earliest` offsets by default, configurable with `KAFKA_STARTING_OFFSETS`. Topic creation, broker setup, retention, and security are host-side responsibilities.
