"""Distributed column-split ingestion for the Uber streaming demo.

Each Docker node publishes only its assigned columns to a partial Kafka topic.
The master node also assembles matching trip_ids and publishes canonical events
into the existing KAFKA_TOPIC consumed by Spark Structured Streaming.
"""
import argparse
import csv
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from kafka import KafkaConsumer, KafkaProducer

LOG = logging.getLogger("distributed_ingestion")
SOURCE_TZ = ZoneInfo(os.getenv("SOURCE_TIMEZONE", "America/New_York"))
ROLES = {
    "master": ("Date/Time",),
    "worker1": ("Lat",),
    "worker2": ("Lon", "Base"),
}
TOPICS = {
    "master": os.getenv("PARTIAL_TOPIC_MASTER", "uber_partial_datetime"),
    "worker1": os.getenv("PARTIAL_TOPIC_WORKER1", "uber_partial_lat"),
    "worker2": os.getenv("PARTIAL_TOPIC_WORKER2", "uber_partial_lon_base"),
}


def make_trip_id(row: dict[str, str], row_number: int) -> str:
    identity = f"{row_number}|{row['Date/Time'].strip()}|{row['Lat'].strip()}|{row['Lon'].strip()}|{row['Base'].strip()}"
    return "uber-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]


def canonical(row: dict[str, str], row_number: int) -> dict[str, object]:
    dt = row["Date/Time"].strip()
    lat = float(row["Lat"])
    lon = float(row["Lon"])
    base = row["Base"].strip()
    ts = datetime.strptime(dt, "%m/%d/%Y %H:%M:%S").replace(tzinfo=SOURCE_TZ).astimezone(timezone.utc)
    if not base or not -90 <= lat <= 90 or not -180 <= lon <= 180:
        raise ValueError("invalid Uber row")
    return {
        "trip_id": make_trip_id(row, row_number),
        "event_timestamp": ts.isoformat(),
        "pickup_latitude": lat,
        "pickup_longitude": lon,
        "base": base,
    }


def partial_event(event: dict[str, object], role: str) -> dict[str, object]:
    if role == "master":
        return {"trip_id": event["trip_id"], "event_timestamp": event["event_timestamp"]}
    if role == "worker1":
        return {"trip_id": event["trip_id"], "pickup_latitude": event["pickup_latitude"]}
    return {"trip_id": event["trip_id"], "pickup_longitude": event["pickup_longitude"], "base": event["base"]}


def producer(bootstrap: str):
    return KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        key_serializer=lambda k: str(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
        acks="all", retries=5, linger_ms=10,
    )


def run_partial(role: str, input_path: str, bootstrap: str, rate: float, limit: int | None):
    topic = TOPICS[role]
    p = producer(bootstrap)
    published = 0
    with Path(input_path).open("r", encoding="utf-8", newline="") as fh:
        for row_number, row in enumerate(csv.DictReader(fh), start=1):
            try:
                trip_id = row["trip_id"].strip()
                if not trip_id:
                    raise ValueError("missing trip_id")
                if role == "master":
                    event = {"trip_id": trip_id, "event_timestamp": datetime.strptime(
                        row["Date/Time"].strip(), "%m/%d/%Y %H:%M:%S"
                    ).replace(tzinfo=SOURCE_TZ).astimezone(timezone.utc).isoformat()}
                elif role == "worker1":
                    lat = float(row["Lat"])
                    if not -90 <= lat <= 90:
                        raise ValueError("invalid latitude")
                    event = {"trip_id": trip_id, "pickup_latitude": lat}
                else:
                    lon = float(row["Lon"])
                    base = row["Base"].strip()
                    if not -180 <= lon <= 180 or not base:
                        raise ValueError("invalid longitude/base")
                    event = {"trip_id": trip_id, "pickup_longitude": lon, "base": base}
            except (KeyError, TypeError, ValueError) as exc:
                LOG.warning("[%s] skipping shard row %s: %s", role, row_number, exc)
                continue
            p.send(topic, key=trip_id, value=event)
            published += 1
            if published % 10 == 0:
                p.flush()
                LOG.info("[%s] published %s partial events", role, published)
            if limit and published >= limit:
                break
            if rate > 0:
                time.sleep(1.0 / rate)
    p.flush(); p.close()
    LOG.info("[%s] finished with %s partial events", role, published)


def run_assembler(bootstrap: str, output_topic: str, rate: float, limit: int | None):
    topics = list(TOPICS.values())
    consumer = KafkaConsumer(
        *topics, bootstrap_servers=bootstrap.split(","),
        group_id="uber-distributed-assembler-v1",
        auto_offset_reset="earliest", enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode()),
    )
    p = producer(bootstrap)
    state: dict[str, dict[str, object]] = {}
    emitted = 0
    last_emit = 0.0
    while not limit or emitted < limit:
        for msg in consumer:
            part = msg.value
            trip_id = part["trip_id"]
            item = state.setdefault(trip_id, {})
            item.update(part)
            if all(k in item for k in ("event_timestamp", "pickup_latitude", "pickup_longitude", "base")):
                if rate > 0:
                    delay = (1.0 / rate) - (time.monotonic() - last_emit)
                    if delay > 0:
                        time.sleep(delay)
                p.send(output_topic, key=trip_id, value=item)
                p.flush()
                emitted += 1
                last_emit = time.monotonic()
                del state[trip_id]
                if emitted % 10 == 0:
                    LOG.info("[master-assembler] emitted %s complete events/sec target=%s", emitted, rate)
                if limit and emitted >= limit:
                    break
    p.flush(); p.close(); consumer.close()


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=["master", "worker1", "worker2"], default=os.getenv("NODE_ROLE", "master"))
    parser.add_argument("--input", default=os.getenv("UBER_INPUT_PATH", "data/raw/uber-raw-data-apr14.csv"))
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--rate", type=float, default=float(os.getenv("DISTRIBUTED_PARTIAL_RATE", "10")))
    parser.add_argument("--merge-rate", type=float, default=float(os.getenv("DISTRIBUTED_MERGE_RATE", "10")))
    parser.add_argument("--limit", type=int, default=int(os.getenv("PRODUCER_LIMIT", "0")) or None)
    args = parser.parse_args()
    if args.role == "master":
        # The master both participates in the Date/Time stream and assembles the three streams.
        import threading
        t = threading.Thread(target=run_partial, args=(args.role, args.input, args.bootstrap, args.rate, args.limit), daemon=True)
        t.start()
        run_assembler(args.bootstrap, os.getenv("KAFKA_TOPIC", "uber_demo"), args.merge_rate, args.limit)
    else:
        run_partial(args.role, input_path=args.input, bootstrap=args.bootstrap, rate=args.rate, limit=args.limit)

if __name__ == "__main__":
    main()
