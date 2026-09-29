"""Publish FiveThirtyEight Uber pickup rows to Kafka in restart-safe batches."""

import argparse
import csv
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Mapping
from zoneinfo import ZoneInfo

LOGGER = logging.getLogger(__name__)
DATASET_URL = (
    "https://raw.githubusercontent.com/fivethirtyeight/uber-tlc-foil-response/"
    "master/uber-trip-data/uber-raw-data-apr14.csv"
)
SOURCE_TIMEZONE = ZoneInfo("America/New_York")


def row_to_event(row: Mapping[str, str], row_number: int) -> dict[str, object]:
    date_time = row["Date/Time"].strip()
    latitude = float(row["Lat"])
    longitude = float(row["Lon"])
    base = row["Base"].strip()
    local_timestamp = datetime.strptime(date_time, "%m/%d/%Y %H:%M:%S").replace(
        tzinfo=SOURCE_TIMEZONE
    )
    timestamp = local_timestamp.astimezone(timezone.utc)
    identity = f"{row_number}|{date_time}|{latitude}|{longitude}|{base}"
    trip_id = f"uber-{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:20]}"
    if not base or not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        raise ValueError("invalid Uber row values")
    return {
        "trip_id": trip_id,
        "event_timestamp": timestamp.isoformat(),
        "pickup_latitude": latitude,
        "pickup_longitude": longitude,
        "base": base,
    }


def read_events(input_path: str) -> Iterator[dict[str, object]]:
    with Path(input_path).open("r", encoding="utf-8", newline="") as csv_file:
        for row_number, row in enumerate(csv.DictReader(csv_file), start=1):
            try:
                yield row_to_event(row, row_number)
            except (KeyError, TypeError, ValueError) as error:
                LOGGER.warning("Skipping malformed row %s: %s", row_number, error)


def create_producer(bootstrap_servers: str):
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=20,
    )


def _load_position(state_path: Path) -> int:
    if not state_path.exists():
        return 0
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
        return max(0, int(data.get("next_row", 0)))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        LOGGER.warning("Invalid producer state; restarting from row 1")
        return 0


def _save_position(state_path: Path, next_row: int) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = state_path.with_suffix(state_path.suffix + ".tmp")
    temporary.write_text(json.dumps({"next_row": next_row}) + "\n", encoding="utf-8")
    temporary.replace(state_path)


def publish_events(
    input_path: str,
    bootstrap_servers: str,
    topic: str,
    batch_size: int = 10,
    interval_seconds: float = 10.0,
    limit: int | None = None,
    state_path: str = "data/producer_state.json",
    resume: bool = True,
) -> int:
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    if interval_seconds < 0:
        raise ValueError("interval_seconds must be >= 0")

    producer = create_producer(bootstrap_servers)
    state_file = Path(state_path)
    start_row = _load_position(state_file) if resume else 0
    published = 0
    current_row = start_row

    event_iterator = iter(read_events(input_path))
    for _ in range(start_row):
        try:
            next(event_iterator)
        except StopIteration:
            break

    try:
        while True:
            if limit is not None and published >= limit:
                break

            remaining = batch_size if limit is None else min(batch_size, limit - published)
            batch = []
            for _ in range(remaining):
                try:
                    batch.append(next(event_iterator))
                except StopIteration:
                    break
            if not batch:
                LOGGER.info("Producer reached end of dataset at event position %s", current_row)
                break

            for event in batch:
                producer.send(topic, key=str(event["trip_id"]), value=event)
            producer.flush()

            current_row += len(batch)
            published += len(batch)
            if resume:
                _save_position(state_file, current_row)

            LOGGER.info(
                "Published batch: %s events | source rows %s-%s | total this run=%s",
                len(batch), current_row - len(batch) + 1, current_row, published,
            )

            if len(batch) < remaining:
                break
            if interval_seconds > 0:
                time.sleep(interval_seconds)
    finally:
        producer.close()

    return published


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=os.getenv("UBER_INPUT_PATH", "data/raw/uber-raw-data-apr14.csv"))
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "uber_trips"))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("PRODUCER_BATCH_SIZE", "10")))
    parser.add_argument("--interval-seconds", type=float, default=float(os.getenv("PRODUCER_INTERVAL_SECONDS", "10")))
    parser.add_argument("--limit", type=int, default=int(os.getenv("PRODUCER_LIMIT", "0")) or None)
    parser.add_argument("--state-path", default=os.getenv("PRODUCER_STATE_PATH", "data/producer_state.json"))
    parser.add_argument("--no-resume", action="store_true")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    args = parse_args()
    count = publish_events(
        args.input,
        args.bootstrap_servers,
        args.topic,
        args.batch_size,
        args.interval_seconds,
        args.limit,
        args.state_path,
        not args.no_resume,
    )
    LOGGER.info("Published %s Uber events to %s", count, args.topic)


if __name__ == "__main__":
    main()
