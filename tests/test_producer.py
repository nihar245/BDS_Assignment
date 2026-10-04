import json

from producer.uber_producer import read_events, row_to_event


VALID_ROW = {
    "Date/Time": "4/1/2014 00:11:05",
    "Lat": "40.769",
    "Lon": "-73.954",
    "Base": "B02512",
}


def test_row_to_event_is_deterministic_and_canonical():
    event = row_to_event(VALID_ROW, 1)
    assert event == row_to_event(VALID_ROW, 1)
    assert set(event) == {
        "trip_id",
        "event_timestamp",
        "pickup_latitude",
        "pickup_longitude",
        "base",
    }
    assert event["event_timestamp"] == "2014-04-01T04:11:05+00:00"


def test_read_events_skips_malformed_rows(tmp_path):
    path = tmp_path / "uber.csv"
    path.write_text(
        "Date/Time,Lat,Lon,Base\n4/1/2014 00:11:05,40.769,-73.954,B02512\n"
        "bad,not-a-number,0,BAD\n",
        encoding="utf-8",
    )
    events = list(read_events(str(path)))
    assert len(events) == 1
    json.dumps(events[0])
