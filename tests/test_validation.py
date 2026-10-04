import pytest

from producer.uber_producer import row_to_event

from test_producer import VALID_ROW


def test_invalid_coordinates_are_rejected():
    row = dict(VALID_ROW, Lat="91")
    with pytest.raises(ValueError):
        row_to_event(row, 1)


def test_invalid_longitude_base_and_timestamp_are_rejected():
    for field, value in (("Lon", "181"), ("Base", ""), ("Date/Time", "bad")):
        row = dict(VALID_ROW, **{field: value})
        with pytest.raises(ValueError):
            row_to_event(row, 1)
