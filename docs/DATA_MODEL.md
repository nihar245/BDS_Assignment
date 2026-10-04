# Data Model

The FiveThirtyEight Uber April 2014 dataset contains 564,516 records and these source columns: `Date/Time`, `Lat`, `Lon`, and `Base`. No unsupported business fields are introduced.

### Canonical Kafka event

| Column | Type | Meaning |
|---|---|---|
| `trip_id` | string | SHA-256-derived deterministic ID for one source row |
| `event_timestamp` | timestamp | Source local pickup time converted from `America/New_York` to UTC |
| `pickup_latitude` | double | Source `Lat` |
| `pickup_longitude` | double | Source `Lon` |
| `base` | string | Source `Base` |

### Spark/dbt analytical fields

| Column | Type | Meaning |
|---|---|---|
| `trip_date` | date | Pickup date in the source timezone |
| `pickup_hour` | integer | Pickup hour, 0-23, in the source timezone |
| `day_of_week` | integer | ISO weekday, 1-7 |
| `day_name` | string | Short weekday label |
| `is_weekend` | boolean | Saturday/Sunday flag |
| `time_of_day` | string | Morning, Afternoon, Evening, or Night bucket |
| `is_peak_hour` | boolean | Configured high-volume hour bucket |
| `latitude_grid` | numeric | Latitude rounded to two decimals for analytical density cells |
| `longitude_grid` | numeric | Longitude rounded to two decimals for analytical density cells |

The analytics layer additionally calculates counts, shares, ranks, daily percentage change, seven-day rolling average, day/hour matrices, base/hour distributions, geographic density, and streaming batch health.

No dropoff, fare, passenger count, distance, payment, duration, driver ID, or revenue fields exist in this dataset and none are modeled.
