# Dashboard

Run `streamlit run dashboard/app.py`.

The dashboard reads dbt-built PostgreSQL analytics tables, refreshes its fragment every 10 seconds, and provides:

- KPI cards: total trips, active bases, average daily trips, busiest hour, peak-hour share, weekend share.
- Daily volume, day-over-day percentage change, and seven-day rolling average.
- Hourly volume and peak-hour classification.
- Day × hour activity matrix.
- Time-of-day and weekday/weekend distributions.
- Base ranking, share, and selected-base hourly distribution.
- Top pickup geographic grid cells and an interactive map.
- Data-quality checks for row counts, valid coordinates, unique IDs, and duplicates.
- Spark batch health: recent batch IDs, batch sizes, and processing timestamps.

All metrics are derived from fields actually present in the FiveThirtyEight dataset. The dashboard does not invent fare, distance, duration, passenger, payment, driver, or revenue metrics.
