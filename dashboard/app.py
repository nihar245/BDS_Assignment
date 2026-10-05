"""Streamlit dashboard for the PostgreSQL Uber analytics models."""

import os

import pandas as pd
import psycopg2
import streamlit as st


st.set_page_config(page_title="Uber Pickup Analytics", layout="wide")
st.title("Uber Pickup Analytics")
st.caption("FiveThirtyEight Uber pickup data • Kafka → Spark → HDFS/PostgreSQL → dbt")


@st.cache_resource
def connection():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "uber_streaming"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )
    conn.autocommit = True
    return conn


@st.cache_data(ttl=20)
def query(sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, connection())


@st.fragment(run_every="30s")
def render_dashboard():
    try:
        kpis = query("SELECT * FROM analytics.agg_kpis").iloc[0]
        by_date = query("SELECT * FROM analytics.agg_trips_by_date ORDER BY trip_date")
        by_hour = query("SELECT * FROM analytics.agg_trips_by_hour ORDER BY pickup_hour")
        by_base = query("SELECT * FROM analytics.agg_trips_by_base ORDER BY trip_count DESC")
        by_day_type = query("SELECT * FROM analytics.agg_trips_by_day_type ORDER BY trip_count DESC")
        by_time = query("SELECT * FROM analytics.agg_trips_by_time_of_day ORDER BY trip_count DESC")
        heatmap = query("SELECT * FROM analytics.agg_day_hour_heatmap ORDER BY day_of_week, pickup_hour")
        base_hour = query("SELECT * FROM analytics.agg_trips_by_base_hour ORDER BY base, pickup_hour")
        geo = query("SELECT * FROM analytics.agg_geo_grid ORDER BY trip_count DESC LIMIT 250")
        batches = query("SELECT * FROM analytics.streaming_batches ORDER BY processed_at DESC LIMIT 10")
    except Exception as error:
        st.error(f"PostgreSQL/dbt analytics are unavailable: {error}")
        return

    peak_share = (float(kpis["peak_trips"]) / float(kpis["total_trips"]) * 100) if kpis["total_trips"] else 0
    weekend_share = (float(kpis["weekend_trips"]) / float(kpis["total_trips"]) * 100) if kpis["total_trips"] else 0

    cols = st.columns(6)
    cols[0].metric("Total Trips", f"{int(kpis['total_trips']):,}")
    cols[1].metric("Active Bases", int(kpis["active_bases"]))
    cols[2].metric("Avg Trips / Day", f"{float(kpis['avg_daily_trips']):,.0f}")
    cols[3].metric("Busiest Hour", f"{int(kpis['busiest_hour']):02d}:00")
    cols[4].metric("Peak Share", f"{peak_share:.1f}%")
    cols[5].metric("Weekend Share", f"{weekend_share:.1f}%")

    st.divider()

    left, right = st.columns(2)
    with left:
        st.subheader("Daily Trip Volume")
        st.line_chart(by_date.set_index("trip_date")["trip_count"])
        st.dataframe(by_date, use_container_width=True, hide_index=True)

    with right:
        st.subheader("Trips by Pickup Hour")
        st.bar_chart(by_hour.set_index("pickup_hour")["trip_count"])
        st.dataframe(by_hour, use_container_width=True, hide_index=True)

    st.subheader("Day × Hour Activity")
    heat = heatmap.pivot(index="day_name", columns="pickup_hour", values="trip_count").fillna(0)
    st.dataframe(heat.style.background_gradient(), use_container_width=True)

    left, right = st.columns(2)
    with left:
        st.subheader("Time of Day")
        st.bar_chart(by_time.set_index("time_of_day")["trip_count"])
    with right:
        st.subheader("Weekday vs Weekend")
        st.bar_chart(by_day_type.set_index("day_type")["trip_count"])

    st.subheader("Uber Base Analytics")
    st.dataframe(by_base, use_container_width=True, hide_index=True)

    selected_base = st.selectbox("Base for hourly distribution", by_base["base"].tolist(), key="base_selector")
    selected = base_hour[base_hour["base"] == selected_base]
    st.bar_chart(selected.set_index("pickup_hour")["trip_count"])

    st.subheader("Top Pickup Grid Cells")
    st.caption("Grid cells are rounded latitude/longitude buckets; they are analytical zones, not named neighborhoods.")
    st.dataframe(geo, use_container_width=True, hide_index=True)

    map_points = geo.rename(columns={"latitude_grid": "lat", "longitude_grid": "lon"})[["lat", "lon"]]
    if not map_points.empty:
        st.map(map_points)

    st.subheader("Streaming Health")
    if batches.empty:
        st.info("No Spark batch metrics have been recorded yet.")
    else:
        latest = batches.iloc[0]
        stream_cols = st.columns(4)
        stream_cols[0].metric("Latest Batch", int(latest["batch_id"]))
        stream_cols[1].metric("Latest Batch Size", int(latest["records_received"]))
        stream_cols[2].metric("Batches Recorded", len(batches))
        stream_cols[3].metric("Last Processed", str(latest["processed_at"]))
        st.dataframe(batches, use_container_width=True, hide_index=True)

render_dashboard()
