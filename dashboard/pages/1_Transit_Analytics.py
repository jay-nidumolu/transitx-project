import streamlit as st
import plotly.express as px
import pandas as pd
import os



st.title("📊 Transit Analytics — Delay Trends (TTC 2023–2024)")
st.caption("Analyze TTC delay + weather patterns using processed data from 2023–2024.")

LOCAL_PATH = "../data/processed/transit_processed.csv"

# -----------------------------
# Load Data
# -----------------------------
if os.path.exists(LOCAL_PATH):
    df = pd.read_csv(LOCAL_PATH)
    # st.success("Loaded processed dataset successfully.")
else:
    st.error("Dataset not found. Run ETL first.")
    st.stop()

# -----------------------------
# Clean Column Names (IMPORTANT)
# -----------------------------
df.rename(columns={
    "temperature_2m (°c)": "temperature",
    "precipitation (mm)": "precipitation",
}, inplace=True)

df["date"] = pd.to_datetime(df["date"], errors="coerce")
df["route"] = df["route"].astype(str).str.strip().str.upper()
df = df[df["route"].str.len() <= 4]   # drop corrupted route values

# -----------------------------
# Sidebar Filters
# -----------------------------
st.sidebar.header("🔎 Filter Options")

# Year filter
years = st.sidebar.multiselect(
    "Select Year(s)",
    options=sorted(df["date"].dt.year.unique()),
    default=sorted(df["date"].dt.year.unique())
)

df_filtered = df[df["date"].dt.year.isin(years)]

# Month filter (slider)
months = st.sidebar.select_slider(
    "Select Month Range",
    options=list(range(1, 13)),
    value=(1, 12),
    format_func=lambda m: pd.to_datetime(str(m), format="%m").strftime("%b")
)

df_filtered = df_filtered[
    (df_filtered["date"].dt.month >= months[0]) &
    (df_filtered["date"].dt.month <= months[1])
]

# Route filter (top 20)
top_routes = df_filtered["route"].value_counts().head(30).index.tolist()

route_selected = st.sidebar.multiselect(
    "Select Route(s)",
    options=top_routes,
    default=top_routes[:5]
)

if route_selected:
    df_filtered = df_filtered[df_filtered["route"].isin(route_selected)]

# Delay filter
delay_min, delay_max = st.sidebar.slider(
    "Delay Range (minutes)",
    int(df_filtered["min_delay"].min()),
    int(df_filtered["min_delay"].max()),
    (0, 20)
)

df_filtered = df_filtered[
    (df_filtered["min_delay"] >= delay_min) &
    (df_filtered["min_delay"] <= delay_max)
]

# Weather Filter
weather_filter = st.sidebar.radio(
    "Weather",
    ["All", "Rain Only", "No Rain"]
)

if weather_filter == "Rain Only":
    df_filtered = df_filtered[df_filtered["precipitation"] > 0]
elif weather_filter == "No Rain":
    df_filtered = df_filtered[df_filtered["precipitation"] <= 0]

# Weekend filter
show_weekends = st.sidebar.checkbox("Include Weekends", value=True)
if not show_weekends:
    df_filtered = df_filtered[df_filtered["date"].dt.weekday < 5]

#st.success(f"Filters applied — {len(df_filtered)} rows")

# -----------------------------
# Visualizations
# -----------------------------

# 1️⃣ Top 10 Routes
st.subheader("🚌 Routes With Highest Average Delay")
route_delay = (
    df_filtered.groupby("route")["min_delay"]
    .mean()
    .sort_values(ascending=False)
    .head(10)
    .reset_index()
)

fig1 = px.bar(
    route_delay,
    x="min_delay",
    y="route",
    orientation="h",
    labels={"route": "Route", "min_delay": "Avg Delay (min)"},
    title="Top Delayed TTC Routes",
    color="min_delay"
)

st.plotly_chart(fig1, use_container_width=True)


# 2️⃣ Delay Trend Over Time
st.subheader("📅 Delay Trend Over Time")
daily = df_filtered.groupby("date")["min_delay"].mean().reset_index()

fig2 = px.line(
    daily,
    x="date",
    y="min_delay",
    title="Daily Average Delay (Filtered)"
)

st.plotly_chart(fig2, use_container_width=True)

# 3️⃣ Weather Impact Scatter
st.subheader("🌧 Weather Impact on Delay")

df_filtered["rain_intensity"] = pd.cut(
    df_filtered["precipitation"],
    bins=[-0.1, 0.1, 2, 5, 20],
    labels=["None", "Light", "Moderate", "Heavy"]
)

fig3 = px.scatter(
    df_filtered,
    x="temperature",
    y="min_delay",
    color="rain_intensity",
    title="Temperature vs Delay (Colored by Rain Intensity)"
)

st.plotly_chart(fig3, use_container_width=True)
