# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # 03 - Analyze Departure Drift
# MAGIC
# MAGIC Visualize ETA instability from the `raw_departure_snapshots` and `departure_drift_metrics` Delta tables.

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql.functions import col, desc

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cumtd_eta_drift")
dbutils.widgets.text("trip_id", "")
dbutils.widgets.text("stop_id", "")
dbutils.widgets.text("top_n", "20")

catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema").strip()
selected_trip_id = dbutils.widgets.get("trip_id").strip()
selected_stop_id = dbutils.widgets.get("stop_id").strip()
top_n = int(dbutils.widgets.get("top_n"))

database_name = f"{catalog}.{schema_name}" if catalog else schema_name
raw_table = f"{database_name}.raw_departure_snapshots"
metrics_table = f"{database_name}.departure_drift_metrics"

# COMMAND ----------

raw_df = spark.table(raw_table)
metrics_df = spark.table(metrics_table)

display(metrics_df.orderBy(desc("drift_minutes"), desc("update_count")).limit(top_n))

# COMMAND ----------

if not selected_trip_id or not selected_stop_id:
    candidate = metrics_df.orderBy(desc("drift_minutes"), desc("update_count")).limit(1).collect()
    if not candidate:
        raise ValueError("No drift metrics available yet. Run ingestion and transformation first.")

    selected_trip_id = candidate[0]["trip_id"]
    selected_stop_id = candidate[0]["stop_id"]

print(f"Analyzing stop_id={selected_stop_id}, trip_id={selected_trip_id}")

# COMMAND ----------

import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

trip_pdf = (
    raw_df.where(col("trip_id") == selected_trip_id)
    .where(col("stop_id") == selected_stop_id)
    .where(col("estimatedDeparture").isNotNull())
    .select("ingestion_timestamp", "estimatedDeparture", "scheduledDeparture", "minutesTillDeparture", "route_short_name")
    .orderBy("ingestion_timestamp")
    .toPandas()
)

if trip_pdf.empty:
    raise ValueError(f"No raw snapshots found for stop_id={selected_stop_id}, trip_id={selected_trip_id}")

trip_pdf["ingestion_timestamp"] = pd.to_datetime(trip_pdf["ingestion_timestamp"])
trip_pdf["estimatedDeparture"] = pd.to_datetime(trip_pdf["estimatedDeparture"])
trip_pdf["scheduledDeparture"] = pd.to_datetime(trip_pdf["scheduledDeparture"])
trip_pdf["time_before_departure_minutes"] = (
    trip_pdf["estimatedDeparture"] - trip_pdf["ingestion_timestamp"]
).dt.total_seconds() / 60.0

# Deduplicate by ingestion_timestamp
trip_pdf = trip_pdf.drop_duplicates(subset=["ingestion_timestamp"], keep="first").reset_index(drop=True)

route_name = trip_pdf["route_short_name"].iloc[0] if "route_short_name" in trip_pdf.columns and pd.notna(trip_pdf["route_short_name"].iloc[0]) else "Unknown"
scheduled = trip_pdf["scheduledDeparture"].iloc[0]

# --- Chart 1: ETA Drift Timeline ---
ACCENT = "#6366f1"    # indigo
SCHED = "#10b981"     # emerald
DRIFT_COLOR = "#ef4444"  # red
BG = "#fafbfc"
GRID = "#e5e7eb"

fig, ax = plt.subplots(figsize=(13, 6), dpi=120)
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)

x = trip_pdf["time_before_departure_minutes"].values
y = trip_pdf["estimatedDeparture"]

# Fill area between scheduled and estimated
scheduled_series = pd.Series([scheduled] * len(y))
ax.fill_between(x, scheduled_series, y, alpha=0.12, color=DRIFT_COLOR, label="_nolegend_")

# Main line + markers
ax.plot(x, y, color=ACCENT, linewidth=2.5, zorder=4, label="Estimated departure")
ax.scatter(x, y, color=ACCENT, s=45, zorder=5, edgecolors="white", linewidth=1.2)

# Scheduled departure reference
ax.axhline(y=scheduled, color=SCHED, linestyle="--", linewidth=2, alpha=0.8, label=f"Scheduled ({scheduled.strftime('%H:%M')})")

# Annotate max drift
max_idx = trip_pdf["estimatedDeparture"].idxmax()
max_est = trip_pdf.loc[max_idx, "estimatedDeparture"]
max_x = trip_pdf.loc[max_idx, "time_before_departure_minutes"]
max_drift_min = (max_est - scheduled).total_seconds() / 60
if max_drift_min > 0.5:
    ax.annotate(
        f"  +{max_drift_min:.1f} min",
        xy=(max_x, max_est),
        fontsize=13, color=DRIFT_COLOR, fontweight="bold",
        ha="left", va="bottom",
    )

# Styling
ax.invert_xaxis()
ax.yaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
ax.set_xlabel("Minutes before estimated departure", fontsize=13, fontweight="medium", labelpad=12)
ax.set_ylabel("Estimated departure time", fontsize=13, fontweight="medium", labelpad=12)
ax.set_title(
    f"ETA Drift Over Time — Route {route_name}, Stop {selected_stop_id}",
    fontsize=17, fontweight="bold", pad=18, color="#1f2937",
)
ax.tick_params(labelsize=11, colors="#4b5563")
for spine in ax.spines.values():
    spine.set_visible(False)
ax.grid(True, alpha=0.5, color=GRID, linewidth=0.8)
ax.legend(loc="upper left", fontsize=11, frameon=True, facecolor="white", edgecolor=GRID, framealpha=0.95)

# Subtitle
fig.text(0.5, -0.01,
    f"Each point is a real-time ETA snapshot taken at 2-minute intervals  ·  {len(trip_pdf)} observations",
    ha="center", fontsize=10, color="#6b7280", style="italic")

plt.tight_layout()
plt.show()

# COMMAND ----------

# --- Chart 2: Distribution of ETA Drift ---
ACCENT = "#6366f1"
BG = "#fafbfc"
GRID = "#e5e7eb"

drift_pdf = (
    metrics_df.where(col("drift_minutes").isNotNull())
    .select("drift_minutes")
    .toPandas()
)

if drift_pdf.empty:
    raise ValueError("No non-null drift_minutes values available.")

median_drift = drift_pdf["drift_minutes"].median()
mean_drift = drift_pdf["drift_minutes"].mean()

fig, ax = plt.subplots(figsize=(13, 6), dpi=120)
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)

# Histogram with gradient-like coloring
n, bins, patches = ax.hist(
    drift_pdf["drift_minutes"], bins=25,
    edgecolor="white", linewidth=1.2, alpha=0.85, color=ACCENT,
)

# Color bars by value (gradient effect)
norm = plt.Normalize(bins.min(), bins.max())
cmap = LinearSegmentedColormap.from_list("", ["#818cf8", "#4f46e5", "#312e81"])
for patch, left_edge in zip(patches, bins[:-1]):
    patch.set_facecolor(cmap(norm(left_edge)))

# Median + mean lines
ax.axvline(median_drift, color="#f59e0b", linewidth=2.5, linestyle="-", label=f"Median: {median_drift:.1f} min", zorder=6)
ax.axvline(mean_drift, color="#ef4444", linewidth=2.5, linestyle="--", label=f"Mean: {mean_drift:.1f} min", zorder=6)

# Styling
ax.set_xlabel("ETA Drift (minutes)", fontsize=13, fontweight="medium", labelpad=12)
ax.set_ylabel("Number of trips", fontsize=13, fontweight="medium", labelpad=12)
ax.set_title(
    "Distribution of ETA Drift Across All Observed Trips",
    fontsize=17, fontweight="bold", pad=18, color="#1f2937",
)
ax.tick_params(labelsize=11, colors="#4b5563")
for spine in ax.spines.values():
    spine.set_visible(False)
ax.grid(True, axis="y", alpha=0.5, color=GRID, linewidth=0.8)
ax.legend(loc="upper right", fontsize=12, frameon=True, facecolor="white", edgecolor=GRID, framealpha=0.95)

fig.text(0.5, -0.01,
    f"Based on {len(drift_pdf)} trip observations  ·  Higher drift = less reliable ETA",
    ha="center", fontsize=10, color="#6b7280", style="italic")

plt.tight_layout()
plt.show()

# COMMAND ----------

# --- Chart 3: Route Ranking by Avg Drift (Student Routes Only) ---
import re
from pyspark.sql.functions import udf, avg as spark_avg
from pyspark.sql.types import StringType

BG = "#fafbfc"
GRID = "#e5e7eb"

# Map route number prefix → display name + color
STUDENT_ROUTES = {
    "1":  {"name": "1 YELLOW",  "color": "#facc15"},
    "5":  {"name": "5 GREEN",   "color": "#22c55e"},
    "10": {"name": "10 GOLD",   "color": "#f59e0b"},
    "12": {"name": "12 TEAL",   "color": "#14b8a6"},
    "13": {"name": "13 SILVER", "color": "#94a3b8"},
    "22": {"name": "22 ILLINI", "color": "#f97316"},
}

def extract_route_number(name):
    if name is None:
        return None
    m = re.match(r"^(\d+)", name)
    return m.group(1) if m else None

extract_udf = udf(extract_route_number, StringType())

# Extract route number, filter to student routes, group
route_nums = list(STUDENT_ROUTES.keys())
routes_pdf = (
    metrics_df
    .withColumn("route_num", extract_udf(col("route_short_name")))
    .where(col("route_num").isin(route_nums))
    .groupBy("route_num")
    .agg(spark_avg("drift_minutes").alias("avg_drift_minutes"))
    .toPandas()
)

# Add display names and sort
routes_pdf["avg_drift_minutes"] = routes_pdf["avg_drift_minutes"].astype(float)
routes_pdf["display_name"] = routes_pdf["route_num"].map(lambda x: STUDENT_ROUTES[x]["name"])
routes_pdf["bar_color"] = routes_pdf["route_num"].map(lambda x: STUDENT_ROUTES[x]["color"])
routes_pdf = routes_pdf.sort_values("avg_drift_minutes", ascending=True)

if routes_pdf.empty:
    print("No student route data yet — collect more snapshots.")
else:
    fig, ax = plt.subplots(figsize=(13, max(4, len(routes_pdf) * 0.8)), dpi=120)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    bars = ax.barh(
        routes_pdf["display_name"], routes_pdf["avg_drift_minutes"],
        color=routes_pdf["bar_color"], edgecolor="white", linewidth=1.2, height=0.6,
    )

    for bar, val in zip(bars, routes_pdf["avg_drift_minutes"]):
        ax.text(
            bar.get_width() + 0.15, bar.get_y() + bar.get_height() / 2,
            f"{val:.1f} min",
            va="center", ha="left", fontsize=12, color="#4b5563", fontweight="bold",
        )

    ax.set_xlabel("Average ETA Drift (minutes)", fontsize=13, fontweight="medium", labelpad=12)
    ax.set_title(
        "ETA Reliability of Popular CUMTD Student Routes",
        fontsize=17, fontweight="bold", pad=18, color="#1f2937",
    )
    ax.tick_params(labelsize=12, colors="#4b5563")
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.grid(True, axis="x", alpha=0.5, color=GRID, linewidth=0.8)
    ax.set_xlim(0, routes_pdf["avg_drift_minutes"].max() * 1.3)

    # Note which routes are missing
    found = set(routes_pdf["route_num"].tolist())
    missing = [STUDENT_ROUTES[k]["name"] for k in route_nums if k not in found]
    subtitle = f"Bars use each route's official color  ·  Based on real-time snapshots at Illinois Terminal"
    if missing:
        subtitle += f"\n{', '.join(missing)} — not yet observed (need more collection time)"

    fig.text(0.5, -0.02, subtitle, ha="center", fontsize=10, color="#6b7280", style="italic")

    plt.tight_layout()
    plt.show()
