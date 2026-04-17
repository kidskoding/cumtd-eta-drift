# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # 03 - Analyze Departure Drift
# MAGIC
# MAGIC Visualize ETA instability across **all buses and stops** from the `raw_departure_snapshots` and `departure_drift_metrics` Delta tables.

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql.functions import col, desc

# COMMAND ----------

# DBTITLE 1,Cell 3
# Remove legacy single-stop widgets if they exist
for w in ["trip_id", "stop_id"]:
    try:
        dbutils.widgets.remove(w)
    except Exception:
        pass

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cumtd_eta_drift")
dbutils.widgets.text("top_n", "20")

catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema").strip()
top_n = int(dbutils.widgets.get("top_n"))

database_name = f"{catalog}.{schema_name}" if catalog else schema_name
raw_table = f"{database_name}.raw_departure_snapshots"
metrics_table = f"{database_name}.departure_drift_metrics"

# COMMAND ----------

raw_df = spark.table(raw_table)
metrics_df = spark.table(metrics_table)

display(metrics_df.orderBy(desc("drift_minutes"), desc("update_count")).limit(top_n))

# COMMAND ----------

# DBTITLE 1,Cell 5
# Select the top drifting trips across ALL stops and buses
TOP_CHART_TRIPS = 5

top_trips = (
    metrics_df
    .orderBy(desc("drift_minutes"), desc("update_count"))
    .limit(TOP_CHART_TRIPS)
    .select("trip_id", "stop_id", "stop_name", "route_short_name", "drift_minutes")
    .collect()
)

if not top_trips:
    raise ValueError("No drift metrics available yet. Run ingestion and transformation first.")

print(f"Top {len(top_trips)} drifting trips across all buses:")
for i, row in enumerate(top_trips, 1):
    stop_label = row['stop_name'] or row['stop_id']
    print(f"  {i}. Route {row['route_short_name']}, {stop_label} — {row['drift_minutes']:.1f} min drift")

# COMMAND ----------

# DBTITLE 1,Cell 6
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

# --- Chart 1: ETA Drift Timeline — Top Drifting Trips Across All Buses ---
COLORS = ["#6366f1", "#ef4444", "#10b981", "#f59e0b", "#8b5cf6"]
BG = "#fafbfc"
GRID = "#e5e7eb"

fig, ax = plt.subplots(figsize=(14, 7), dpi=120)
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)

total_points = 0
for i, row in enumerate(top_trips):
    trip_pdf = (
        raw_df.where(col("trip_id") == row["trip_id"])
        .where(col("stop_id") == row["stop_id"])
        .where(col("estimatedDeparture").isNotNull())
        .select("ingestion_timestamp", "estimatedDeparture", "scheduledDeparture")
        .orderBy("ingestion_timestamp")
        .toPandas()
    )

    if trip_pdf.empty:
        continue

    trip_pdf["ingestion_timestamp"] = pd.to_datetime(trip_pdf["ingestion_timestamp"])
    trip_pdf["estimatedDeparture"] = pd.to_datetime(trip_pdf["estimatedDeparture"])
    trip_pdf["scheduledDeparture"] = pd.to_datetime(trip_pdf["scheduledDeparture"])
    trip_pdf = trip_pdf.drop_duplicates(subset=["ingestion_timestamp"], keep="first")

    # Drift from scheduled in minutes
    trip_pdf["drift_from_scheduled"] = (
        trip_pdf["estimatedDeparture"] - trip_pdf["scheduledDeparture"]
    ).dt.total_seconds() / 60.0

    trip_pdf["time_before_departure"] = (
        trip_pdf["estimatedDeparture"] - trip_pdf["ingestion_timestamp"]
    ).dt.total_seconds() / 60.0

    color = COLORS[i % len(COLORS)]
    stop_label = row["stop_name"] or row["stop_id"]
    label = f"Rt {row['route_short_name']}, {stop_label}"

    ax.plot(trip_pdf["time_before_departure"], trip_pdf["drift_from_scheduled"],
            color=color, linewidth=2.2, label=label, zorder=4)
    ax.scatter(trip_pdf["time_before_departure"], trip_pdf["drift_from_scheduled"],
               color=color, s=30, zorder=5, edgecolors="white", linewidth=0.8)

    total_points += len(trip_pdf)

ax.axhline(y=0, color="#9ca3af", linestyle="--", linewidth=1.5, alpha=0.7, label="On schedule")

# Styling
ax.invert_xaxis()
ax.set_xlabel("Minutes before estimated departure", fontsize=13, fontweight="medium", labelpad=12)
ax.set_ylabel("Drift from schedule (minutes)", fontsize=13, fontweight="medium", labelpad=12)
ax.set_title(
    "ETA Drift Over Time \u2014 Top Drifting Trips Across All Buses",
    fontsize=17, fontweight="bold", pad=18, color="#1f2937",
)
ax.tick_params(labelsize=11, colors="#4b5563")
for spine in ax.spines.values():
    spine.set_visible(False)
ax.grid(True, alpha=0.5, color=GRID, linewidth=0.8)
ax.legend(loc="upper left", fontsize=10, frameon=True, facecolor="white", edgecolor=GRID, framealpha=0.95)

fig.text(0.5, -0.01,
    f"Each line = one trip's ETA snapshots at \u223c2-min intervals  \u00b7  {total_points} total observations  \u00b7  Positive = running late",
    ha="center", fontsize=10, color="#6b7280", style="italic")

plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Stop-level drift comparison
# --- Chart 2: Average Drift by Stop (All Buses) ---
from pyspark.sql.functions import avg as spark_avg, count as spark_count

BG = "#fafbfc"
GRID = "#e5e7eb"

stops_pdf = (
    metrics_df
    .where(col("stop_name").isNotNull())
    .groupBy("stop_id", "stop_name")
    .agg(
        spark_avg("drift_minutes").alias("avg_drift_minutes"),
        spark_count("*").alias("trip_count"),
    )
    .where(col("trip_count") >= 2)
    .orderBy(desc("avg_drift_minutes"))
    .limit(top_n)
    .toPandas()
)

stops_pdf["avg_drift_minutes"] = stops_pdf["avg_drift_minutes"].astype(float)
stops_pdf = stops_pdf.sort_values("avg_drift_minutes", ascending=True)
stops_pdf["label"] = stops_pdf["stop_name"].fillna(stops_pdf["stop_id"])

if stops_pdf.empty:
    print("No stop data yet \u2014 collect more snapshots.")
else:
    cmap = LinearSegmentedColormap.from_list("", ["#a5b4fc", "#6366f1", "#312e81"])
    norm = plt.Normalize(stops_pdf["avg_drift_minutes"].min(), stops_pdf["avg_drift_minutes"].max())
    colors = [cmap(norm(v)) for v in stops_pdf["avg_drift_minutes"]]

    fig, ax = plt.subplots(figsize=(13, max(4, len(stops_pdf) * 0.55)), dpi=120)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    bars = ax.barh(
        stops_pdf["label"], stops_pdf["avg_drift_minutes"],
        color=colors, edgecolor="white", linewidth=1.2, height=0.6,
    )

    for bar, val, cnt in zip(bars, stops_pdf["avg_drift_minutes"], stops_pdf["trip_count"]):
        ax.text(
            bar.get_width() + 0.15, bar.get_y() + bar.get_height() / 2,
            f"{val:.1f} min ({int(cnt)} trips)",
            va="center", ha="left", fontsize=11, color="#4b5563", fontweight="bold",
        )

    ax.set_xlabel("Average ETA Drift (minutes)", fontsize=13, fontweight="medium", labelpad=12)
    ax.set_title(
        f"Top {len(stops_pdf)} Stops by Average ETA Drift (All Buses)",
        fontsize=17, fontweight="bold", pad=18, color="#1f2937",
    )
    ax.tick_params(labelsize=12, colors="#4b5563")
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.grid(True, axis="x", alpha=0.5, color=GRID, linewidth=0.8)
    ax.set_xlim(0, stops_pdf["avg_drift_minutes"].max() * 1.35)

    fig.text(0.5, -0.02,
        f"Stops with \u2265 2 observed trips  \u00b7  Darker bars = higher drift  \u00b7  All buses included",
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

# --- Chart 3: Route Ranking by Average ETA Drift ---
from pyspark.sql.functions import avg as spark_avg, count as spark_count

BG = "#fafbfc"
GRID = "#e5e7eb"

# Aggregate drift by route across all buses, require at least 2 observations
routes_pdf = (
    metrics_df
    .where(col("route_short_name").isNotNull())
    .groupBy("route_short_name")
    .agg(
        spark_avg("drift_minutes").alias("avg_drift_minutes"),
        spark_count("*").alias("trip_count"),
    )
    .where(col("trip_count") >= 2)
    .orderBy(desc("avg_drift_minutes"))
    .limit(top_n)
    .toPandas()
)

routes_pdf["avg_drift_minutes"] = routes_pdf["avg_drift_minutes"].astype(float)
routes_pdf = routes_pdf.sort_values("avg_drift_minutes", ascending=True)

if routes_pdf.empty:
    print("No route data yet \u2014 collect more snapshots.")
else:
    # Color bars with a gradient from light to dark indigo
    cmap = LinearSegmentedColormap.from_list("", ["#a5b4fc", "#6366f1", "#312e81"])
    norm = plt.Normalize(routes_pdf["avg_drift_minutes"].min(), routes_pdf["avg_drift_minutes"].max())
    colors = [cmap(norm(v)) for v in routes_pdf["avg_drift_minutes"]]

    fig, ax = plt.subplots(figsize=(13, max(4, len(routes_pdf) * 0.55)), dpi=120)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    bars = ax.barh(
        routes_pdf["route_short_name"], routes_pdf["avg_drift_minutes"],
        color=colors, edgecolor="white", linewidth=1.2, height=0.6,
    )

    for bar, val, count in zip(bars, routes_pdf["avg_drift_minutes"], routes_pdf["trip_count"]):
        ax.text(
            bar.get_width() + 0.15, bar.get_y() + bar.get_height() / 2,
            f"{val:.1f} min ({int(count)} trips)",
            va="center", ha="left", fontsize=11, color="#4b5563", fontweight="bold",
        )

    ax.set_xlabel("Average ETA Drift (minutes)", fontsize=13, fontweight="medium", labelpad=12)
    ax.set_title(
        f"Top {len(routes_pdf)} CUMTD Routes by Average ETA Drift",
        fontsize=17, fontweight="bold", pad=18, color="#1f2937",
    )
    ax.tick_params(labelsize=12, colors="#4b5563")
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.grid(True, axis="x", alpha=0.5, color=GRID, linewidth=0.8)
    ax.set_xlim(0, routes_pdf["avg_drift_minutes"].max() * 1.35)

    fig.text(0.5, -0.02,
        f"Routes with \u2265 2 observed trips  \u00b7  Darker bars = higher drift  \u00b7  Based on real-time ETA snapshots",
        ha="center", fontsize=10, color="#6b7280", style="italic")

    plt.tight_layout()
    plt.show()
