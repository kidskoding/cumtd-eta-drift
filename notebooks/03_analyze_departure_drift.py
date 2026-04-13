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

# --- Chart 3: Route Ranking by Avg Drift ---
BG = "#fafbfc"
GRID = "#e5e7eb"

top_routes_pdf = (
    metrics_df.groupBy("route_short_name")
    .avg("drift_minutes")
    .withColumnRenamed("avg(drift_minutes)", "avg_drift_minutes")
    .orderBy("avg_drift_minutes")
    .toPandas()
)

if top_routes_pdf.empty:
    raise ValueError("No route drift data available.")

# Sort for horizontal bar (top routes at top)
top_routes_pdf = top_routes_pdf.sort_values("avg_drift_minutes", ascending=True)

fig, ax = plt.subplots(figsize=(13, max(6, len(top_routes_pdf) * 0.4)), dpi=120)
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)

# Color gradient: low drift = green, high drift = red
values = top_routes_pdf["avg_drift_minutes"].values
norm = plt.Normalize(values.min(), values.max())
cmap = LinearSegmentedColormap.from_list("", ["#10b981", "#f59e0b", "#ef4444"])
colors = [cmap(norm(v)) for v in values]

bars = ax.barh(
    top_routes_pdf["route_short_name"], values,
    color=colors, edgecolor="white", linewidth=0.8, height=0.7,
)

# Value labels on bars
for bar, val in zip(bars, values):
    ax.text(
        bar.get_width() + 0.1, bar.get_y() + bar.get_height() / 2,
        f"{val:.1f} min",
        va="center", ha="left", fontsize=10, color="#4b5563", fontweight="medium",
    )

# Styling
ax.set_xlabel("Average ETA Drift (minutes)", fontsize=13, fontweight="medium", labelpad=12)
ax.set_title(
    "Which CUMTD Routes Have the Most Unreliable ETAs?",
    fontsize=17, fontweight="bold", pad=18, color="#1f2937",
)
ax.tick_params(labelsize=11, colors="#4b5563")
for spine in ax.spines.values():
    spine.set_visible(False)
ax.grid(True, axis="x", alpha=0.5, color=GRID, linewidth=0.8)

# Extend x-axis slightly for labels
ax.set_xlim(0, values.max() * 1.2)

fig.text(0.5, -0.01,
    f"Green = reliable  ·  Red = frequently delayed  ·  Based on {metrics_df.count()} trip observations",
    ha="center", fontsize=10, color="#6b7280", style="italic")

plt.tight_layout()
plt.show()
