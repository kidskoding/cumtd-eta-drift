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
# Pick the highest-drift 12E TEAL trip with enough data points
top = (
    metrics_df
    .where(col("route_short_name").like("%12E TEAL%"))
    .where(col("update_count") >= 10)
    .orderBy(desc("drift_minutes"), desc("update_count"))
    .first()
)

if not top:
    raise ValueError("No 12E TEAL trips found with enough snapshots.")

route_label = top["route_short_name"]
stop_label = top["stop_name"] or top["stop_id"]
print(f"Selected: Route {route_label} at {stop_label}")
print(f"  Drift: {top['drift_minutes']:.1f} min  |  Snapshots: {top['update_count']}")

# COMMAND ----------

# DBTITLE 1,Cell 6
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import pandas as pd
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

# ── Fetch & prep data for the single selected trip ──────────────────────
trip_pdf = (
    raw_df.where(col("trip_id") == top["trip_id"])
    .where(col("stop_id") == top["stop_id"])
    .where(col("estimatedDeparture").isNotNull())
    .select("ingestion_timestamp", "estimatedDeparture", "scheduledDeparture")
    .orderBy("ingestion_timestamp")
    .toPandas()
)

trip_pdf["ingestion_timestamp"] = pd.to_datetime(trip_pdf["ingestion_timestamp"])
trip_pdf["estimatedDeparture"] = pd.to_datetime(trip_pdf["estimatedDeparture"])
trip_pdf["scheduledDeparture"] = pd.to_datetime(trip_pdf["scheduledDeparture"])
trip_pdf = trip_pdf.drop_duplicates(subset=["ingestion_timestamp"], keep="first").reset_index(drop=True)

scheduled_time = trip_pdf["scheduledDeparture"].iloc[0]

# "Actual" departure = last ETA (closest to ground truth)
actual_departure = trip_pdf["estimatedDeparture"].iloc[-1]
actual_drift_min = (actual_departure - scheduled_time).total_seconds() / 60.0

# Each snapshot's predicted departure converted to drift-from-schedule
trip_pdf["predicted_drift"] = (
    trip_pdf["estimatedDeparture"] - trip_pdf["scheduledDeparture"]
).dt.total_seconds() / 60.0

# Prediction error at each snapshot (how far off the prediction was from actual)
trip_pdf["prediction_error"] = trip_pdf["predicted_drift"] - actual_drift_min

# ── Chart 1 — Predicted vs Actual Departure ───────────────────────────
ACCENT = "#6366f1"
ACTUAL_COLOR = "#ef4444"
SCHED_COLOR = "#f59e0b"
ERROR_COLOR = "#fca5a5"
BG = "#ffffff"

fig, ax = plt.subplots(figsize=(14, 7), dpi=140)
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)

x = trip_pdf["ingestion_timestamp"]
y_pred = trip_pdf["predicted_drift"]

# Shaded error region between prediction and actual
ax.fill_between(x, actual_drift_min, y_pred, alpha=0.10, color=ACTUAL_COLOR,
                label="_nolegend_", zorder=2)

# Actual departure line (ground truth)
ax.axhline(y=actual_drift_min, color=ACTUAL_COLOR, linewidth=2, linestyle="-",
           zorder=3, alpha=0.85)
ax.text(x.iloc[0] + pd.Timedelta(minutes=1), actual_drift_min + 0.4,
        f"Actual: +{actual_drift_min:.0f} min late  ({actual_departure.strftime('%I:%M %p')})",
        fontsize=10.5, fontweight="bold", color=ACTUAL_COLOR, va="bottom")

# Scheduled baseline
ax.axhline(y=0, color=SCHED_COLOR, linewidth=1.5, linestyle="--", zorder=1, alpha=0.6)
ax.text(x.iloc[0] + pd.Timedelta(minutes=1), 0.4,
        f"Scheduled: {scheduled_time.strftime('%I:%M %p')}",
        fontsize=10, fontweight="bold", color=SCHED_COLOR, va="bottom", alpha=0.8)

# Predicted ETA line
ax.plot(x, y_pred, color=ACCENT, linewidth=2.5, solid_capstyle="round", zorder=4,
        label="Predicted drift")
ax.scatter(x, y_pred, color=ACCENT, s=18, zorder=5, edgecolors="white", linewidth=0.6)

# Convergence annotation — mark where prediction first gets within 1 min of actual
close_mask = (y_pred - actual_drift_min).abs() <= 1.0
if close_mask.any():
    converge_idx = close_mask.idxmax()
    converge_x = x.iloc[converge_idx]
    converge_y = y_pred.iloc[converge_idx]
    mins_before = (actual_departure - converge_x).total_seconds() / 60.0
    if mins_before > 2:
        ax.annotate(
            f"Converges {mins_before:.0f} min before",
            xy=(converge_x, converge_y),
            xytext=(converge_x - pd.Timedelta(minutes=12), converge_y + 2),
            fontsize=10, fontweight="bold", color="#059669",
            arrowprops=dict(arrowstyle="->", color="#059669", lw=1.5,
                            connectionstyle="arc3,rad=0.2"),
            zorder=6,
        )

# Formatting
ax.xaxis.set_major_formatter(mdates.DateFormatter("%I:%M %p"))
ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=10))
fig.autofmt_xdate(rotation=0, ha="center")

ax.set_xlabel("Time of observation", fontsize=12, color="#6b7280", labelpad=10)
ax.set_ylabel("Drift from schedule (min)", fontsize=12, color="#6b7280", labelpad=10)
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%+.0f"))

fig.suptitle(
    f"Route {route_label}  \u00b7  {stop_label}",
    fontsize=20, fontweight="bold", color="#111827", x=0.12, ha="left", y=0.98,
)
fig.text(
    0.12, 0.92,
    f"Predicted ETA vs actual departure  \u00b7  {len(trip_pdf)} snapshots  \u00b7  Shaded area = prediction error",
    fontsize=12, color="#9ca3af", ha="left", va="top",
)

ax.tick_params(labelsize=10, colors="#6b7280", length=0)
y_max = max(y_pred.max(), actual_drift_min)
ax.set_ylim(bottom=min(-2, y_pred.min() - 1), top=y_max + 3)
for spine in ax.spines.values():
    spine.set_visible(False)
ax.grid(axis="y", alpha=0.3, color="#e5e7eb", linewidth=0.8)

plt.tight_layout(rect=[0, 0, 1, 0.90])
plt.show()

# COMMAND ----------

# DBTITLE 1,Stop-level drift comparison
# ── Chart 2: Average Drift by Stop ──────────────────────────────────────
from pyspark.sql.functions import avg as spark_avg, count as spark_count

stops_pdf = (
    metrics_df
    .where(col("stop_name").isNotNull())
    .groupBy("stop_id", "stop_name")
    .agg(
        spark_avg("drift_minutes").alias("avg_drift"),
        spark_count("*").alias("trips"),
    )
    .where(col("trips") >= 2)
    .orderBy(desc("avg_drift"))
    .limit(top_n)
    .toPandas()
)

stops_pdf["avg_drift"] = stops_pdf["avg_drift"].astype(float)
stops_pdf = stops_pdf.sort_values("avg_drift", ascending=True)
stops_pdf["label"] = stops_pdf["stop_name"].fillna(stops_pdf["stop_id"])

ACCENT = "#6366f1"
BG = "#ffffff"

if stops_pdf.empty:
    print("No stop data yet.")
else:
    n_stops = len(stops_pdf)
    fig, ax = plt.subplots(figsize=(12, max(3.5, n_stops * 0.5)), dpi=140)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    # Single-colour bars with subtle gradient via alpha
    alphas = np.linspace(0.4, 1.0, n_stops)
    for idx, (label, val, trips, alpha) in enumerate(
        zip(stops_pdf["label"], stops_pdf["avg_drift"], stops_pdf["trips"], alphas)
    ):
        ax.barh(idx, val, color=ACCENT, alpha=alpha, height=0.6, edgecolor="white", linewidth=0.8)
        ax.text(val + 0.2, idx, f"{val:.1f} min", va="center", fontsize=10, color="#374151", fontweight="bold")
        ax.text(val + 0.2, idx - 0.22, f"{int(trips)} trips", va="center", fontsize=8.5, color="#9ca3af")

    ax.set_yticks(range(n_stops))
    ax.set_yticklabels(stops_pdf["label"], fontsize=10, color="#374151")
    ax.set_xlabel("")

    ax.set_title("Average ETA Drift by Stop", fontsize=20, fontweight="bold", color="#111827", pad=20, loc="left")
    ax.text(0.0, 1.06, "Stops with ≥ 2 observed trips  ·  All routes combined",
            transform=ax.transAxes, fontsize=12, color="#9ca3af", va="bottom")

    ax.tick_params(left=False, bottom=False, labelsize=10, colors="#6b7280")
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.grid(axis="x", alpha=0.25, color="#e5e7eb", linewidth=0.8)
    ax.set_xlim(0, stops_pdf["avg_drift"].max() * 1.3)

    plt.tight_layout()
    plt.show()

# COMMAND ----------

# ── Chart 3: Drift Distribution ────────────────────────────────────────
ACCENT = "#6366f1"
BG = "#ffffff"

drift_pdf = (
    metrics_df.where(col("drift_minutes").isNotNull())
    .select("drift_minutes")
    .toPandas()
)

if drift_pdf.empty:
    raise ValueError("No drift data available.")

med = drift_pdf["drift_minutes"].median()
avg = drift_pdf["drift_minutes"].mean()

fig, ax = plt.subplots(figsize=(12, 5), dpi=140)
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)

n, bins, patches = ax.hist(
    drift_pdf["drift_minutes"], bins=30,
    color=ACCENT, alpha=0.75, edgecolor="white", linewidth=1,
)

# Median & mean
ax.axvline(med, color="#f59e0b", linewidth=2, linestyle="-", zorder=5)
ax.axvline(avg, color="#ef4444", linewidth=2, linestyle="--", zorder=5)

# Annotations instead of legend
ax.text(med + 0.3, ax.get_ylim()[1] * 0.92, f"Median {med:.1f} min",
        fontsize=10, fontweight="bold", color="#f59e0b")
ax.text(avg + 0.3, ax.get_ylim()[1] * 0.82, f"Mean {avg:.1f} min",
        fontsize=10, fontweight="bold", color="#ef4444")

ax.set_xlabel("ETA drift (minutes)", fontsize=12, color="#6b7280", labelpad=10)
ax.set_ylabel("Number of trips", fontsize=12, color="#6b7280", labelpad=10)

ax.set_title("Distribution of ETA Drift", fontsize=20, fontweight="bold", color="#111827", pad=20, loc="left")
ax.text(0.0, 1.06, f"{len(drift_pdf)} trip observations  ·  Higher drift = less reliable ETA",
        transform=ax.transAxes, fontsize=12, color="#9ca3af", va="bottom")

ax.tick_params(labelsize=10, colors="#6b7280", length=0)
for spine in ax.spines.values():
    spine.set_visible(False)
ax.grid(axis="y", alpha=0.25, color="#e5e7eb", linewidth=0.8)

plt.tight_layout()
plt.show()

# COMMAND ----------

# ── Chart 4: Route Ranking by Average Drift ────────────────────────────
from pyspark.sql.functions import avg as spark_avg, count as spark_count

ACCENT = "#6366f1"
BG = "#ffffff"

routes_pdf = (
    metrics_df
    .where(col("route_short_name").isNotNull())
    .groupBy("route_short_name")
    .agg(
        spark_avg("drift_minutes").alias("avg_drift"),
        spark_count("*").alias("trips"),
    )
    .where(col("trips") >= 2)
    .orderBy(desc("avg_drift"))
    .limit(top_n)
    .toPandas()
)

routes_pdf["avg_drift"] = routes_pdf["avg_drift"].astype(float)
routes_pdf = routes_pdf.sort_values("avg_drift", ascending=True)

if routes_pdf.empty:
    print("No route data yet.")
else:
    n_routes = len(routes_pdf)
    fig, ax = plt.subplots(figsize=(12, max(3.5, n_routes * 0.5)), dpi=140)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    alphas = np.linspace(0.4, 1.0, n_routes)
    for idx, (name, val, trips, alpha) in enumerate(
        zip(routes_pdf["route_short_name"], routes_pdf["avg_drift"], routes_pdf["trips"], alphas)
    ):
        ax.barh(idx, val, color=ACCENT, alpha=alpha, height=0.6, edgecolor="white", linewidth=0.8)
        ax.text(val + 0.2, idx, f"{val:.1f} min", va="center", fontsize=10, color="#374151", fontweight="bold")
        ax.text(val + 0.2, idx - 0.22, f"{int(trips)} trips", va="center", fontsize=8.5, color="#9ca3af")

    ax.set_yticks(range(n_routes))
    ax.set_yticklabels(routes_pdf["route_short_name"], fontsize=10, color="#374151")
    ax.set_xlabel("")

    ax.set_title("Average ETA Drift by Route", fontsize=20, fontweight="bold", color="#111827", pad=20, loc="left")
    ax.text(0.0, 1.06, "Routes with ≥ 2 observed trips  ·  Based on real-time ETA snapshots",
            transform=ax.transAxes, fontsize=12, color="#9ca3af", va="bottom")

    ax.tick_params(left=False, bottom=False, labelsize=10, colors="#6b7280")
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.grid(axis="x", alpha=0.25, color="#e5e7eb", linewidth=0.8)
    ax.set_xlim(0, routes_pdf["avg_drift"].max() * 1.3)

    plt.tight_layout()
    plt.show()

# COMMAND ----------

# DBTITLE 1,ML narrative intro
# MAGIC %md
# MAGIC ---
# MAGIC ## Next: Can We Predict Bus Delays Before They Happen?
# MAGIC
# MAGIC The charts above show that ETA drift follows recognizable patterns — buses that will be
# MAGIC late often show early warning signs in their first few minutes of tracking.
# MAGIC
# MAGIC **→ See `05_predict_departure_drift`** for a machine learning model that predicts final
# MAGIC delay using only the first 15 minutes of ETA observations, logged to MLflow.
