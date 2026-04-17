# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # 03 - Explore ETA Drift
# MAGIC
# MAGIC Exploratory data analysis for the CUMTD ETA drift dataset.
# MAGIC
# MAGIC This notebook answers the basic data questions before product/dashboard work:
# MAGIC
# MAGIC - How many routes, stops, trips, and snapshots did we observe?
# MAGIC - What does the ETA drift distribution look like?
# MAGIC - Which routes and stops have the highest observed drift?
# MAGIC - What does one repeated trip snapshot sequence look like?

# COMMAND ----------

from __future__ import annotations

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from pyspark.sql.functions import (
    avg as spark_avg,
    col,
    count as spark_count,
    desc,
    expr,
    percentile_approx,
)

try:
    from eta_analysis_utils import COLORS, add_bar_labels, kpi_cards_html, stop_label, style_axis
except ModuleNotFoundError:
    from notebooks.eta_analysis_utils import COLORS, add_bar_labels, kpi_cards_html, stop_label, style_axis

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cumtd_eta_drift")
dbutils.widgets.text("top_n", "15")
dbutils.widgets.text("min_trips", "2")
dbutils.widgets.text("featured_route", "")

catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema").strip()
top_n = int(dbutils.widgets.get("top_n"))
min_trips = int(dbutils.widgets.get("min_trips"))
featured_route = dbutils.widgets.get("featured_route").strip()

database_name = f"{catalog}.{schema_name}" if catalog else schema_name
raw_table = f"{database_name}.raw_departure_snapshots"
metrics_table = f"{database_name}.departure_drift_metrics"

raw_df = spark.table(raw_table)
metrics_df = spark.table(metrics_table)

if metrics_df.count() == 0:
    raise ValueError("No drift metrics found. Run ingestion and dbt before EDA.")

print(f"Raw table: {raw_table}")
print(f"Metrics table: {metrics_table}")

# COMMAND ----------

overall = metrics_df.agg(
    spark_count("*").alias("observed_trip_metrics"),
    expr("count(distinct route_short_name)").alias("observed_routes"),
    expr("count(distinct stop_id)").alias("observed_stops"),
    percentile_approx("drift_minutes", 0.5).alias("median_drift"),
    percentile_approx("drift_minutes", 0.9).alias("p90_drift"),
).first()

raw_counts = raw_df.agg(
    spark_count("*").alias("raw_snapshot_rows"),
    expr("count(distinct run_id)").alias("ingestion_runs"),
).first()

displayHTML(
    kpi_cards_html(
        [
            ("Raw Snapshot Rows", f"{raw_counts['raw_snapshot_rows']:,}", "API departure observations"),
            ("Observed Trip Metrics", f"{overall['observed_trip_metrics']:,}", "stop/trip groups with repeated snapshots"),
            ("Observed Routes", f"{overall['observed_routes']:,}", "routes present in collected stop feeds"),
            ("P90 ETA Drift", f"{overall['p90_drift']:.1f} min", "high-end prediction movement"),
        ]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drift Distribution
# MAGIC
# MAGIC The key EDA metric is `drift_minutes`: for the same stop/trip, how far apart were the
# MAGIC earliest and latest estimated departure times?

# COMMAND ----------

drift_pdf = metrics_df.where(col("drift_minutes").isNotNull()).select("drift_minutes").toPandas()

if drift_pdf.empty:
    print("No drift values available.")
else:
    drift_pdf["drift_minutes"] = drift_pdf["drift_minutes"].astype(float)
    median_drift = drift_pdf["drift_minutes"].median()
    p90_drift = drift_pdf["drift_minutes"].quantile(0.9)

    fig, ax = plt.subplots(figsize=(12, 5.5), dpi=140)
    fig.patch.set_facecolor(COLORS["bg"])
    ax.hist(
        drift_pdf["drift_minutes"],
        bins=30,
        color=COLORS["blue"],
        alpha=0.78,
        edgecolor="white",
        linewidth=1,
    )
    ax.axvline(median_drift, color=COLORS["amber"], linewidth=2.2)
    ax.axvline(p90_drift, color=COLORS["red"], linewidth=2.2, linestyle="--")
    ymax = ax.get_ylim()[1]
    ax.text(median_drift + 0.15, ymax * 0.90, f"median {median_drift:.1f} min", color=COLORS["amber"], fontweight="bold")
    ax.text(p90_drift + 0.15, ymax * 0.78, f"p90 {p90_drift:.1f} min", color=COLORS["red"], fontweight="bold")
    ax.set_title("Distribution of ETA Drift", fontsize=20, fontweight="bold", color=COLORS["ink"], loc="left", pad=18)
    ax.set_xlabel("ETA drift for one stop/trip (minutes)", fontsize=12, color=COLORS["muted"], labelpad=10)
    ax.set_ylabel("Observed trip count", fontsize=12, color=COLORS["muted"], labelpad=10)
    style_axis(ax, "y")
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Route-Level EDA
# MAGIC
# MAGIC This table and chart are descriptive. They only cover routes present in the collected stop feeds.

# COMMAND ----------

route_eda_df = (
    metrics_df.where(col("route_short_name").isNotNull())
    .groupBy("route_short_name")
    .agg(
        spark_count("*").alias("observed_trips"),
        spark_avg("drift_minutes").alias("avg_drift_minutes"),
        percentile_approx("drift_minutes", 0.5).alias("p50_drift_minutes"),
        percentile_approx("drift_minutes", 0.9).alias("p90_drift_minutes"),
        spark_avg("update_count").alias("avg_update_count"),
    )
    .where(col("observed_trips") >= min_trips)
    .orderBy(desc("p90_drift_minutes"))
)

display(route_eda_df)

route_pdf = route_eda_df.limit(top_n).toPandas()
if not route_pdf.empty:
    route_pdf["p90_drift_minutes"] = route_pdf["p90_drift_minutes"].astype(float)
    route_pdf = route_pdf.sort_values("p90_drift_minutes", ascending=True)

    fig, ax = plt.subplots(figsize=(12, max(4, len(route_pdf) * 0.48)), dpi=140)
    fig.patch.set_facecolor(COLORS["bg"])
    ax.barh(route_pdf["route_short_name"], route_pdf["p90_drift_minutes"], color=COLORS["purple"], height=0.62)
    add_bar_labels(ax, route_pdf["p90_drift_minutes"].tolist())
    ax.set_title("Highest-Drift Observed Routes", fontsize=20, fontweight="bold", color=COLORS["ink"], loc="left", pad=18)
    ax.set_xlabel("P90 ETA drift (minutes)", fontsize=12, color=COLORS["muted"], labelpad=10)
    style_axis(ax, "x")
    ax.set_xlim(0, route_pdf["p90_drift_minutes"].max() * 1.25)
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop-Level EDA

# COMMAND ----------

stop_eda_df = (
    metrics_df.where(col("stop_id").isNotNull())
    .groupBy("stop_id", "stop_display_name", "stop_name")
    .agg(
        spark_count("*").alias("observed_trips"),
        spark_avg("drift_minutes").alias("avg_drift_minutes"),
        percentile_approx("drift_minutes", 0.9).alias("p90_drift_minutes"),
    )
    .where(col("observed_trips") >= min_trips)
    .orderBy(desc("p90_drift_minutes"))
)

display(stop_eda_df)

stop_pdf = stop_eda_df.limit(top_n).toPandas()
if not stop_pdf.empty:
    stop_pdf["label"] = stop_pdf.apply(stop_label, axis=1)
    stop_pdf["p90_drift_minutes"] = stop_pdf["p90_drift_minutes"].astype(float)
    stop_pdf = stop_pdf.sort_values("p90_drift_minutes", ascending=True)

    fig, ax = plt.subplots(figsize=(12, max(4, len(stop_pdf) * 0.55)), dpi=140)
    fig.patch.set_facecolor(COLORS["bg"])
    ax.barh(stop_pdf["label"], stop_pdf["p90_drift_minutes"], color=COLORS["blue"], height=0.62)
    add_bar_labels(ax, stop_pdf["p90_drift_minutes"].tolist())
    ax.set_title("Highest-Drift Observed Stops", fontsize=20, fontweight="bold", color=COLORS["ink"], loc="left", pad=18)
    ax.set_xlabel("P90 ETA drift (minutes)", fontsize=12, color=COLORS["muted"], labelpad=10)
    style_axis(ax, "x")
    ax.set_xlim(0, stop_pdf["p90_drift_minutes"].max() * 1.25)
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## One Bus, Many ETAs
# MAGIC
# MAGIC This is the core phenomenon: the same stop/trip can have many estimated departure times.
# MAGIC The final observed ETA is a feed reference point, not confirmed actual departure.

# COMMAND ----------

candidate_df = metrics_df.where(col("update_count") >= 5)
if featured_route:
    candidate_df = candidate_df.where(col("route_short_name").contains(featured_route))

featured = candidate_df.orderBy(desc("drift_minutes"), desc("update_count")).first()
if not featured:
    featured = metrics_df.orderBy(desc("drift_minutes"), desc("update_count")).first()

featured_route_name = featured["route_short_name"] or "Unknown route"
featured_stop_name = featured["stop_display_name"] or featured["stop_name"] or featured["stop_id"]

trip_pdf = (
    raw_df.where(col("trip_id") == featured["trip_id"])
    .where(col("stop_id") == featured["stop_id"])
    .where(col("estimatedDeparture").isNotNull())
    .select("ingestion_timestamp", "estimatedDeparture", "scheduledDeparture", "minutesTillDeparture")
    .orderBy("ingestion_timestamp")
    .toPandas()
)

if trip_pdf.empty:
    print("No raw snapshots found for the selected featured trip.")
else:
    trip_pdf["ingestion_timestamp"] = pd.to_datetime(trip_pdf["ingestion_timestamp"])
    trip_pdf["estimatedDeparture"] = pd.to_datetime(trip_pdf["estimatedDeparture"])
    trip_pdf["scheduledDeparture"] = pd.to_datetime(trip_pdf["scheduledDeparture"])
    trip_pdf = trip_pdf.drop_duplicates(subset=["ingestion_timestamp"], keep="first")
    trip_pdf["offset_from_schedule_minutes"] = (
        trip_pdf["estimatedDeparture"] - trip_pdf["scheduledDeparture"]
    ).dt.total_seconds() / 60.0
    trip_pdf["time_before_eta_minutes"] = (
        trip_pdf["estimatedDeparture"] - trip_pdf["ingestion_timestamp"]
    ).dt.total_seconds() / 60.0

    final_observed_eta = trip_pdf["estimatedDeparture"].iloc[-1]
    final_observed_offset = trip_pdf["offset_from_schedule_minutes"].iloc[-1]

    fig, ax = plt.subplots(figsize=(13, 6), dpi=140)
    fig.patch.set_facecolor(COLORS["bg"])
    ax.plot(
        trip_pdf["time_before_eta_minutes"],
        trip_pdf["offset_from_schedule_minutes"],
        color=COLORS["purple"],
        linewidth=2.6,
        marker="o",
        markersize=4,
    )
    ax.axhline(0, color=COLORS["amber"], linestyle="--", linewidth=1.8)
    ax.axhline(final_observed_offset, color=COLORS["red"], linestyle="-", linewidth=1.8, alpha=0.75)
    ax.invert_xaxis()
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%+.0f"))
    ax.set_title(
        f"ETA Drift Lifecycle: {featured_route_name} at {featured_stop_name}",
        fontsize=19,
        fontweight="bold",
        color=COLORS["ink"],
        loc="left",
        pad=18,
    )
    ax.text(
        0,
        1.04,
        f"{len(trip_pdf)} snapshots. Final observed ETA: {final_observed_eta.strftime('%I:%M %p')} "
        f"(reference only).",
        transform=ax.transAxes,
        fontsize=12,
        color=COLORS["muted"],
    )
    ax.set_xlabel("Minutes before current estimated departure", fontsize=12, color=COLORS["muted"], labelpad=10)
    ax.set_ylabel("Estimated departure offset from schedule (minutes)", fontsize=12, color=COLORS["muted"], labelpad=10)
    style_axis(ax, "y")
    plt.tight_layout()
    plt.show()

    display(trip_pdf)
