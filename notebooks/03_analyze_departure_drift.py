# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # 03 - ETA Trust Dashboard
# MAGIC
# MAGIC Existing transit apps answer: **when is the bus predicted to come?**
# MAGIC
# MAGIC This notebook answers the missing second question: **how much should a student trust that ETA?**
# MAGIC
# MAGIC The visuals below use repeated real-time MTD API snapshots to measure ETA drift, summarize
# MAGIC route/stop reliability, and turn prediction volatility into practical student planning guidance.

# COMMAND ----------

from __future__ import annotations

import math
from typing import Iterable

import matplotlib.dates as mdates
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
    max as spark_max,
    min as spark_min,
    percentile_approx,
    round as spark_round,
)

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cumtd_eta_drift")
dbutils.widgets.text("top_n", "15")
dbutils.widgets.text("route_min_trips", "2")
dbutils.widgets.text("stop_min_trips", "2")
dbutils.widgets.text("featured_route", "")

catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema").strip()
top_n = int(dbutils.widgets.get("top_n"))
route_min_trips = int(dbutils.widgets.get("route_min_trips"))
stop_min_trips = int(dbutils.widgets.get("stop_min_trips"))
featured_route = dbutils.widgets.get("featured_route").strip()

database_name = f"{catalog}.{schema_name}" if catalog else schema_name
raw_table = f"{database_name}.raw_departure_snapshots"
metrics_table = f"{database_name}.departure_drift_metrics"

print(f"Reading raw snapshots from {raw_table}")
print(f"Reading drift metrics from {metrics_table}")

# COMMAND ----------

raw_df = spark.table(raw_table)
metrics_df = spark.table(metrics_table)

if metrics_df.count() == 0:
    raise ValueError(
        "No drift metrics found yet. Run ingestion, then dbt run/test, before this analysis notebook."
    )

# COMMAND ----------

LOW_DRIFT_MIN = 2.0
MEDIUM_DRIFT_MIN = 5.0
HIGH_DRIFT_MIN = 10.0

COLORS = {
    "ink": "#1f2937",
    "muted": "#6b7280",
    "faint": "#e5e7eb",
    "bg": "#ffffff",
    "green": "#16a34a",
    "amber": "#f59e0b",
    "red": "#ef4444",
    "blue": "#2563eb",
    "purple": "#6366f1",
}


def route_risk_label(p90_drift: float) -> str:
    if p90_drift <= LOW_DRIFT_MIN:
        return "High trust"
    if p90_drift <= MEDIUM_DRIFT_MIN:
        return "Medium trust"
    return "Low trust"


def route_risk_color(p90_drift: float) -> str:
    if p90_drift <= LOW_DRIFT_MIN:
        return COLORS["green"]
    if p90_drift <= MEDIUM_DRIFT_MIN:
        return COLORS["amber"]
    return COLORS["red"]


def trust_score_from_p90(p90_drift: float) -> int:
    # A simple, explainable score: p90 drift of 0 min -> 100, p90 drift of 10+ min -> 10.
    return int(max(10, min(100, round(100 - (p90_drift * 9)))))


def suggested_buffer_minutes(p90_drift: float) -> int:
    # Round up to a student-friendly buffer. This is not a predicted delay.
    return int(max(1, math.ceil(p90_drift)))


def stop_label(row: pd.Series | dict) -> str:
    values = row if isinstance(row, dict) else row.to_dict()
    return (
        values.get("stop_display_name")
        or values.get("stop_name")
        or values.get("stop_id")
        or "Unknown stop"
    )


def style_axis(ax, grid_axis: str = "x") -> None:
    ax.set_facecolor(COLORS["bg"])
    ax.tick_params(labelsize=10, colors=COLORS["muted"], length=0)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.grid(axis=grid_axis, alpha=0.35, color=COLORS["faint"], linewidth=0.8)


def add_bar_labels(ax, values: Iterable[float], suffix: str = " min") -> None:
    x_max = max(values) if values else 0
    for patch in ax.patches:
        width = patch.get_width()
        ax.text(
            width + max(x_max * 0.015, 0.08),
            patch.get_y() + patch.get_height() / 2,
            f"{width:.1f}{suffix}",
            va="center",
            fontsize=10,
            color=COLORS["ink"],
            fontweight="bold",
        )


def display_kpi_cards(cards: list[tuple[str, str, str]]) -> None:
    card_html = "".join(
        f"""
        <div style="border:1px solid #e5e7eb;border-radius:8px;padding:16px 18px;background:#ffffff;">
          <div style="font-size:13px;color:#6b7280;margin-bottom:8px;">{label}</div>
          <div style="font-size:30px;font-weight:800;color:#111827;line-height:1;">{value}</div>
          <div style="font-size:12px;color:#6b7280;margin-top:8px;">{note}</div>
        </div>
        """
        for label, value, note in cards
    )
    displayHTML(
        f"""
        <div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:12px 0 18px 0;">
          {card_html}
        </div>
        """
    )

# COMMAND ----------

route_summary_df = (
    metrics_df.where(col("route_short_name").isNotNull())
    .groupBy("route_short_name")
    .agg(
        spark_count("*").alias("observed_trips"),
        spark_avg("drift_minutes").alias("avg_drift_minutes"),
        percentile_approx("drift_minutes", 0.5).alias("p50_drift_minutes"),
        percentile_approx("drift_minutes", 0.9).alias("p90_drift_minutes"),
        spark_avg("update_count").alias("avg_update_count"),
        spark_avg(expr("case when drift_minutes >= 5 then 1.0 else 0.0 end")).alias("pct_trips_drift_ge_5_min"),
    )
    .where(col("observed_trips") >= route_min_trips)
)

stop_summary_df = (
    metrics_df.where(col("stop_id").isNotNull())
    .groupBy("stop_id", "stop_display_name", "stop_name")
    .agg(
        spark_count("*").alias("observed_trips"),
        spark_avg("drift_minutes").alias("avg_drift_minutes"),
        percentile_approx("drift_minutes", 0.9).alias("p90_drift_minutes"),
    )
    .where(col("observed_trips") >= stop_min_trips)
)

overall = metrics_df.agg(
    spark_count("*").alias("observed_trips"),
    expr("count(distinct route_short_name)").alias("observed_routes"),
    expr("count(distinct stop_id)").alias("observed_stops"),
    percentile_approx("drift_minutes", 0.5).alias("median_drift"),
    percentile_approx("drift_minutes", 0.9).alias("p90_drift"),
).first()

display_kpi_cards(
    [
        ("Observed Trip Metrics", f"{overall['observed_trips']:,}", "stop/trip groups with repeated snapshots"),
        ("Observed Routes", f"{overall['observed_routes']:,}", "only routes seen in collected stop feeds"),
        ("Median ETA Drift", f"{overall['median_drift']:.1f} min", "typical prediction movement"),
        ("P90 ETA Drift", f"{overall['p90_drift']:.1f} min", "student-safe buffer signal"),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Which Routes Should Students Trust Least?
# MAGIC
# MAGIC This is not actual delay. It is ETA instability: how much the prediction moved for the same bus/trip.
# MAGIC A route with high p90 drift deserves more buffer because its ETA has historically changed more.

# COMMAND ----------

route_trust_pdf = (
    route_summary_df.orderBy(desc("p90_drift_minutes"))
    .limit(top_n)
    .toPandas()
)

if route_trust_pdf.empty:
    print("No route summary data available yet.")
else:
    route_trust_pdf["p90_drift_minutes"] = route_trust_pdf["p90_drift_minutes"].astype(float)
    route_trust_pdf["avg_drift_minutes"] = route_trust_pdf["avg_drift_minutes"].astype(float)
    route_trust_pdf["trust_score"] = route_trust_pdf["p90_drift_minutes"].apply(trust_score_from_p90)
    route_trust_pdf["risk_label"] = route_trust_pdf["p90_drift_minutes"].apply(route_risk_label)
    route_trust_pdf["suggested_buffer_min"] = route_trust_pdf["p90_drift_minutes"].apply(suggested_buffer_minutes)
    route_plot_pdf = route_trust_pdf.sort_values("p90_drift_minutes", ascending=True)

    fig, ax = plt.subplots(figsize=(13, max(4.5, len(route_plot_pdf) * 0.48)), dpi=140)
    fig.patch.set_facecolor(COLORS["bg"])
    bar_colors = [route_risk_color(v) for v in route_plot_pdf["p90_drift_minutes"]]

    ax.barh(
        route_plot_pdf["route_short_name"],
        route_plot_pdf["p90_drift_minutes"],
        color=bar_colors,
        height=0.62,
    )
    add_bar_labels(ax, route_plot_pdf["p90_drift_minutes"].tolist())
    ax.set_title("Routes With the Shakiest ETAs", fontsize=20, fontweight="bold", color=COLORS["ink"], loc="left", pad=18)
    ax.text(
        0,
        1.04,
        "Ranked by p90 ETA drift. Red means the ETA often moved enough that students should add buffer.",
        transform=ax.transAxes,
        fontsize=12,
        color=COLORS["muted"],
    )
    ax.set_xlabel("P90 ETA drift (minutes)", fontsize=12, color=COLORS["muted"], labelpad=10)
    style_axis(ax, "x")
    ax.set_xlim(0, route_plot_pdf["p90_drift_minutes"].max() * 1.22)
    plt.tight_layout()
    plt.show()

    display(
        route_trust_pdf[
            [
                "route_short_name",
                "observed_trips",
                "avg_drift_minutes",
                "p50_drift_minutes",
                "p90_drift_minutes",
                "trust_score",
                "risk_label",
                "suggested_buffer_min",
            ]
        ].sort_values("p90_drift_minutes", ascending=False)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. What Buffer Should a Student Add?
# MAGIC
# MAGIC A practical product should not just show a metric. It should turn drift into advice:
# MAGIC **safe to trust**, **keep checking**, or **leave extra buffer**.

# COMMAND ----------

if route_trust_pdf.empty:
    print("No route buffer data available yet.")
else:
    buffer_pdf = route_trust_pdf.sort_values("suggested_buffer_min", ascending=True)
    fig, ax = plt.subplots(figsize=(13, max(4.5, len(buffer_pdf) * 0.48)), dpi=140)
    fig.patch.set_facecolor(COLORS["bg"])

    ax.barh(
        buffer_pdf["route_short_name"],
        buffer_pdf["suggested_buffer_min"],
        color=[route_risk_color(v) for v in buffer_pdf["p90_drift_minutes"]],
        height=0.62,
    )
    for patch, score, label in zip(ax.patches, buffer_pdf["trust_score"], buffer_pdf["risk_label"]):
        width = patch.get_width()
        ax.text(
            width + 0.18,
            patch.get_y() + patch.get_height() / 2,
            f"add {int(width)} min  ·  trust {score}/100  ·  {label}",
            va="center",
            fontsize=10,
            color=COLORS["ink"],
            fontweight="bold",
        )

    ax.set_title("Student Buffer Guide by Route", fontsize=20, fontweight="bold", color=COLORS["ink"], loc="left", pad=18)
    ax.text(
        0,
        1.04,
        "Suggested buffer is the rounded-up p90 ETA drift from observed trips, not actual lateness.",
        transform=ax.transAxes,
        fontsize=12,
        color=COLORS["muted"],
    )
    ax.set_xlabel("Suggested buffer (minutes)", fontsize=12, color=COLORS["muted"], labelpad=10)
    style_axis(ax, "x")
    ax.set_xlim(0, buffer_pdf["suggested_buffer_min"].max() * 1.65)
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Where Are ETAs Most Volatile?
# MAGIC
# MAGIC This turns platform/stop metadata into something students can recognize: not just `IT:5`,
# MAGIC but the stop or boarding point label carried through the pipeline.

# COMMAND ----------

stop_risk_pdf = (
    stop_summary_df.orderBy(desc("p90_drift_minutes"))
    .limit(top_n)
    .toPandas()
)

if stop_risk_pdf.empty:
    print("No stop summary data available yet.")
else:
    stop_risk_pdf["label"] = stop_risk_pdf.apply(stop_label, axis=1)
    stop_risk_pdf["p90_drift_minutes"] = stop_risk_pdf["p90_drift_minutes"].astype(float)
    stop_plot_pdf = stop_risk_pdf.sort_values("p90_drift_minutes", ascending=True)

    fig, ax = plt.subplots(figsize=(13, max(4.5, len(stop_plot_pdf) * 0.55)), dpi=140)
    fig.patch.set_facecolor(COLORS["bg"])
    ax.barh(
        stop_plot_pdf["label"],
        stop_plot_pdf["p90_drift_minutes"],
        color=[route_risk_color(v) for v in stop_plot_pdf["p90_drift_minutes"]],
        height=0.62,
    )
    add_bar_labels(ax, stop_plot_pdf["p90_drift_minutes"].tolist())
    ax.set_title("Stops Where ETAs Need the Most Buffer", fontsize=20, fontweight="bold", color=COLORS["ink"], loc="left", pad=18)
    ax.text(
        0,
        1.04,
        "Ranked by p90 ETA drift across observed trips at each stop/platform.",
        transform=ax.transAxes,
        fontsize=12,
        color=COLORS["muted"],
    )
    ax.set_xlabel("P90 ETA drift (minutes)", fontsize=12, color=COLORS["muted"], labelpad=10)
    style_axis(ax, "x")
    ax.set_xlim(0, stop_plot_pdf["p90_drift_minutes"].max() * 1.25)
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. One Bus, Many ETAs
# MAGIC
# MAGIC This chart shows the core idea. The same trip can have many different ETAs before departure.
# MAGIC The final observed ETA is only a reference point from the feed, **not confirmed actual departure**.

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

    fig, ax = plt.subplots(figsize=(13.5, 6.5), dpi=140)
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
    ax.fill_between(
        trip_pdf["time_before_eta_minutes"],
        final_observed_offset,
        trip_pdf["offset_from_schedule_minutes"],
        color=COLORS["red"],
        alpha=0.08,
    )
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
        f"(reference only, not actual departure).",
        transform=ax.transAxes,
        fontsize=12,
        color=COLORS["muted"],
    )
    ax.set_xlabel("Minutes before the current estimated departure", fontsize=12, color=COLORS["muted"], labelpad=10)
    ax.set_ylabel("Estimated departure offset from schedule (minutes)", fontsize=12, color=COLORS["muted"], labelpad=10)
    style_axis(ax, "y")
    plt.tight_layout()
    plt.show()

    display(trip_pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. How Wide Is the ETA Uncertainty?
# MAGIC
# MAGIC This distribution is the simplest blog visual: it shows whether most ETAs are stable or if
# MAGIC a meaningful chunk of trips have several minutes of prediction movement.

# COMMAND ----------

drift_pdf = metrics_df.where(col("drift_minutes").isNotNull()).select("drift_minutes").toPandas()

if drift_pdf.empty:
    print("No drift distribution available yet.")
else:
    drift_pdf["drift_minutes"] = drift_pdf["drift_minutes"].astype(float)
    median_drift = drift_pdf["drift_minutes"].median()
    p90_drift = drift_pdf["drift_minutes"].quantile(0.9)

    fig, ax = plt.subplots(figsize=(13, 5.5), dpi=140)
    fig.patch.set_facecolor(COLORS["bg"])
    ax.hist(
        drift_pdf["drift_minutes"],
        bins=30,
        color=COLORS["blue"],
        alpha=0.78,
        edgecolor="white",
        linewidth=1,
    )
    ax.axvspan(0, LOW_DRIFT_MIN, color=COLORS["green"], alpha=0.08)
    ax.axvspan(LOW_DRIFT_MIN, MEDIUM_DRIFT_MIN, color=COLORS["amber"], alpha=0.10)
    ax.axvspan(MEDIUM_DRIFT_MIN, max(drift_pdf["drift_minutes"].max(), MEDIUM_DRIFT_MIN), color=COLORS["red"], alpha=0.08)
    ax.axvline(median_drift, color=COLORS["amber"], linewidth=2.2)
    ax.axvline(p90_drift, color=COLORS["red"], linewidth=2.2, linestyle="--")
    ymax = ax.get_ylim()[1]
    ax.text(median_drift + 0.15, ymax * 0.90, f"median {median_drift:.1f} min", color=COLORS["amber"], fontweight="bold")
    ax.text(p90_drift + 0.15, ymax * 0.78, f"p90 {p90_drift:.1f} min", color=COLORS["red"], fontweight="bold")
    ax.set_title("ETA Drift Distribution", fontsize=20, fontweight="bold", color=COLORS["ink"], loc="left", pad=18)
    ax.text(
        0,
        1.04,
        "Green: stable. Yellow: watch it. Red: add buffer.",
        transform=ax.transAxes,
        fontsize=12,
        color=COLORS["muted"],
    )
    ax.set_xlabel("ETA drift for one stop/trip (minutes)", fontsize=12, color=COLORS["muted"], labelpad=10)
    ax.set_ylabel("Observed trip count", fontsize=12, color=COLORS["muted"], labelpad=10)
    style_axis(ax, "y")
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Route × Stop Heatmap
# MAGIC
# MAGIC This is the dashboard view: where are specific route/stop combinations most unstable?

# COMMAND ----------

heatmap_pdf = (
    metrics_df.where(col("route_short_name").isNotNull())
    .where(col("stop_id").isNotNull())
    .groupBy("route_short_name", "stop_display_name", "stop_name", "stop_id")
    .agg(
        spark_count("*").alias("observed_trips"),
        spark_avg("drift_minutes").alias("avg_drift_minutes"),
    )
    .where(col("observed_trips") >= 1)
    .toPandas()
)

if heatmap_pdf.empty:
    print("No heatmap data available yet.")
else:
    heatmap_pdf["stop_label"] = heatmap_pdf.apply(stop_label, axis=1)
    top_routes = (
        heatmap_pdf.groupby("route_short_name")["observed_trips"]
        .sum()
        .sort_values(ascending=False)
        .head(min(10, top_n))
        .index
    )
    top_stops = (
        heatmap_pdf.groupby("stop_label")["observed_trips"]
        .sum()
        .sort_values(ascending=False)
        .head(min(10, top_n))
        .index
    )
    matrix = (
        heatmap_pdf[heatmap_pdf["route_short_name"].isin(top_routes) & heatmap_pdf["stop_label"].isin(top_stops)]
        .pivot_table(
            index="stop_label",
            columns="route_short_name",
            values="avg_drift_minutes",
            aggfunc="mean",
        )
        .reindex(index=top_stops, columns=top_routes)
    )

    fig, ax = plt.subplots(figsize=(13, max(5.5, len(matrix.index) * 0.48)), dpi=140)
    fig.patch.set_facecolor(COLORS["bg"])
    im = ax.imshow(matrix.values, aspect="auto", cmap="RdYlGn_r")
    ax.set_xticks(np.arange(len(matrix.columns)))
    ax.set_xticklabels(matrix.columns, rotation=35, ha="right", fontsize=9, color=COLORS["ink"])
    ax.set_yticks(np.arange(len(matrix.index)))
    ax.set_yticklabels(matrix.index, fontsize=9, color=COLORS["ink"])
    ax.set_title("Average ETA Drift by Route and Stop", fontsize=20, fontweight="bold", color=COLORS["ink"], loc="left", pad=18)
    ax.text(
        0,
        1.04,
        "Darker red cells are route/stop combinations where the ETA moved more in the observed sample.",
        transform=ax.transAxes,
        fontsize=12,
        color=COLORS["muted"],
    )
    for spine in ax.spines.values():
        spine.set_visible(False)
    cbar = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.02)
    cbar.set_label("Avg ETA drift (min)", color=COLORS["muted"])
    cbar.ax.tick_params(colors=COLORS["muted"])
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Blog/LinkedIn Takeaway
# MAGIC
# MAGIC **This is not another bus tracker.** MTD already gives ETAs.
# MAGIC
# MAGIC This project adds the missing confidence layer:
# MAGIC
# MAGIC > When the app says a bus is coming, how much should a student trust that prediction?
# MAGIC
# MAGIC The dashboard can become a student-facing product:
# MAGIC
# MAGIC - **ETA:** what MTD predicts now
# MAGIC - **Trust score:** how stable this route/stop has been historically
# MAGIC - **Typical drift:** how much the ETA usually moves
# MAGIC - **Suggested buffer:** how many extra minutes to add before leaving
