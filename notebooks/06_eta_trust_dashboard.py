# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # 06 - ETA Trust Dashboard
# MAGIC
# MAGIC This is the student-facing story layer.
# MAGIC
# MAGIC MTD already gives an ETA. This dashboard adds the missing context:
# MAGIC **how much should a student trust that ETA, and how much buffer should they add?**

# COMMAND ----------

from __future__ import annotations

import matplotlib.pyplot as plt
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
    from eta_analysis_utils import (
        COLORS,
        add_bar_labels,
        kpi_cards_html,
        route_risk_color,
        route_risk_label,
        stop_label,
        style_axis,
        suggested_buffer_minutes,
        trust_score_from_p90,
    )
except ModuleNotFoundError:
    from notebooks.eta_analysis_utils import (
        COLORS,
        add_bar_labels,
        kpi_cards_html,
        route_risk_color,
        route_risk_label,
        stop_label,
        style_axis,
        suggested_buffer_minutes,
        trust_score_from_p90,
    )

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cumtd_eta_drift")
dbutils.widgets.text("top_n", "15")
dbutils.widgets.text("route_min_trips", "2")
dbutils.widgets.text("stop_min_trips", "2")

catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema").strip()
top_n = int(dbutils.widgets.get("top_n"))
route_min_trips = int(dbutils.widgets.get("route_min_trips"))
stop_min_trips = int(dbutils.widgets.get("stop_min_trips"))

database_name = f"{catalog}.{schema_name}" if catalog else schema_name
metrics_table = f"{database_name}.departure_drift_metrics"
metrics_df = spark.table(metrics_table)

if metrics_df.count() == 0:
    raise ValueError("No drift metrics found. Run ingestion and dbt before the dashboard.")

# COMMAND ----------

route_summary_df = (
    metrics_df.where(col("route_short_name").isNotNull())
    .groupBy("route_short_name")
    .agg(
        spark_count("*").alias("observed_trips"),
        spark_avg("drift_minutes").alias("avg_drift_minutes"),
        percentile_approx("drift_minutes", 0.5).alias("p50_drift_minutes"),
        percentile_approx("drift_minutes", 0.9).alias("p90_drift_minutes"),
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
    spark_count("*").alias("observed_trip_metrics"),
    expr("count(distinct route_short_name)").alias("observed_routes"),
    expr("count(distinct stop_id)").alias("observed_stops"),
    percentile_approx("drift_minutes", 0.5).alias("median_drift"),
    percentile_approx("drift_minutes", 0.9).alias("p90_drift"),
).first()

displayHTML(
    kpi_cards_html(
        [
            ("Observed Trip Metrics", f"{overall['observed_trip_metrics']:,}", "stop/trip groups with repeated snapshots"),
            ("Observed Routes", f"{overall['observed_routes']:,}", "routes seen in collected stop feeds"),
            ("Median ETA Drift", f"{overall['median_drift']:.1f} min", "typical prediction movement"),
            ("P90 ETA Drift", f"{overall['p90_drift']:.1f} min", "buffer signal for students"),
        ]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Route Trust Score
# MAGIC
# MAGIC A higher trust score means the route's ETA was more stable in the observed sample.

# COMMAND ----------

route_trust_pdf = (
    route_summary_df.orderBy(desc("p90_drift_minutes"))
    .limit(top_n)
    .toPandas()
)

if route_trust_pdf.empty:
    print("No route trust data available.")
else:
    route_trust_pdf["p90_drift_minutes"] = route_trust_pdf["p90_drift_minutes"].astype(float)
    route_trust_pdf["avg_drift_minutes"] = route_trust_pdf["avg_drift_minutes"].astype(float)
    route_trust_pdf["trust_score"] = route_trust_pdf["p90_drift_minutes"].apply(trust_score_from_p90)
    route_trust_pdf["risk_label"] = route_trust_pdf["p90_drift_minutes"].apply(route_risk_label)
    route_trust_pdf["suggested_buffer_min"] = route_trust_pdf["p90_drift_minutes"].apply(suggested_buffer_minutes)
    route_plot_pdf = route_trust_pdf.sort_values("trust_score", ascending=True)

    fig, ax = plt.subplots(figsize=(13, max(4.5, len(route_plot_pdf) * 0.48)), dpi=140)
    fig.patch.set_facecolor(COLORS["bg"])
    ax.barh(
        route_plot_pdf["route_short_name"],
        route_plot_pdf["trust_score"],
        color=[route_risk_color(v) for v in route_plot_pdf["p90_drift_minutes"]],
        height=0.62,
    )
    for patch, p90, label in zip(ax.patches, route_plot_pdf["p90_drift_minutes"], route_plot_pdf["risk_label"]):
        width = patch.get_width()
        ax.text(
            width + 1,
            patch.get_y() + patch.get_height() / 2,
            f"{width:.0f}/100  ·  p90 drift {p90:.1f} min  ·  {label}",
            va="center",
            fontsize=10,
            color=COLORS["ink"],
            fontweight="bold",
        )
    ax.set_title("ETA Trust Score by Route", fontsize=20, fontweight="bold", color=COLORS["ink"], loc="left", pad=18)
    ax.text(
        0,
        1.04,
        "Based on p90 ETA drift. This is a confidence signal, not actual lateness.",
        transform=ax.transAxes,
        fontsize=12,
        color=COLORS["muted"],
    )
    ax.set_xlabel("ETA trust score", fontsize=12, color=COLORS["muted"], labelpad=10)
    ax.set_xlim(0, 110)
    style_axis(ax, "x")
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
        ].sort_values("trust_score", ascending=True)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Student Buffer Guide
# MAGIC
# MAGIC This turns the analytics into a practical recommendation: how many extra minutes should a
# MAGIC student build in when using the ETA?

# COMMAND ----------

if route_trust_pdf.empty:
    print("No buffer data available.")
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
    ax.set_title("Suggested Buffer by Route", fontsize=20, fontweight="bold", color=COLORS["ink"], loc="left", pad=18)
    ax.text(
        0,
        1.04,
        "Rounded-up p90 ETA drift from observed trips. Use this as planning context.",
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
# MAGIC ## Stops Where Students Should Be More Careful

# COMMAND ----------

stop_risk_pdf = (
    stop_summary_df.orderBy(desc("p90_drift_minutes"))
    .limit(top_n)
    .toPandas()
)

if stop_risk_pdf.empty:
    print("No stop trust data available.")
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
# MAGIC ## Route × Stop Heatmap

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
    print("No heatmap data available.")
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
        .pivot_table(index="stop_label", columns="route_short_name", values="avg_drift_minutes", aggfunc="mean")
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
        "Darker red cells are route/stop combinations where the ETA moved more.",
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
# MAGIC ## Product Takeaway
# MAGIC
# MAGIC This dashboard is the bridge from analytics to a student-facing app:
# MAGIC
# MAGIC - **ETA:** what MTD predicts now
# MAGIC - **Trust score:** how stable this route/stop has been historically
# MAGIC - **Typical drift:** how much the ETA usually moves
# MAGIC - **Suggested buffer:** how many extra minutes to add before leaving
