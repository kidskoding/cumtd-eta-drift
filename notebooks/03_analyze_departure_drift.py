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

trip_pdf = (
    raw_df.where(col("trip_id") == selected_trip_id)
    .where(col("stop_id") == selected_stop_id)
    .where(col("estimatedDeparture").isNotNull())
    .select("ingestion_timestamp", "estimatedDeparture", "scheduledDeparture", "minutesTillDeparture")
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

plt.figure(figsize=(10, 5))
plt.plot(
    trip_pdf["time_before_departure_minutes"],
    trip_pdf["estimatedDeparture"],
    marker="o",
    linewidth=2,
)
plt.gca().invert_xaxis()
plt.xlabel("Minutes before estimated departure")
plt.ylabel("Estimated departure time")
plt.title(f"ETA drift over repeated snapshots: stop={selected_stop_id}, trip={selected_trip_id}")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

display(trip_pdf)

# COMMAND ----------

drift_pdf = (
    metrics_df.where(col("drift_minutes").isNotNull())
    .select("drift_minutes")
    .toPandas()
)

if drift_pdf.empty:
    raise ValueError("No non-null drift_minutes values available.")

plt.figure(figsize=(10, 5))
plt.hist(drift_pdf["drift_minutes"], bins=30, edgecolor="black", alpha=0.75)
plt.xlabel("Drift minutes")
plt.ylabel("Trip count")
plt.title("Distribution of ETA drift across stop/trip observations")
plt.grid(True, axis="y", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

top_stops_df = (
    metrics_df.groupBy("stop_id")
    .avg("drift_minutes")
    .withColumnRenamed("avg(drift_minutes)", "avg_drift_minutes")
    .orderBy(desc("avg_drift_minutes"))
    .limit(top_n)
)

top_routes_df = (
    metrics_df.groupBy("route_short_name")
    .avg("drift_minutes")
    .withColumnRenamed("avg(drift_minutes)", "avg_drift_minutes")
    .orderBy(desc("avg_drift_minutes"))
    .limit(top_n)
)

display(top_stops_df)
display(top_routes_df)
