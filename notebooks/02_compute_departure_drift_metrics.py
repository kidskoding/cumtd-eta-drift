# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Compute Departure Drift Metrics
# MAGIC
# MAGIC Transform raw departure snapshots into one drift metrics row per stop/trip/route grouping.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    first,
    lag,
    last,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    unix_timestamp,
    when,
)

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cumtd_eta_drift")
dbutils.widgets.text("min_snapshots_per_trip", "2")

catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema").strip()
min_snapshots_per_trip = int(dbutils.widgets.get("min_snapshots_per_trip"))

database_name = f"{catalog}.{schema_name}" if catalog else schema_name
raw_table = f"{database_name}.raw_departure_snapshots"
metrics_table = f"{database_name}.departure_drift_metrics"

# COMMAND ----------

raw_df = (
    spark.table(raw_table)
    .where(col("trip_id").isNotNull())
    .where(col("stop_id").isNotNull())
    .where(col("scheduledDeparture").isNotNull())
    .where(col("estimatedDeparture").isNotNull())
)

with_features = (
    raw_df.withColumn(
        "estimate_offset_minutes",
        (unix_timestamp("estimatedDeparture") - unix_timestamp("scheduledDeparture")) / 60.0,
    )
    .withColumn(
        "time_to_departure_minutes",
        (unix_timestamp("estimatedDeparture") - unix_timestamp("ingestion_timestamp")) / 60.0,
    )
)

trip_window = Window.partitionBy("stop_id", "trip_id", "route_id", "route_short_name").orderBy("ingestion_timestamp")
full_trip_window = trip_window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

ordered = (
    with_features.withColumn("previous_estimatedDeparture", lag("estimatedDeparture").over(trip_window))
    .withColumn("first_scheduledDeparture", first("scheduledDeparture", ignorenulls=True).over(full_trip_window))
    .withColumn("first_observed_estimatedDeparture", first("estimatedDeparture", ignorenulls=True).over(full_trip_window))
    .withColumn("last_observed_estimatedDeparture", last("estimatedDeparture", ignorenulls=True).over(full_trip_window))
    .withColumn(
        "eta_delta_minutes",
        (unix_timestamp("estimatedDeparture") - unix_timestamp("previous_estimatedDeparture")) / 60.0,
    )
    .withColumn("monotonicity_violation", when(col("eta_delta_minutes") < 0, 1).otherwise(0))
    .withColumn("forward_jump_minutes", when(col("eta_delta_minutes") > 0, col("eta_delta_minutes")).otherwise(0.0))
    .withColumn("backward_jump_minutes", when(col("eta_delta_minutes") < 0, -col("eta_delta_minutes")).otherwise(0.0))
)

# COMMAND ----------

metrics_df = (
    ordered.groupBy("stop_id", "trip_id", "route_id", "route_short_name")
    .agg(
        first("stop_name", ignorenulls=True).alias("stop_name"),
        first("first_scheduledDeparture", ignorenulls=True).alias("scheduledDeparture"),
        spark_min("ingestion_timestamp").alias("first_ingestion_timestamp"),
        spark_max("ingestion_timestamp").alias("last_ingestion_timestamp"),
        first("first_observed_estimatedDeparture", ignorenulls=True).alias("first_estimatedDeparture"),
        first("last_observed_estimatedDeparture", ignorenulls=True).alias("last_estimatedDeparture"),
        spark_min("estimatedDeparture").alias("min_estimatedDeparture"),
        spark_max("estimatedDeparture").alias("max_estimatedDeparture"),
        avg("estimate_offset_minutes").alias("avg_estimate_offset_minutes"),
        spark_min("estimate_offset_minutes").alias("min_estimate_offset_minutes"),
        spark_max("estimate_offset_minutes").alias("max_estimate_offset_minutes"),
        count("*").alias("update_count"),
        spark_sum("monotonicity_violation").alias("monotonicity_violation_count"),
        spark_max("forward_jump_minutes").alias("max_forward_jump_minutes"),
        spark_max("backward_jump_minutes").alias("max_backward_jump_minutes"),
        avg("time_to_departure_minutes").alias("avg_time_to_departure_minutes"),
        spark_min("time_to_departure_minutes").alias("min_time_to_departure_minutes"),
        spark_max("time_to_departure_minutes").alias("max_time_to_departure_minutes"),
    )
    .withColumn(
        "drift_minutes",
        (unix_timestamp("max_estimatedDeparture") - unix_timestamp("min_estimatedDeparture")) / 60.0,
    )
    .withColumn("metric_computed_at", current_timestamp())
    .where(col("update_count") >= min_snapshots_per_trip)
    .select(
        "stop_id",
        "stop_name",
        "trip_id",
        "route_id",
        "route_short_name",
        "scheduledDeparture",
        "first_ingestion_timestamp",
        "last_ingestion_timestamp",
        "first_estimatedDeparture",
        "last_estimatedDeparture",
        "min_estimatedDeparture",
        "max_estimatedDeparture",
        "avg_estimate_offset_minutes",
        "min_estimate_offset_minutes",
        "max_estimate_offset_minutes",
        "drift_minutes",
        "update_count",
        "monotonicity_violation_count",
        "max_forward_jump_minutes",
        "max_backward_jump_minutes",
        "avg_time_to_departure_minutes",
        "min_time_to_departure_minutes",
        "max_time_to_departure_minutes",
        "metric_computed_at",
    )
)

display(metrics_df.orderBy(col("drift_minutes").desc(), col("update_count").desc()))

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database_name}")

(
    metrics_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(metrics_table)
)

print(f"Wrote {metrics_df.count()} rows to {metrics_table}")
