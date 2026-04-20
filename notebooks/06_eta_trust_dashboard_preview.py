# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # 06 - ETA Trust Dashboard Preview
# MAGIC
# MAGIC Databricks should own the polished dashboard layer.
# MAGIC
# MAGIC This notebook is only a preview/sanity check for the dashboard-ready dbt marts:
# MAGIC
# MAGIC - `eta_trust_route_scores`
# MAGIC - `eta_trust_stop_scores`
# MAGIC - `eta_trust_route_stop_heatmap`
# MAGIC
# MAGIC Build the final dashboard in Databricks SQL/Lakeview using these tables.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cumtd_eta_drift")

catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema").strip()
database_name = f"{catalog}.{schema_name}" if catalog else schema_name

route_scores_table = f"{database_name}.eta_trust_route_scores"
stop_scores_table = f"{database_name}.eta_trust_stop_scores"
heatmap_table = f"{database_name}.eta_trust_route_stop_heatmap"

print(f"Route score table: {route_scores_table}")
print(f"Stop score table: {stop_scores_table}")
print(f"Route-stop heatmap table: {heatmap_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI Cards
# MAGIC
# MAGIC Use these as Databricks dashboard counters.

# COMMAND ----------

display(
    spark.sql(
        f"""
        select
            count(*) as scored_routes,
            round(avg(eta_trust_score), 1) as avg_route_trust_score,
            round(avg(suggested_buffer_minutes), 1) as avg_suggested_buffer_minutes,
            round(avg(p90_drift_minutes), 2) as avg_p90_drift_minutes
        from {route_scores_table}
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Route Trust Score
# MAGIC
# MAGIC Databricks dashboard visual: bar chart sorted by `eta_trust_score` ascending.

# COMMAND ----------

display(
    spark.sql(
        f"""
        select
            route_short_name,
            observed_trip_count,
            eta_trust_score,
            eta_trust_label,
            student_guidance,
            suggested_buffer_minutes,
            round(p50_drift_minutes, 2) as p50_drift_minutes,
            round(p90_drift_minutes, 2) as p90_drift_minutes,
            round(pct_trips_drift_ge_5_min * 100, 1) as pct_trips_drift_ge_5_min
        from {route_scores_table}
        order by eta_trust_score asc, observed_trip_count desc
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop / Platform Trust Score
# MAGIC
# MAGIC Databricks dashboard visual: bar chart or table sorted by `suggested_buffer_minutes` descending.

# COMMAND ----------

display(
    spark.sql(
        f"""
        select
            stop_id,
            coalesce(stop_display_name, stop_name, stop_id) as stop_label,
            observed_trip_count,
            eta_trust_score,
            eta_trust_label,
            student_guidance,
            suggested_buffer_minutes,
            round(p90_drift_minutes, 2) as p90_drift_minutes
        from {stop_scores_table}
        order by suggested_buffer_minutes desc, observed_trip_count desc
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Route × Stop Heatmap
# MAGIC
# MAGIC Databricks dashboard visual: heatmap using route on one axis, stop label on the other,
# MAGIC and `avg_drift_minutes` or `eta_trust_score` as the color value.

# COMMAND ----------

display(
    spark.sql(
        f"""
        select
            route_short_name,
            coalesce(stop_display_name, stop_name, stop_id) as stop_label,
            observed_trip_count,
            round(avg_drift_minutes, 2) as avg_drift_minutes,
            round(p90_drift_minutes, 2) as p90_drift_minutes,
            eta_trust_score,
            eta_trust_label,
            suggested_buffer_minutes
        from {heatmap_table}
        order by p90_drift_minutes desc, observed_trip_count desc
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard Build Notes
# MAGIC
# MAGIC Suggested Databricks dashboard tiles:
# MAGIC
# MAGIC 1. KPI: scored routes, average trust score, average suggested buffer.
# MAGIC 2. Bar chart: `eta_trust_route_scores` by route, sorted by trust score.
# MAGIC 3. Bar chart/table: `eta_trust_stop_scores` by stop/platform, sorted by suggested buffer.
# MAGIC 4. Heatmap: `eta_trust_route_stop_heatmap`, route × stop, colored by p90 drift or trust score.
# MAGIC 5. Detail table: route, trust label, student guidance, suggested buffer.
