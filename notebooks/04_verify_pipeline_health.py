# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # 04 - Verify Pipeline Health
# MAGIC
# MAGIC Run after Lambda writes S3 snapshots, the ingestion notebook appends raw Delta rows, and dbt builds the transformation models.

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField, StructType

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cumtd_eta_drift")
dbutils.widgets.text("min_raw_rows", "1")

catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema").strip()
min_raw_rows = int(dbutils.widgets.get("min_raw_rows"))

database_name = f"{catalog}.{schema_name}" if catalog else schema_name

raw_table = f"{database_name}.raw_departure_snapshots"
audit_table = f"{database_name}.departure_ingestion_audit"
staging_table = f"{database_name}.stg_departure_snapshots"
metrics_table = f"{database_name}.departure_drift_metrics"
stop_summary_table = f"{database_name}.daily_stop_drift_summary"
route_summary_table = f"{database_name}.daily_route_drift_summary"
route_scores_table = f"{database_name}.eta_trust_route_scores"
stop_scores_table = f"{database_name}.eta_trust_stop_scores"
heatmap_table = f"{database_name}.eta_trust_route_stop_heatmap"

print(f"Verifying pipeline tables in {database_name}")

# COMMAND ----------

def table_exists(table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


def scalar(sql: str):
    return spark.sql(sql).first()[0]


def count_where(table_name: str, condition: str) -> int:
    return scalar(f"select count(*) from {table_name} where {condition}")


checks = []
check_schema = StructType(
    [
        StructField("check_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("observed_value", StringType(), True),
        StructField("details", StringType(), True),
    ]
)

for table_name in [
    raw_table,
    audit_table,
    staging_table,
    metrics_table,
    stop_summary_table,
    route_summary_table,
    route_scores_table,
    stop_scores_table,
    heatmap_table,
]:
    checks.append(
        Row(
            check_name=f"table_exists:{table_name}",
            status="pass" if table_exists(table_name) else "fail",
            observed_value=table_name,
            details=None,
        )
    )

if not table_exists(raw_table):
    display(spark.createDataFrame(checks, check_schema))
    raise ValueError(f"Missing required raw table: {raw_table}")

# COMMAND ----------

raw_rows = scalar(f"select count(*) from {raw_table}")
rows_with_stop_name = count_where(raw_table, "stop_name is not null")
rows_with_trip_id = count_where(raw_table, "trip_id is not null")
rows_with_eta = count_where(raw_table, "estimatedDeparture is not null")
rows_with_schedule = count_where(raw_table, "scheduledDeparture is not null")

quality_checks = [
    ("raw_rows", raw_rows, raw_rows >= min_raw_rows, f"Expected at least {min_raw_rows} raw row(s)."),
    ("rows_with_stop_name", rows_with_stop_name, rows_with_stop_name > 0, "Expected at least one non-null stop_name."),
    ("rows_with_trip_id", rows_with_trip_id, rows_with_trip_id > 0, "Expected at least one non-null trip_id."),
    ("rows_with_eta", rows_with_eta, rows_with_eta > 0, "Expected at least one non-null estimatedDeparture."),
    ("rows_with_schedule", rows_with_schedule, rows_with_schedule > 0, "Expected at least one non-null scheduledDeparture."),
]

for check_name, observed_value, passed, details in quality_checks:
    checks.append(
        Row(
            check_name=check_name,
            status="pass" if passed else "fail",
            observed_value=str(observed_value),
            details=None if passed else details,
        )
    )

if table_exists(metrics_table):
    metric_rows = scalar(f"select count(*) from {metrics_table}")
    metrics_with_drift = count_where(metrics_table, "drift_minutes is not null")
    checks.append(
        Row(
            check_name="metric_rows",
            status="pass" if metric_rows > 0 else "warn",
            observed_value=str(metric_rows),
            details=None if metric_rows > 0 else "Need repeated snapshots for dbt to produce meaningful trip drift rows.",
        )
    )
    checks.append(
        Row(
            check_name="metrics_with_drift",
            status="pass" if metrics_with_drift > 0 else "warn",
            observed_value=str(metrics_with_drift),
            details=None if metrics_with_drift > 0 else "Drift stays empty until the same stop/trip appears in multiple snapshots.",
        )
    )

for trust_table in [route_scores_table, stop_scores_table, heatmap_table]:
    if table_exists(trust_table):
        trust_rows = scalar(f"select count(*) from {trust_table}")
        checks.append(
            Row(
                check_name=f"dashboard_rows:{trust_table}",
                status="pass" if trust_rows > 0 else "warn",
                observed_value=str(trust_rows),
                details=None if trust_rows > 0 else "Dashboard mart exists but has no rows yet.",
            )
        )

checks_df = spark.createDataFrame(checks, check_schema)
display(checks_df.orderBy(col("status"), col("check_name")))

failed_checks = [row for row in checks if row.status == "fail"]
if failed_checks:
    raise ValueError(f"{len(failed_checks)} required pipeline health check(s) failed.")

# COMMAND ----------

display(
    spark.sql(
        f"""
        select
            source_stop_id,
            stop_id,
            stop_name,
            route_short_name,
            count(*) as rows,
            min(ingestion_timestamp) as first_seen_at,
            max(ingestion_timestamp) as last_seen_at
        from {raw_table}
        group by source_stop_id, stop_id, stop_name, route_short_name
        order by rows desc
        limit 25
        """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        select
            source_stop_id,
            status,
            rows_extracted,
            error_message,
            ingestion_timestamp
        from {audit_table}
        order by ingestion_timestamp desc
        limit 25
        """
    )
)

print("Pipeline health verification complete.")
