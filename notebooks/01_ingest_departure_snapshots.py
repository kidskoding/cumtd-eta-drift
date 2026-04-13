# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # 01 - Ingest CUMTD Departure Snapshots
# MAGIC
# MAGIC Read JSON snapshots deposited by the AWS Lambda fetcher in S3, flatten them, and append to Delta.

# COMMAND ----------

from datetime import datetime, timezone
import json
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

from pyspark.sql import Row
from pyspark.sql.functions import col, input_file_name
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cumtd_eta_drift")
dbutils.widgets.text("s3_bucket", "cumtd-eta-drift")
dbutils.widgets.text("s3_prefix", "raw-departures")
dbutils.widgets.text("stop_ids", "IT")
dbutils.widgets.text("lookahead_minutes", "60")

catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema").strip()
s3_bucket = dbutils.widgets.get("s3_bucket").strip()
s3_prefix = dbutils.widgets.get("s3_prefix").strip().strip("/")
stop_ids = [s.strip() for s in dbutils.widgets.get("stop_ids").split(",") if s.strip()]
lookahead_minutes = int(dbutils.widgets.get("lookahead_minutes"))

if not stop_ids:
    raise ValueError("Provide at least one stop_id in the stop_ids widget.")

database_name = f"{catalog}.{schema_name}" if catalog else schema_name
raw_table = f"{database_name}.raw_departure_snapshots"
audit_table = f"{database_name}.departure_ingestion_audit"
s3_path = f"s3://{s3_bucket}/{s3_prefix}"

print(f"Reading snapshots from: {s3_path}")
print(f"Writing to: {raw_table}")

# COMMAND ----------

raw_schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("ingestion_date", DateType(), False),
        StructField("source_stop_id", StringType(), False),
        StructField("stop_id", StringType(), True),
        StructField("stop_name", StringType(), True),
        StructField("trip_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("route_short_name", StringType(), True),
        StructField("scheduledDeparture", TimestampType(), True),
        StructField("estimatedDeparture", TimestampType(), True),
        StructField("recordedTime", TimestampType(), True),
        StructField("minutesTillDeparture", DoubleType(), True),
        StructField("isRealTime", BooleanType(), True),
        StructField("response_time", TimestampType(), True),
        StructField("raw_departure_json", StringType(), True),
    ]
)

audit_schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("ingestion_date", DateType(), False),
        StructField("source_stop_id", StringType(), False),
        StructField("request_url", StringType(), True),
        StructField("status", StringType(), False),
        StructField("http_status_code", IntegerType(), True),
        StructField("rows_extracted", LongType(), False),
        StructField("duration_seconds", DoubleType(), True),
        StructField("error_message", StringType(), True),
    ]
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database_name}")

empty_df = spark.createDataFrame([], raw_schema)
(
    empty_df.write.format("delta")
    .mode("ignore")
    .option("mergeSchema", "false")
    .saveAsTable(raw_table)
)

empty_audit_df = spark.createDataFrame([], audit_schema)
(
    empty_audit_df.write.format("delta")
    .mode("ignore")
    .option("mergeSchema", "false")
    .saveAsTable(audit_table)
)

# COMMAND ----------

def _first_present(payload: Dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    return None


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)

    text = str(value).strip()
    if not text:
        return None

    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    raise ValueError(f"Could not parse timestamp: {value!r}")


def _as_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    return float(value)


def _as_bool(value: Any) -> Optional[bool]:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "t", "1", "yes", "y"}


def _nested(payload: Dict[str, Any], *path: str) -> Any:
    cursor: Any = payload
    for part in path:
        if not isinstance(cursor, dict):
            return None
        cursor = cursor.get(part)
    return cursor

# COMMAND ----------

def flatten_departure(
    run_id: str,
    source_stop_id: str,
    source_stop_name: str | None,
    payload: Dict[str, Any],
    departure: Dict[str, Any],
    ingestion_ts: datetime,
) -> Row:
    """Convert a single departure dict into a typed Row."""
    stop = departure.get("stop") or payload.get("stop") or {}
    route = departure.get("route") or {}
    trip = departure.get("trip") or {}

    scheduled = _first_present(departure, ["scheduledDeparture", "scheduled_departure", "scheduled"])
    estimated = _first_present(departure, ["estimatedDeparture", "estimated_departure", "expected"])
    recorded = _first_present(departure, ["recordedTime", "recorded_time"])

    return Row(
        run_id=run_id,
        ingestion_timestamp=ingestion_ts,
        ingestion_date=ingestion_ts.date(),
        source_stop_id=source_stop_id,
        stop_id=_first_present(departure, ["stop_id", "stopId"]) or _first_present(stop, ["stop_id", "id"]),
        stop_name=_first_present(departure, ["stop_name", "stopName"]) or _first_present(stop, ["stop_name", "name"]) or source_stop_name,
        trip_id=_first_present(departure, ["trip_id", "tripId"]) or _first_present(trip, ["trip_id", "tripId", "id"]),
        route_id=_first_present(departure, ["route_id", "routeId"]) or _first_present(route, ["route_id", "routeId", "id"]),
        route_short_name=_first_present(departure, ["route_short_name", "routeShortName", "headsign"])
        or _first_present(route, ["route_short_name", "routeShortName", "short_name", "shortName", "route_id", "id"]),
        scheduledDeparture=_parse_timestamp(scheduled),
        estimatedDeparture=_parse_timestamp(estimated),
        recordedTime=_parse_timestamp(recorded),
        minutesTillDeparture=_as_float(_first_present(departure, ["minutesTillDeparture", "minutes_till_departure", "mins"])),
        isRealTime=_as_bool(_first_present(departure, ["isRealTime", "is_real_time", "is_monitored"])),
        response_time=_parse_timestamp(payload.get("time")),
        raw_departure_json=json.dumps(departure, sort_keys=True),
    )


def iter_departures(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """Extract departures list from various API response shapes."""
    departures = payload.get("departures")
    if departures is None:
        departures = payload.get("result")
    if departures is None:
        departures = payload.get("Result")
    if departures is None:
        departures = _nested(payload, "data", "departures")
    if departures is None:
        departures = _nested(payload, "result", "departures")
    if departures is None:
        return []
    return departures


def read_s3_snapshots_raw(s3_base: str) -> List[Dict[str, Any]]:
    """Read JSON files from S3 — serverless compatible (no sparkContext needed)."""
    envelopes = []
    try:
        # List files using dbutils (works on serverless)
        files = dbutils.fs.ls(s3_base)
        json_paths = []
        for f in files:
            if f.isDir():
                # Traverse date subdirectories
                try:
                    sub_files = dbutils.fs.ls(f.path)
                    json_paths.extend([sf.path for sf in sub_files if sf.path.endswith(".json")])
                except Exception:
                    continue
            elif f.path.endswith(".json"):
                json_paths.append(f.path)

        if not json_paths:
            return []

        # Read all JSON files as single-line text, parse each
        text_df = spark.read.option("wholetext", True).text(json_paths)
        for row in text_df.collect():
            try:
                envelopes.append(json.loads(row.value))
            except (json.JSONDecodeError, TypeError):
                continue
    except Exception as e:
        print(f"Warning: Could not read S3 snapshots: {e}")
    return envelopes

# COMMAND ----------

run_id = str(uuid4())
ingestion_ts = datetime.now(timezone.utc)
rows: List[Row] = []
audit_rows: List[Row] = []

# ---- Read JSON snapshots deposited by Lambda into S3 ----
print(f"Scanning {s3_path} for new snapshots...")
envelopes = read_s3_snapshots_raw(s3_path)
print(f"Found {len(envelopes)} snapshot file(s) in S3")

for envelope in envelopes:
    stop_id = envelope.get("stop_id", "unknown")
    stop_name = envelope.get("stop_name")
    fetch_ts_str = envelope.get("fetch_timestamp")
    fetch_ts = _parse_timestamp(fetch_ts_str) if fetch_ts_str else ingestion_ts
    payload = envelope.get("api_response", {})

    try:
        stop_rows = [
            flatten_departure(run_id, stop_id, stop_name, payload, dep, fetch_ts)
            for dep in iter_departures(payload)
        ]
        rows.extend(stop_rows)
        audit_rows.append(
            Row(
                run_id=run_id,
                ingestion_timestamp=ingestion_ts,
                ingestion_date=ingestion_ts.date(),
                source_stop_id=stop_id,
                request_url=f"{s3_path}/{stop_id}",
                status="success",
                http_status_code=None,
                rows_extracted=len(stop_rows),
                duration_seconds=None,
                error_message=None,
            )
        )
    except Exception as exc:
        audit_rows.append(
            Row(
                run_id=run_id,
                ingestion_timestamp=ingestion_ts,
                ingestion_date=ingestion_ts.date(),
                source_stop_id=stop_id,
                request_url=f"{s3_path}/{stop_id}",
                status="failed",
                http_status_code=None,
                rows_extracted=0,
                duration_seconds=None,
                error_message=str(exc),
            )
        )

if rows:
    snapshot_df = spark.createDataFrame(rows, raw_schema)
else:
    snapshot_df = spark.createDataFrame([], raw_schema)

if audit_rows:
    audit_df = spark.createDataFrame(audit_rows, audit_schema)
else:
    audit_df = spark.createDataFrame([], audit_schema)

print(f"Parsed {len(rows)} departure rows from {len(envelopes)} files")
display(snapshot_df.orderBy(col("source_stop_id"), col("estimatedDeparture")))
display(audit_df.orderBy(col("source_stop_id")))

# COMMAND ----------

if rows:
    (
        snapshot_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "false")
        .saveAsTable(raw_table)
    )

(
    audit_df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "false")
    .saveAsTable(audit_table)
)

print(f"Wrote {len(rows)} rows to {raw_table}")
print(f"Wrote {len(audit_rows)} audit rows to {audit_table}")
print(f"run_id={run_id}")
