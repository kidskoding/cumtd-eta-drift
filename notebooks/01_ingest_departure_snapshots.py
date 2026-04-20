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
dbutils.widgets.text("lookahead_minutes", "60")
dbutils.widgets.text("stop_name_overrides", "IT=Illinois Terminal")

catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema").strip()
s3_bucket = dbutils.widgets.get("s3_bucket").strip()
s3_prefix = dbutils.widgets.get("s3_prefix").strip().strip("/")
lookahead_minutes = int(dbutils.widgets.get("lookahead_minutes"))
stop_name_override_text = dbutils.widgets.get("stop_name_overrides").strip()

database_name = f"{catalog}.{schema_name}" if catalog else schema_name
raw_table = f"{database_name}.raw_departure_snapshots"
audit_table = f"{database_name}.departure_ingestion_audit"
route_colors_table = f"{database_name}.route_colors"
s3_path = f"s3://{s3_bucket}/{s3_prefix}"
route_colors_s3_path = f"s3://{s3_bucket}/route-colors/route_colors.json"

print(f"Reading snapshots from: {s3_path}")
print(f"Writing to: {raw_table}")

# COMMAND ----------

def parse_stop_name_overrides(value: str) -> Dict[str, str]:
    """Parse comma-separated stop-name pairs, e.g. IT=Illinois Terminal,IU=Illini Union."""
    overrides: Dict[str, str] = {}
    if not value:
        return overrides

    for pair in value.split(","):
        if "=" not in pair:
            raise ValueError(f"Invalid stop_name_overrides entry: {pair!r}")
        stop_id, stop_name = pair.split("=", 1)
        stop_id = stop_id.strip()
        stop_name = stop_name.strip()
        if stop_id and stop_name:
            overrides[stop_id] = stop_name
    return overrides


stop_name_overrides = parse_stop_name_overrides(stop_name_override_text)

# COMMAND ----------

raw_schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("ingestion_date", DateType(), False),
        StructField("source_stop_id", StringType(), False),
        StructField("stop_id", StringType(), True),
        StructField("stop_name", StringType(), True),
        StructField("stop_display_name", StringType(), True),
        StructField("stop_group_id", StringType(), True),
        StructField("stop_group_name", StringType(), True),
        StructField("boarding_point_id", StringType(), True),
        StructField("boarding_point_name", StringType(), True),
        StructField("boarding_point_sub_name", StringType(), True),
        StructField("stop_code", StringType(), True),
        StructField("stop_city", StringType(), True),
        StructField("stop_latitude", DoubleType(), True),
        StructField("stop_longitude", DoubleType(), True),
        StructField("is_station", BooleanType(), True),
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

raw_column_types = {
    "stop_display_name": "STRING",
    "stop_group_id": "STRING",
    "stop_group_name": "STRING",
    "boarding_point_id": "STRING",
    "boarding_point_name": "STRING",
    "boarding_point_sub_name": "STRING",
    "stop_code": "STRING",
    "stop_city": "STRING",
    "stop_latitude": "DOUBLE",
    "stop_longitude": "DOUBLE",
    "is_station": "BOOLEAN",
}

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

existing_raw_columns = {field.name for field in spark.table(raw_table).schema.fields}
for column_name, column_type in raw_column_types.items():
    if column_name not in existing_raw_columns:
        spark.sql(f"ALTER TABLE {raw_table} ADD COLUMNS ({column_name} {column_type})")

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


def _boarding_point_map(stop_metadata: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    boarding_points = stop_metadata.get("boarding_points") or stop_metadata.get("boardingPoints") or []
    if not isinstance(boarding_points, list):
        return {}

    return {
        str(boarding_point.get("id")): boarding_point
        for boarding_point in boarding_points
        if isinstance(boarding_point, dict) and boarding_point.get("id")
    }


def _resolve_stop_metadata(
    stop_metadata: Dict[str, Any],
    departure_stop_id: str | None,
    fallback_stop_name: str | None,
) -> Dict[str, Any]:
    boarding_point = _boarding_point_map(stop_metadata).get(str(departure_stop_id))

    group_name = (
        stop_metadata.get("stop_group_name")
        or stop_metadata.get("name")
        or fallback_stop_name
    )
    group_id = stop_metadata.get("stop_group_id") or stop_metadata.get("id")

    if boarding_point:
        boarding_point_name = boarding_point.get("name")
        boarding_point_sub_name = boarding_point.get("sub_name") or boarding_point.get("subName")
        display_name = boarding_point_name or (
            f"{group_name} ({boarding_point_sub_name})"
            if group_name and boarding_point_sub_name
            else group_name
        )
        return {
            "stop_display_name": display_name,
            "stop_group_id": group_id,
            "stop_group_name": group_name,
            "boarding_point_id": boarding_point.get("id"),
            "boarding_point_name": boarding_point_name,
            "boarding_point_sub_name": boarding_point_sub_name,
            "stop_code": boarding_point.get("stop_code") or boarding_point.get("stopCode") or stop_metadata.get("stop_code") or stop_metadata.get("stopCode"),
            "stop_city": stop_metadata.get("city"),
            "stop_latitude": _as_float(boarding_point.get("latitude")),
            "stop_longitude": _as_float(boarding_point.get("longitude")),
            "is_station": _as_bool(stop_metadata.get("is_station") if "is_station" in stop_metadata else stop_metadata.get("isStation")),
        }

    return {
        "stop_display_name": group_name,
        "stop_group_id": group_id,
        "stop_group_name": group_name,
        "boarding_point_id": departure_stop_id if departure_stop_id and ":" in departure_stop_id else None,
        "boarding_point_name": None,
        "boarding_point_sub_name": None,
        "stop_code": stop_metadata.get("stop_code") or stop_metadata.get("stopCode"),
        "stop_city": stop_metadata.get("city"),
        "stop_latitude": _as_float(stop_metadata.get("latitude")),
        "stop_longitude": _as_float(stop_metadata.get("longitude")),
        "is_station": _as_bool(stop_metadata.get("is_station") if "is_station" in stop_metadata else stop_metadata.get("isStation")),
    }

# COMMAND ----------

def flatten_departure(
    run_id: str,
    source_stop_id: str,
    source_stop_name: str | None,
    source_stop_metadata: Dict[str, Any],
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
    departure_stop_id = _first_present(departure, ["stop_id", "stopId"]) or _first_present(stop, ["stop_id", "id"])
    stop_context = _resolve_stop_metadata(source_stop_metadata, departure_stop_id, source_stop_name)
    stop_name = (
        _first_present(departure, ["stop_name", "stopName"])
        or _first_present(stop, ["stop_name", "name"])
        or stop_context["stop_group_name"]
        or source_stop_name
    )

    return Row(
        run_id=run_id,
        ingestion_timestamp=ingestion_ts,
        ingestion_date=ingestion_ts.date(),
        source_stop_id=source_stop_id,
        stop_id=departure_stop_id,
        stop_name=stop_name,
        stop_display_name=stop_context["stop_display_name"] or stop_name,
        stop_group_id=stop_context["stop_group_id"],
        stop_group_name=stop_context["stop_group_name"],
        boarding_point_id=stop_context["boarding_point_id"],
        boarding_point_name=stop_context["boarding_point_name"],
        boarding_point_sub_name=stop_context["boarding_point_sub_name"],
        stop_code=stop_context["stop_code"],
        stop_city=stop_context["stop_city"],
        stop_latitude=stop_context["stop_latitude"],
        stop_longitude=stop_context["stop_longitude"],
        is_station=stop_context["is_station"],
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
    stop_name = envelope.get("stop_name") or stop_name_overrides.get(stop_id)
    stop_metadata = envelope.get("stop_metadata") or {}
    fetch_ts_str = envelope.get("fetch_timestamp")
    fetch_ts = _parse_timestamp(fetch_ts_str) if fetch_ts_str else ingestion_ts
    payload = envelope.get("api_response", {})

    try:
        stop_rows = [
            flatten_departure(run_id, stop_id, stop_name, stop_metadata, payload, dep, fetch_ts)
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

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType as ST

route_colors_schema = StructType([
    StructField("route_short_name", ST(), False),
    StructField("route_group_name", ST(), True),
    StructField("hex_color", ST(), True),
    StructField("text_hex_color", ST(), True),
])

# Always ensure the table exists so downstream dbt models never fail on a missing table.
spark.createDataFrame([], route_colors_schema).write.format("delta").mode("ignore").saveAsTable(route_colors_table)

try:
    color_text_df = spark.read.option("wholetext", True).text(route_colors_s3_path)
    color_json_str = color_text_df.collect()[0].value
    color_data = json.loads(color_json_str)
    color_df = spark.createDataFrame(color_data, route_colors_schema)
    (
        color_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(route_colors_table)
    )
    print(f"Wrote {len(color_data)} route color rows to {route_colors_table}")
except Exception as exc:
    print(f"Warning: Could not load route colors from S3 (table exists but may be empty): {exc}")
