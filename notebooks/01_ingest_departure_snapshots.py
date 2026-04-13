# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingest CUMTD Departure Snapshots
# MAGIC
# MAGIC Poll the CUMTD stops departures endpoint for one or more stops and append a flattened, typed snapshot to Delta.

# COMMAND ----------

from datetime import datetime, timezone
import json
import time
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

import requests
from pyspark.sql import Row
from pyspark.sql.functions import col
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
dbutils.widgets.text("api_base_url", "https://developer.mtd.org/api")
dbutils.widgets.text("api_version", "v3")
dbutils.widgets.text("departures_endpoint_path", "json/getdeparturesbystop")
dbutils.widgets.text("stop_ids", "it")
dbutils.widgets.text("lookahead_minutes", "60")
dbutils.widgets.text("request_timeout_seconds", "30")
dbutils.widgets.text("max_retries", "3")
dbutils.widgets.text("api_key_scope", "")
dbutils.widgets.text("api_key_name", "")
dbutils.widgets.text("api_key_param", "key")

catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema").strip()
api_base_url = dbutils.widgets.get("api_base_url").strip().rstrip("/")
api_version = dbutils.widgets.get("api_version").strip().strip("/")
departures_endpoint_path = dbutils.widgets.get("departures_endpoint_path").strip().strip("/")
stop_ids = [s.strip() for s in dbutils.widgets.get("stop_ids").split(",") if s.strip()]
lookahead_minutes = int(dbutils.widgets.get("lookahead_minutes"))
request_timeout_seconds = int(dbutils.widgets.get("request_timeout_seconds"))
max_retries = int(dbutils.widgets.get("max_retries"))
api_key_scope = dbutils.widgets.get("api_key_scope").strip()
api_key_name = dbutils.widgets.get("api_key_name").strip()
api_key_param = dbutils.widgets.get("api_key_param").strip() or "key"

if not stop_ids:
    raise ValueError("Provide at least one stop_id in the stop_ids widget.")

database_name = f"{catalog}.{schema_name}" if catalog else schema_name
raw_table = f"{database_name}.raw_departure_snapshots"
audit_table = f"{database_name}.departure_ingestion_audit"


class DepartureFetchError(RuntimeError):
    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code

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

def build_departures_url(stop_id: str) -> str:
    # Keep this single function isolated so the exact v3 path can be changed without touching parsing or writes.
    return f"{api_base_url}/{api_version}/{departures_endpoint_path}"


def build_query_params(stop_id: str) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "stop_id": stop_id,
        "pt": lookahead_minutes,
    }

    if api_key_scope and api_key_name:
        params[api_key_param] = dbutils.secrets.get(scope=api_key_scope, key=api_key_name)

    return params


def fetch_departures(stop_id: str) -> tuple[Dict[str, Any], Optional[int]]:
    last_error: Optional[Exception] = None
    last_status_code: Optional[int] = None
    url = build_departures_url(stop_id)
    params = build_query_params(stop_id)

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=request_timeout_seconds)
            last_status_code = response.status_code
            response.raise_for_status()
            return response.json(), response.status_code
        except requests.RequestException as exc:
            last_error = exc
            if attempt == max_retries:
                break
            time.sleep(min(2 ** (attempt - 1), 10))

    raise DepartureFetchError(
        f"Failed to fetch departures for stop_id={stop_id} after {max_retries} attempts",
        status_code=last_status_code,
    ) from last_error


def iter_departures(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    departures = payload.get("departures")
    if departures is None:
        departures = _nested(payload, "data", "departures")
    if departures is None:
        departures = _nested(payload, "result", "departures")
    if departures is None:
        return []
    return departures


def flatten_departure(
    run_id: str,
    source_stop_id: str,
    payload: Dict[str, Any],
    departure: Dict[str, Any],
    ingestion_ts: datetime,
) -> Row:
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
        stop_name=_first_present(departure, ["stop_name", "stopName"]) or _first_present(stop, ["stop_name", "name"]),
        trip_id=_first_present(departure, ["trip_id", "tripId"]) or _first_present(trip, ["trip_id", "id"]),
        route_id=_first_present(departure, ["route_id", "routeId"]) or _first_present(route, ["route_id", "id"]),
        route_short_name=_first_present(departure, ["route_short_name", "routeShortName", "headsign"])
        or _first_present(route, ["route_short_name", "short_name", "route_id"]),
        scheduledDeparture=_parse_timestamp(scheduled),
        estimatedDeparture=_parse_timestamp(estimated),
        recordedTime=_parse_timestamp(recorded),
        minutesTillDeparture=_as_float(_first_present(departure, ["minutesTillDeparture", "minutes_till_departure", "mins"])),
        isRealTime=_as_bool(_first_present(departure, ["isRealTime", "is_real_time", "is_monitored"])),
        response_time=_parse_timestamp(payload.get("time")),
        raw_departure_json=json.dumps(departure, sort_keys=True),
    )

# COMMAND ----------

run_id = str(uuid4())
ingestion_ts = datetime.now(timezone.utc)
rows: List[Row] = []
audit_rows: List[Row] = []

for stop_id in stop_ids:
    started_at = time.monotonic()
    request_url = build_departures_url(stop_id)
    try:
        payload, http_status_code = fetch_departures(stop_id)
        stop_rows = [
            flatten_departure(run_id, stop_id, payload, departure, ingestion_ts)
            for departure in iter_departures(payload)
        ]
        rows.extend(stop_rows)
        audit_rows.append(
            Row(
                run_id=run_id,
                ingestion_timestamp=ingestion_ts,
                ingestion_date=ingestion_ts.date(),
                source_stop_id=stop_id,
                request_url=request_url,
                status="success",
                http_status_code=http_status_code,
                rows_extracted=len(stop_rows),
                duration_seconds=float(time.monotonic() - started_at),
                error_message=None,
            )
        )
    except Exception as exc:
        http_status_code = exc.status_code if isinstance(exc, DepartureFetchError) else None
        audit_rows.append(
            Row(
                run_id=run_id,
                ingestion_timestamp=ingestion_ts,
                ingestion_date=ingestion_ts.date(),
                source_stop_id=stop_id,
                request_url=request_url,
                status="failed",
                http_status_code=http_status_code,
                rows_extracted=0,
                duration_seconds=float(time.monotonic() - started_at),
                error_message=str(exc),
            )
        )

if rows:
    snapshot_df = spark.createDataFrame(rows, raw_schema)
else:
    snapshot_df = spark.createDataFrame([], raw_schema)

audit_df = spark.createDataFrame(audit_rows, audit_schema)

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
