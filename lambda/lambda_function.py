"""AWS Lambda: Fetch CUMTD departures and write to S3.

Environment variables (set in Lambda configuration):
  CUMTD_API_KEY        – Your CUMTD developer API key
  S3_BUCKET            – Target S3 bucket name  (e.g. cumtd-eta-drift)
  S3_PREFIX            – Key prefix              (default: raw-departures)
  STOP_IDS             – Comma-separated stop IDs (default: IT)
  LOOKAHEAD_MINUTES    – Departure lookahead     (default: 60)

S3 key layout:
  {S3_PREFIX}/{YYYY-MM-DD}/{HH-MM-SS}_{stop_id}.json

Schedule via EventBridge rule (e.g. every 2 minutes) to build snapshot history.
"""

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict
from urllib.parse import quote, urlencode

import boto3
import urllib3

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------
API_BASE_URL = os.environ.get("CUMTD_API_BASE_URL", "https://api.mtd.dev")
API_KEY = os.environ.get("CUMTD_API_KEY", "")
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "raw-departures").strip("/")
STOP_IDS = [s.strip() for s in os.environ.get("STOP_IDS", "IT").split(",") if s.strip()]
LOOKAHEAD_MINUTES = int(os.environ.get("LOOKAHEAD_MINUTES", "60"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT_SECONDS", "10"))

http = urllib3.PoolManager(timeout=urllib3.Timeout(total=REQUEST_TIMEOUT))
s3 = boto3.client("s3")


def _build_url(stop_id: str) -> str:
    base = API_BASE_URL.rstrip("/")
    encoded_stop_id = quote(stop_id, safe="")
    return f"{base}/stops/{encoded_stop_id}/departures"


def _fetch(stop_id: str) -> Dict[str, Any]:
    url = _build_url(stop_id)
    params = {"time": str(LOOKAHEAD_MINUTES)}
    headers = {"X-ApiKey": API_KEY} if API_KEY else {}

    query = urlencode(params)
    full_url = f"{url}?{query}"

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = http.request("GET", full_url, headers=headers)
            if resp.status == 200:
                return json.loads(resp.data.decode("utf-8"))
            last_err = RuntimeError(f"HTTP {resp.status}: {resp.data[:200]}")
        except Exception as exc:
            last_err = exc
        if attempt < MAX_RETRIES:
            time.sleep(min(2 ** (attempt - 1), 8))

    raise RuntimeError(
        f"Failed to fetch stop_id={stop_id} after {MAX_RETRIES} attempts: {last_err}"
    )


def _write_to_s3(stop_id: str, payload: Dict[str, Any], ts: datetime) -> str:
    date_part = ts.strftime("%Y-%m-%d")
    time_part = ts.strftime("%H-%M-%S")
    key = f"{S3_PREFIX}/{date_part}/{time_part}_{stop_id}.json"

    envelope = {
        "fetch_timestamp": ts.isoformat(),
        "stop_id": stop_id,
        "api_response": payload,
    }

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(envelope, default=str),
        ContentType="application/json",
    )
    return f"s3://{S3_BUCKET}/{key}"


def lambda_handler(event, context):
    ts = datetime.now(timezone.utc)
    results = []

    for stop_id in STOP_IDS:
        try:
            payload = _fetch(stop_id)
            s3_path = _write_to_s3(stop_id, payload, ts)
            results.append({"stop_id": stop_id, "status": "success", "s3_path": s3_path})
        except Exception as exc:
            results.append({"stop_id": stop_id, "status": "failed", "error": str(exc)})

    return {
        "statusCode": 200,
        "body": json.dumps(
            {"timestamp": ts.isoformat(), "results": results}, default=str
        ),
    }
