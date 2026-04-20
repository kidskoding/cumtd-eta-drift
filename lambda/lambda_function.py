"""AWS Lambda: Fetch CUMTD departures and write to S3.

Environment variables (set in Lambda configuration):
  CUMTD_API_KEY        – Your CUMTD developer API key
  S3_BUCKET            – Target S3 bucket name  (e.g. cumtd-eta-drift)
  S3_PREFIX            – Key prefix              (default: raw-departures)
  STOP_IDS             – Comma-separated explicit stop IDs to always include
                         (default: empty). Merged with auto-discovered stops.
  ROUTE_FILTERS        – Comma-separated route name keywords to restrict
                         discovery to (e.g. "YELLOW,GREEN,GOLD"). If blank
                         or unset, ALL routes are included automatically.
  LOOKAHEAD_MINUTES    – Departure lookahead     (default: 60)

S3 key layout:
  {S3_PREFIX}/{YYYY-MM-DD}/{HH-MM-SS}_{stop_id}.json

Schedule via EventBridge rule (e.g. every 2 minutes) to build snapshot history.
"""

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Set
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
STOP_IDS = [s.strip() for s in os.environ.get("STOP_IDS", "").split(",") if s.strip()]
ROUTE_FILTERS = [
    s.strip().upper()
    for s in os.environ.get("ROUTE_FILTERS", "").split(",")
    if s.strip()
]
LOOKAHEAD_MINUTES = int(os.environ.get("LOOKAHEAD_MINUTES", "60"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT_SECONDS", "10"))
# Cap on stop groups when fallback /stops endpoint is used
MAX_STOPS = int(os.environ.get("MAX_STOPS", "150"))

http = urllib3.PoolManager(timeout=urllib3.Timeout(total=REQUEST_TIMEOUT))
s3 = boto3.client("s3")

# Cache discovered stops across warm Lambda invocations
_route_stops_cache: Set[str] = set()
_cache_populated = False


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def _request_json(url: str, params: Dict[str, str] | None = None) -> Dict[str, Any]:
    if params is None:
        params = {}
    # Include API key as query param (some CUMTD endpoints expect ?key=...)
    if API_KEY and "key" not in params:
        params["key"] = API_KEY
    query = f"?{urlencode(params)}" if params else ""
    full_url = f"{url}{query}"
    # Also send as header for endpoints that prefer it
    headers = {"X-ApiKey": API_KEY} if API_KEY else {}

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = http.request("GET", full_url, headers=headers)
            if resp.status == 200:
                return json.loads(resp.data.decode("utf-8"))
            last_err = RuntimeError(f"HTTP {resp.status}: {resp.data[:300]}")
        except Exception as exc:
            last_err = exc
        if attempt < MAX_RETRIES:
            time.sleep(min(2 ** (attempt - 1), 8))

    raise RuntimeError(
        f"Failed to fetch {full_url} after {MAX_RETRIES} attempts: {last_err}"
    )


# ---------------------------------------------------------------------------
# Stop / departure helpers
# ---------------------------------------------------------------------------
def _build_stop_url(stop_id: str) -> str:
    base = API_BASE_URL.rstrip("/")
    encoded_stop_id = quote(stop_id, safe="")
    return f"{base}/stops/{encoded_stop_id}"


def _build_departures_url(stop_id: str) -> str:
    return f"{_build_stop_url(stop_id)}/departures"


def _fetch_departures(stop_id: str) -> Dict[str, Any]:
    return _request_json(
        _build_departures_url(stop_id),
        params={"time": str(LOOKAHEAD_MINUTES)},
    )


def _fetch_stop_metadata(stop_id: str) -> Dict[str, Any]:
    return _request_json(_build_stop_url(stop_id))


def _extract_stop_metadata(stop_metadata: Dict[str, Any]) -> Dict[str, Any]:
    result = stop_metadata.get("result") or stop_metadata.get("Result") or {}
    if not isinstance(result, dict):
        return {}

    location = result.get("location") or {}
    boarding_points = []
    for boarding_point in result.get("boardingPoints") or []:
        if not isinstance(boarding_point, dict):
            continue
        bp_location = boarding_point.get("location") or {}
        boarding_points.append(
            {
                "id": boarding_point.get("id"),
                "name": boarding_point.get("name"),
                "sub_name": boarding_point.get("subName"),
                "stop_code": boarding_point.get("stopCode"),
                "url": boarding_point.get("url"),
                "is_accessible": boarding_point.get("isAccessible"),
                "latitude": bp_location.get("latitude"),
                "longitude": bp_location.get("longitude"),
            }
        )

    return {
        "stop_group_id": result.get("id"),
        "stop_group_name": result.get("name"),
        "stop_code": result.get("stopCode"),
        "url": result.get("url"),
        "city": result.get("city"),
        "is_station": result.get("isStation"),
        "is_accessible": result.get("isAccessible"),
        "latitude": location.get("latitude"),
        "longitude": location.get("longitude"),
        "boarding_points": boarding_points,
    }


# ---------------------------------------------------------------------------
# Active-stop discovery
# ---------------------------------------------------------------------------
def _stops_from_list(stops_list: list) -> Set[str]:
    """Deduplicate a raw stops list into stop group IDs."""
    out: Set[str] = set()
    for stop in stops_list:
        if not isinstance(stop, dict):
            continue
        sid = _get_str(stop, ["stop_id", "id"])
        if sid:
            out.add(sid.split(":")[0] if ":" in sid else sid)
    return out


def _discover_stops() -> Set[str]:
    """Discover stop group IDs served by active routes.

    Strategy (fastest-first):
    1. GET /routes/groups  →  collect gtfsRoute names per group
       (respects ROUTE_FILTERS if set)
    2. GET /routes/{id}/stops  per route  →  collect stop group IDs
       Expected result: ~50-150 unique stop groups
    3. If route-endpoint yields nothing (404 / unsupported), fall back to
       GET /stops but cap at MAX_STOPS unique stop groups so Lambda does
       not time out polling thousands of stops.
    """
    global _route_stops_cache, _cache_populated

    if _cache_populated and _route_stops_cache:
        print(f"[route-discovery] Returning {len(_route_stops_cache)} cached stops")
        return _route_stops_cache

    base = API_BASE_URL.rstrip("/")
    discovered: Set[str] = set()

    # --- Phase 1: collect active route identifiers from /routes/groups ---
    active_route_ids: List[str] = []
    try:
        groups_data = _request_json(f"{base}/routes/groups")
        groups = groups_data.get("result") or groups_data.get("Result") or []
        for group in groups:
            group_name = (group.get("routeGroupName") or "").upper()
            if ROUTE_FILTERS and not any(f in group_name for f in ROUTE_FILTERS):
                continue
            for route in group.get("routes") or []:
                for gtfs_name in route.get("gtfsRoutes") or []:
                    if gtfs_name and str(gtfs_name) not in active_route_ids:
                        active_route_ids.append(str(gtfs_name))
                rid = (route.get("routeId")
                       or route.get("id")
                       or route.get("route_id"))
                if rid and str(rid) not in active_route_ids:
                    active_route_ids.append(str(rid))
        print(f"[route-discovery] {len(active_route_ids)} active route identifiers")
    except Exception as exc:
        print(f"[route-discovery] Could not fetch /routes/groups: {exc}")

    # --- Phase 2: per-route stops endpoint ---
    for rid in active_route_ids:
        try:
            stops_data = _request_json(
                f"{base}/routes/{quote(rid, safe='')}/stops"
            )
            discovered |= _stops_from_list(_extract_list(stops_data))
        except Exception:
            pass  # endpoint may not exist; silence and move on

    print(f"[route-discovery] Route-based discovery: {len(discovered)} stop groups")

    # --- Phase 3: fallback — /stops capped at MAX_STOPS ---
    if not discovered:
        print(f"[route-discovery] Falling back to /stops (cap={MAX_STOPS})")
        try:
            stops_data = _request_json(f"{base}/stops")
            for stop in _extract_list(stops_data):
                if not isinstance(stop, dict):
                    continue
                sid = _get_str(stop, ["stop_id", "id"])
                if not sid:
                    continue
                discovered.add(sid.split(":")[0] if ":" in sid else sid)
                if len(discovered) >= MAX_STOPS:
                    break
            print(f"[route-discovery] Fallback: {len(discovered)} stop groups")
        except Exception as exc:
            print(f"[route-discovery] /stops fallback failed: {exc}")

    if discovered:
        _route_stops_cache = discovered
        _cache_populated = True
        print(f"[route-discovery] Final stop set: {len(discovered)} groups")
    else:
        _cache_populated = False
        print("[route-discovery] WARNING: No stops found — will retry on next invocation")

    return discovered


def _extract_list(data: Dict[str, Any]) -> list:
    """Pull the list of items from an API response, tolerating several
    common envelope shapes."""
    for key in ("result", "results", "routes", "stops", "data"):
        val = data.get(key)
        if isinstance(val, list):
            return val
    for key in ("result", "results", "routes", "stops", "data"):
        val = data.get(key)
        if isinstance(val, dict):
            return list(val.values())
    return []


def _get_str(obj: dict, keys: list) -> str:
    """Return the first non-empty string value for the given keys."""
    for k in keys:
        v = obj.get(k)
        if v:
            return str(v)
    return ""


# ---------------------------------------------------------------------------
# Route colors
# ---------------------------------------------------------------------------
ROUTE_COLORS_KEY = "route-colors/route_colors.json"

def _fetch_route_colors() -> List[Dict[str, str]]:
    base = API_BASE_URL.rstrip("/")
    data = _request_json(f"{base}/routes/groups")
    groups = data.get("result") or data.get("Result") or []
    rows = []
    for group in groups:
        color = group.get("color") or "808285"
        text_color = group.get("textColor") or "ffffff"
        group_name = group.get("routeGroupName") or ""
        for route in group.get("routes") or []:
            for gtfs_name in route.get("gtfsRoutes") or []:
                rows.append({
                    "route_short_name": gtfs_name,
                    "route_group_name": group_name,
                    "hex_color": color,
                    "text_hex_color": text_color,
                })
    return rows


def _write_route_colors_to_s3(rows: List[Dict[str, str]]) -> None:
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=ROUTE_COLORS_KEY,
        Body=json.dumps(rows, default=str),
        ContentType="application/json",
    )


# ---------------------------------------------------------------------------
# S3 writer
# ---------------------------------------------------------------------------
def _write_to_s3(stop_id: str, stop_metadata: Dict[str, Any],
                 payload: Dict[str, Any], ts: datetime) -> str:
    date_part = ts.strftime("%Y-%m-%d")
    time_part = ts.strftime("%H-%M-%S")
    key = f"{S3_PREFIX}/{date_part}/{time_part}_{stop_id}.json"

    envelope = {
        "fetch_timestamp": ts.isoformat(),
        "stop_id": stop_id,
        "stop_name": stop_metadata.get("stop_group_name"),
        "stop_metadata": stop_metadata,
        "api_response": payload,
    }

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(envelope, default=str),
        ContentType="application/json",
    )
    return f"s3://{S3_BUCKET}/{key}"


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------
def lambda_handler(event, context):
    ts = datetime.now(timezone.utc)
    results = []

    # Discover stops from routes, merge with any explicit STOP_IDS
    all_stop_ids = list(STOP_IDS)
    discovered = _discover_stops()
    for sid in discovered:
        if sid not in all_stop_ids:
            all_stop_ids.append(sid)

    if not all_stop_ids:
        return {
            "statusCode": 200,
            "body": json.dumps(
                {"timestamp": ts.isoformat(),
                 "error": "No stops discovered. Check API connectivity.",
                 "api_base_url": API_BASE_URL,
                 "api_key_set": bool(API_KEY),
                 "explicit_stop_ids": STOP_IDS,
                 "route_filters": ROUTE_FILTERS},
                default=str,
            ),
        }

    print(f"[lambda] Polling {len(all_stop_ids)} stops: {all_stop_ids}")

    try:
        color_rows = _fetch_route_colors()
        _write_route_colors_to_s3(color_rows)
        print(f"[lambda] Wrote {len(color_rows)} route color rows to s3://{S3_BUCKET}/{ROUTE_COLORS_KEY}")
    except Exception as exc:
        print(f"[lambda] Warning: Could not refresh route colors: {exc}")

    for stop_id in all_stop_ids:
        try:
            stop_metadata = _extract_stop_metadata(_fetch_stop_metadata(stop_id))
            payload = _fetch_departures(stop_id)
            s3_path = _write_to_s3(stop_id, stop_metadata, payload, ts)
            results.append(
                {
                    "stop_id": stop_id,
                    "stop_name": stop_metadata.get("stop_group_name"),
                    "status": "success",
                    "s3_path": s3_path,
                }
            )
        except Exception as exc:
            results.append({"stop_id": stop_id, "status": "failed",
                            "error": str(exc)})

    return {
        "statusCode": 200,
        "body": json.dumps(
            {"timestamp": ts.isoformat(), "results": results}, default=str
        ),
    }
