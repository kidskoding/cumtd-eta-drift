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
# Route-based stop discovery
# ---------------------------------------------------------------------------
def _discover_stops() -> Set[str]:
    """Discover stop IDs by querying the CUMTD routes API.

    If ROUTE_FILTERS is set, only routes whose name contains one of the
    keywords are included.  If ROUTE_FILTERS is empty, ALL routes are
    included (default — captures every bus in the system).

    Uses REST endpoints:
      GET /routes              -> list all routes
      GET /routes/{id}/stops   -> stops served by a route
    """
    global _route_stops_cache, _cache_populated

    # Only return cache if it actually contains stops
    if _cache_populated and _route_stops_cache:
        print(f"[route-discovery] Returning {len(_route_stops_cache)} cached stops")
        return _route_stops_cache

    base = API_BASE_URL.rstrip("/")
    discovered: Set[str] = set()

    try:
        # 1. List all routes
        routes_url = f"{base}/routes"
        print(f"[route-discovery] Fetching routes from: {routes_url}")
        routes_data = _request_json(routes_url)
        print(f"[route-discovery] Routes response keys: {list(routes_data.keys())}")
        routes_list = _extract_list(routes_data)
        print(f"[route-discovery] Extracted {len(routes_list)} routes from response")

        # 2. Filter by keywords (or include all if no filters set)
        matching = []
        for route in routes_list:
            if not isinstance(route, dict):
                continue
            name = _get_str(route, ["route_short_name", "shortName",
                                    "short_name", "name"]).upper()
            route_id = _get_str(route, ["route_id", "id"])
            if not route_id:
                continue

            if not ROUTE_FILTERS:
                # No filters → include every route
                matching.append({"id": route_id, "name": name})
            else:
                for kw in ROUTE_FILTERS:
                    if kw in name:
                        matching.append({"id": route_id, "name": name})
                        break

        print(f"[route-discovery] Matched {len(matching)} routes "
              f"(filters={ROUTE_FILTERS or 'ALL'}): "
              f"{[m['name'] for m in matching]}")

        # 3. For each matched route, discover its stops
        for m in matching:
            try:
                encoded = quote(m["id"], safe="")
                stops_data = _request_json(f"{base}/routes/{encoded}/stops")
                stops_list = _extract_list(stops_data)

                for stop in stops_list:
                    if not isinstance(stop, dict):
                        continue
                    sid = _get_str(stop, ["stop_id", "id"])
                    if sid:
                        # Use group-level ID (strip boarding-point suffix)
                        group_id = sid.split(":")[0] if ":" in sid else sid
                        discovered.add(group_id)
            except Exception as exc:
                print(f"[route-discovery] Could not get stops for "
                      f"{m['name']} ({m['id']}): {exc}")

        print(f"[route-discovery] Discovered {len(discovered)} unique stop "
              f"groups: {sorted(discovered)}")

    except Exception as exc:
        print(f"[route-discovery] Failed to fetch routes list: {exc}")

    # Only cache if we actually found stops — never cache empty results
    if discovered:
        _route_stops_cache = discovered
        _cache_populated = True
    else:
        # Reset cache so next invocation retries
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
