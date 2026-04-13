# CUMTD v3 Transit ETA Drift Audit

Databricks-first project for auditing how CUMTD real-time departure estimates drift over repeated observations of the same trip at the same stop.

## Layers

1. **Ingestion**
   - Notebook: `notebooks/01_ingest_departure_snapshots.py`
   - Reads JSON snapshots written by the AWS Lambda departure fetcher into S3.
   - Flattens API responses into typed rows.
   - Appends to Delta table `raw_departure_snapshots`.

2. **Feature / Drift Computation**
   - Notebook: `notebooks/02_compute_departure_drift_metrics.py`
   - Computes estimate offset, drift, update counts, monotonicity violation counts, and time-to-departure features.
   - Writes Delta table `departure_drift_metrics`.
   - dbt scaffold: `dbt/` contains the preferred production transformation path once raw ingestion is verified.

3. **Analysis + Visualization**
   - Notebook: `notebooks/03_analyze_departure_drift.py`
   - Produces a single-trip ETA line plot, drift histogram, and top stops/routes by average drift.

4. **Verification**
   - Notebook: `notebooks/04_verify_pipeline_health.py`
   - Checks the raw, audit, staging, mart, and summary tables after an end-to-end run.

## Databricks Setup

Import the files in `notebooks/` as Databricks source notebooks.

Recommended job order:

1. Run `01_ingest_departure_snapshots.py` on a schedule, for example every 1-5 minutes during service windows.
2. Run `02_compute_departure_drift_metrics.py` after ingestion, or as a separate batch job.
3. Run dbt from the `dbt/` directory once raw ingestion is populated.
4. Run `04_verify_pipeline_health.py` to confirm the raw rows, stop names, ETAs, and dbt models are healthy.
5. Run `03_analyze_departure_drift.py` interactively or on a reporting cadence.

There is also a Databricks Asset Bundle scaffold in `databricks.yml`. It defines a paused job that runs ingestion and then drift metric computation every five minutes. The default Unity Catalog target is `workspace.cumtd_eta_drift`; set `s3_bucket`, `s3_prefix`, and `stop_ids` for your workspace before enabling the schedule.

## dbt Transformation Layer

The `dbt/` directory is scaffolded for dbt on Databricks. Ingestion stays in the Databricks Python notebook; dbt starts from the raw Delta tables and owns the clean, intermediate, and mart models.

```text
raw_departure_snapshots
departure_ingestion_audit
  -> stg_departure_snapshots
  -> stg_departure_ingestion_audit
  -> int_departure_eta_updates
  -> departure_drift_metrics
  -> daily_stop_drift_summary
  -> daily_route_drift_summary
```

To configure dbt locally, copy `dbt/profiles.yml.example` to your dbt profiles directory or to `dbt/profiles.yml`, then fill in your Databricks host, catalog, schema, SQL warehouse or cluster HTTP path, and token.

Example commands:

```bash
cd dbt
../.venv/bin/dbt debug --profiles-dir .
../.venv/bin/dbt run --profiles-dir . --vars '{"catalog": "workspace", "raw_schema": "cumtd_eta_drift", "mart_schema": "cumtd_eta_drift"}'
../.venv/bin/dbt test --profiles-dir . --vars '{"catalog": "workspace", "raw_schema": "cumtd_eta_drift", "mart_schema": "cumtd_eta_drift"}'
```

On this machine, `/opt/homebrew/bin/dbt` is the dbt Cloud CLI. Use the project virtualenv binary above for dbt Core unless your PATH points to a separate dbt Core install.

Keep `notebooks/02_compute_departure_drift_metrics.py` as a fallback/reference while validating the dbt output against real ingested rows.

## Local Project Setup

This repo is initialized with `uv`.

```bash
uv sync
uv run python -m py_compile notebooks/01_ingest_departure_snapshots.py notebooks/02_compute_departure_drift_metrics.py notebooks/03_analyze_departure_drift.py notebooks/04_verify_pipeline_health.py
```

The local environment is useful for dependency locking and syntax checks. The notebooks still require a Databricks runtime for `spark`, `dbutils`, Delta tables, and job execution.

## Configuration

The ingestion notebook uses Databricks widgets:

- `catalog`: Unity Catalog catalog name, default `workspace`.
- `schema`: target schema/database, default `cumtd_eta_drift`.
- `s3_bucket`: S3 bucket containing Lambda departure snapshots, default `cumtd-eta-drift`.
- `s3_prefix`: S3 prefix containing Lambda departure snapshots, default `raw-departures`.
- `stop_ids`: comma-separated stop IDs to query.
- `lookahead_minutes`: retained for compatibility with the Lambda fetch configuration.
- `stop_name_overrides`: optional comma-separated stop name fallbacks, default `IT=Illinois Terminal`.

The live API call happens in `lambda/lambda_function.py`, which uses `GET https://api.mtd.dev/stops/{stopId}/departures?time=...` with the `X-ApiKey` header.
The Lambda also calls `GET https://api.mtd.dev/stops/{stopId}` so new S3 snapshots include `stop_name`; the override widget keeps older S3 snapshots usable.
Stop metadata is carried through the raw and dbt tables so charts can use labels like `Illinois Terminal (Platform 5)` instead of only API IDs like `IT:5`.

## Delta Tables

### raw_departure_snapshots

One row per observed departure prediction at ingestion time.

| Column | Type |
| --- | --- |
| run_id | string |
| ingestion_timestamp | timestamp |
| ingestion_date | date |
| source_stop_id | string |
| stop_id | string |
| stop_name | string |
| stop_display_name | string |
| stop_group_id | string |
| stop_group_name | string |
| boarding_point_id | string |
| boarding_point_name | string |
| boarding_point_sub_name | string |
| stop_code | string |
| stop_city | string |
| stop_latitude | double |
| stop_longitude | double |
| is_station | boolean |
| trip_id | string |
| route_id | string |
| route_short_name | string |
| scheduledDeparture | timestamp |
| estimatedDeparture | timestamp |
| recordedTime | timestamp |
| minutesTillDeparture | double |
| isRealTime | boolean |
| response_time | timestamp |
| raw_departure_json | string |

### departure_drift_metrics

One row per stop/trip/route grouping.

| Column | Type |
| --- | --- |
| metric_key | string |
| first_ingestion_date | date |
| last_ingestion_date | date |
| stop_id | string |
| stop_name | string |
| stop_display_name | string |
| stop_group_id | string |
| stop_group_name | string |
| boarding_point_id | string |
| boarding_point_name | string |
| boarding_point_sub_name | string |
| stop_code | string |
| stop_city | string |
| stop_latitude | double |
| stop_longitude | double |
| is_station | boolean |
| trip_id | string |
| route_id | string |
| route_short_name | string |
| scheduledDeparture | timestamp |
| first_ingestion_timestamp | timestamp |
| last_ingestion_timestamp | timestamp |
| first_estimatedDeparture | timestamp |
| last_estimatedDeparture | timestamp |
| min_estimatedDeparture | timestamp |
| max_estimatedDeparture | timestamp |
| avg_estimate_offset_minutes | double |
| min_estimate_offset_minutes | double |
| max_estimate_offset_minutes | double |
| drift_minutes | double |
| update_count | long |
| monotonicity_violation_count | long |
| max_forward_jump_minutes | double |
| max_backward_jump_minutes | double |
| avg_time_to_departure_minutes | double |
| min_time_to_departure_minutes | double |
| max_time_to_departure_minutes | double |
| metric_computed_at | timestamp |

### departure_ingestion_audit

One row per stop queried in each ingestion run.

| Column | Type |
| --- | --- |
| run_id | string |
| ingestion_timestamp | timestamp |
| ingestion_date | date |
| source_stop_id | string |
| request_url | string |
| status | string |
| http_status_code | integer |
| rows_extracted | long |
| duration_seconds | double |
| error_message | string |

## Notes

- The ingestion parser accepts both the requested v3-style field names (`scheduledDeparture`, `estimatedDeparture`) and common CUMTD legacy names (`scheduled`, `expected`).
- API failures are captured in `departure_ingestion_audit` per stop, so one bad stop does not erase successful observations from the same scheduled run.
- `run_id` ties raw snapshot rows and audit rows back to the same ingestion attempt; `ingestion_date` supports date filtering, partitioning, and daily dbt summaries.
- `monotonicity_violation_count` counts changes where the estimated departure time moves earlier between snapshots for the same stop/trip. This is a practical instability signal; update the rule later if the project adopts a stricter domain definition.
- The notebooks are structured so streaming ingestion, Databricks Workflows, dashboards, ML correction models, and anomaly detection can be added without replacing the core table contracts.
