# CUMTD Departure Fetcher — AWS Lambda

Fetches real-time departures from the MTD Developer API and writes JSON snapshots to S3.

## Setup

### 1. Create S3 Bucket

```bash
aws s3 mb s3://cumtd-eta-drift
```

### 2. Create Lambda Function

```bash
# Zip the function
zip lambda_function.zip lambda_function.py

# Create the function (Python 3.12, arm64 for cost savings)
aws lambda create-function \
  --function-name cumtd-departure-fetcher \
  --runtime python3.12 \
  --architecture arm64 \
  --handler lambda_function.lambda_handler \
  --role arn:aws:iam::<ACCOUNT_ID>:role/<LAMBDA_ROLE> \
  --zip-file fileb://lambda_function.zip \
  --timeout 30 \
  --memory-size 128 \
  --environment Variables="{
    CUMTD_API_KEY=<YOUR_API_KEY>,
    CUMTD_API_BASE_URL=https://api.mtd.dev,
    S3_BUCKET=cumtd-eta-drift,
    S3_PREFIX=raw-departures,
    STOP_IDS=IT,
    LOOKAHEAD_MINUTES=60
  }"
```

### 3. IAM Role Permissions

The Lambda execution role needs:

```json
{
  "Effect": "Allow",
  "Action": ["s3:PutObject"],
  "Resource": "arn:aws:s3:::cumtd-eta-drift/raw-departures/*"
}
```

### 4. Schedule with EventBridge

```bash
# Run every 2 minutes to build snapshot history
aws events put-rule \
  --name cumtd-fetch-schedule \
  --schedule-expression "rate(2 minutes)"

aws lambda add-permission \
  --function-name cumtd-departure-fetcher \
  --statement-id eventbridge-invoke \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:<REGION>:<ACCOUNT_ID>:rule/cumtd-fetch-schedule

aws events put-targets \
  --rule cumtd-fetch-schedule \
  --targets "Id=1,Arn=arn:aws:lambda:<REGION>:<ACCOUNT_ID>:function:cumtd-departure-fetcher"
```

### 5. Grant Databricks Access to S3

Create an external location in Unity Catalog so the notebook can read from S3:

```sql
CREATE EXTERNAL LOCATION cumtd_s3
  URL 's3://cumtd-eta-drift/'
  WITH (STORAGE CREDENTIAL <your_storage_credential>);
```

Or use an existing IAM instance profile / storage credential that has read access to the bucket.

## S3 Key Layout

```
s3://cumtd-eta-drift/raw-departures/
  2026-04-13/
    12-00-00_IT.json
    12-02-00_IT.json
    ...
  2026-04-14/
    ...
```

Each JSON file contains:

```json
{
  "fetch_timestamp": "2026-04-13T12:00:00+00:00",
  "stop_id": "IT",
  "api_response": { ... }
}
```

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `CUMTD_API_KEY` | Yes | — | Your CUMTD developer API key |
| `CUMTD_API_BASE_URL` | No | `https://api.mtd.dev` | MTD Developer API base URL |
| `S3_BUCKET` | Yes | — | Target S3 bucket |
| `S3_PREFIX` | No | `raw-departures` | S3 key prefix |
| `STOP_IDS` | No | `IT` | Comma-separated stop IDs |
| `LOOKAHEAD_MINUTES` | No | `60` | Departure lookahead window |
| `MAX_RETRIES` | No | `3` | API retry attempts |
| `REQUEST_TIMEOUT_SECONDS` | No | `10` | HTTP timeout |
