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
    ROUTE_FILTERS=YELLOW,GREEN,GOLD,TEAL,SILVER,ILLINI,
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

## Route-Based Stop Discovery

The Lambda can automatically discover which stops to poll based on route names. Set `ROUTE_FILTERS` to a comma-separated list of route name keywords:

```
ROUTE_FILTERS=YELLOW,GREEN,GOLD,TEAL,SILVER,ILLINI
```

On each invocation the Lambda will:
1. Call `GET /routes` to list all CUMTD routes
2. Filter to routes whose name contains any of your keywords (e.g. "1N YELLOW", "10E GOLD")
3. Call `GET /routes/{id}/stops` for each match to discover stop IDs
4. Merge discovered stops with `STOP_IDS` and poll departures from all of them

This means you don't need to manually look up stop IDs for every route — just set the route color names and the Lambda figures out the rest. Results are cached across warm invocations to minimize API calls.

## S3 Key Layout

```
s3://cumtd-eta-drift/raw-departures/
  2026-04-13/
    12-00-00_IT.json
    12-02-00_IT.json
    12-00-00_PAR.json
    ...
  2026-04-14/
    ...
```

Each JSON file contains:

```json
{
  "fetch_timestamp": "2026-04-13T12:00:00+00:00",
  "stop_id": "IT",
  "stop_name": "Illinois Terminal",
  "stop_metadata": {
    "stop_group_id": "IT",
    "stop_group_name": "Illinois Terminal",
    "boarding_points": [
      {
        "id": "IT:5",
        "name": "Illinois Terminal (Platform 5)",
        "sub_name": "Platform 5",
        "latitude": 40.115,
        "longitude": -88.241
      }
    ]
  },
  "api_response": { ... }
}
```

## GitHub Actions Deployment

The repo includes `.github/workflows/deploy-lambda.yml`, which packages `lambda_function.py` and updates the Lambda function whenever that file changes on `master`. You can also run it manually from the GitHub Actions tab.

Configure these GitHub settings before using it:

| Setting | Type | Example |
|---|---|---|
| `AWS_ROLE_TO_ASSUME` | Secret | `arn:aws:iam::<ACCOUNT_ID>:role/github-cumtd-lambda-deploy` |
| `AWS_REGION` | Variable | `us-east-1` |
| `LAMBDA_FUNCTION_NAME` | Variable | `cumtd-departure-fetcher` |

The deploy role should trust GitHub's OIDC provider and only need Lambda update permissions for this function:

Trust policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:kidskoding/cumtd-eta-drift:*"
        }
      }
    }
  ]
}
```

Permissions policy:

```json
{
  "Effect": "Allow",
  "Action": [
    "lambda:UpdateFunctionCode",
    "lambda:GetFunction",
    "lambda:GetFunctionConfiguration"
  ],
  "Resource": "arn:aws:lambda:<REGION>:<ACCOUNT_ID>:function:cumtd-departure-fetcher"
}
```

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `CUMTD_API_KEY` | Yes | — | Your CUMTD developer API key |
| `CUMTD_API_BASE_URL` | No | `https://api.mtd.dev` | MTD Developer API base URL |
| `S3_BUCKET` | Yes | — | Target S3 bucket |
| `S3_PREFIX` | No | `raw-departures` | S3 key prefix |
| `STOP_IDS` | No | `IT` | Comma-separated stop IDs to always poll |
| `ROUTE_FILTERS` | No | — | Comma-separated route name keywords (e.g. `YELLOW,GREEN,GOLD,TEAL,SILVER,ILLINI`). Auto-discovers stops for matching routes. |
| `LOOKAHEAD_MINUTES` | No | `60` | Departure lookahead window |
| `MAX_RETRIES` | No | `3` | API retry attempts |
| `REQUEST_TIMEOUT_SECONDS` | No | `10` | HTTP timeout |
