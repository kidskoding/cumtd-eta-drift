#!/usr/bin/env bash
# ============================================================================
# setup_aws.sh — Provision all AWS infra for the CUMTD departure fetcher
#
# Prerequisites:
#   - AWS CLI v2 installed and configured (aws configure)
#   - A CUMTD API key from https://api.mtd.dev
#
# Usage:
#   chmod +x setup_aws.sh
#   ./setup_aws.sh
# ============================================================================
set -euo pipefail

# ----- CONFIGURE THESE -----
AWS_REGION="${AWS_REGION:-us-east-1}"
S3_BUCKET="cumtd-eta-drift"
FUNCTION_NAME="cumtd-departure-fetcher"
ROLE_NAME="cumtd-lambda-role"
RULE_NAME="cumtd-fetch-schedule"
SCHEDULE_RATE="rate(2 minutes)"           # adjust as needed
CUMTD_API_KEY="${CUMTD_API_KEY:?Set CUMTD_API_KEY env var before running}"
STOP_IDS="${STOP_IDS:-IT}"
# ----------------------------

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> Account: $ACCOUNT_ID | Region: $AWS_REGION"

# ---- 1. S3 Bucket ----
echo "==> Creating S3 bucket: $S3_BUCKET"
if aws s3api head-bucket --bucket "$S3_BUCKET" 2>/dev/null; then
    echo "    Bucket already exists, skipping."
else
    if [ "$AWS_REGION" = "us-east-1" ]; then
        aws s3api create-bucket --bucket "$S3_BUCKET" --region "$AWS_REGION"
    else
        aws s3api create-bucket --bucket "$S3_BUCKET" --region "$AWS_REGION" \
            --create-bucket-configuration LocationConstraint="$AWS_REGION"
    fi
    echo "    Created."
fi

# ---- 2. IAM Role ----
echo "==> Creating IAM role: $ROLE_NAME"
TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}'

ROLE_ARN=$(aws iam create-role \
    --role-name "$ROLE_NAME" \
    --assume-role-policy-document "$TRUST_POLICY" \
    --query 'Role.Arn' --output text 2>/dev/null) || \
ROLE_ARN=$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text)
echo "    Role ARN: $ROLE_ARN"

# Attach basic Lambda execution (CloudWatch Logs)
aws iam attach-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole 2>/dev/null || true

# Inline policy for S3 write
S3_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:PutObject"],
    "Resource": "arn:aws:s3:::${S3_BUCKET}/raw-departures/*"
  }]
}
EOF
)
aws iam put-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-name "${FUNCTION_NAME}-s3-write" \
    --policy-document "$S3_POLICY"
echo "    Policies attached."

# IAM role propagation can take a few seconds
echo "    Waiting 10s for IAM propagation..."
sleep 10

# ---- 3. Lambda Function ----
echo "==> Packaging and creating Lambda function: $FUNCTION_NAME"
cd "$SCRIPT_DIR"
zip -j /tmp/lambda_function.zip lambda_function.py

aws lambda create-function \
    --function-name "$FUNCTION_NAME" \
    --runtime python3.12 \
    --architecture arm64 \
    --handler lambda_function.lambda_handler \
    --role "$ROLE_ARN" \
    --zip-file fileb:///tmp/lambda_function.zip \
    --timeout 30 \
    --memory-size 128 \
    --environment "Variables={
        CUMTD_API_KEY=$CUMTD_API_KEY,
        S3_BUCKET=$S3_BUCKET,
        S3_PREFIX=raw-departures,
        STOP_IDS=$STOP_IDS,
        LOOKAHEAD_MINUTES=60
    }" \
    --region "$AWS_REGION" 2>/dev/null && echo "    Created." || {
        echo "    Function exists — updating code and config..."
        aws lambda update-function-code \
            --function-name "$FUNCTION_NAME" \
            --zip-file fileb:///tmp/lambda_function.zip \
            --region "$AWS_REGION" > /dev/null
        sleep 5
        aws lambda update-function-configuration \
            --function-name "$FUNCTION_NAME" \
            --environment "Variables={
                CUMTD_API_KEY=$CUMTD_API_KEY,
                S3_BUCKET=$S3_BUCKET,
                S3_PREFIX=raw-departures,
                STOP_IDS=$STOP_IDS,
                LOOKAHEAD_MINUTES=60
            }" \
            --region "$AWS_REGION" > /dev/null
        echo "    Updated."
    }

rm /tmp/lambda_function.zip

# ---- 4. EventBridge Schedule ----
echo "==> Creating EventBridge rule: $RULE_NAME ($SCHEDULE_RATE)"
RULE_ARN=$(aws events put-rule \
    --name "$RULE_NAME" \
    --schedule-expression "$SCHEDULE_RATE" \
    --region "$AWS_REGION" \
    --query 'RuleArn' --output text)
echo "    Rule ARN: $RULE_ARN"

LAMBDA_ARN="arn:aws:lambda:${AWS_REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"

aws lambda add-permission \
    --function-name "$FUNCTION_NAME" \
    --statement-id eventbridge-invoke \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "$RULE_ARN" \
    --region "$AWS_REGION" 2>/dev/null || true

aws events put-targets \
    --rule "$RULE_NAME" \
    --targets "Id=1,Arn=$LAMBDA_ARN" \
    --region "$AWS_REGION" > /dev/null

echo ""
echo "============================================"
echo " Setup complete!"
echo "============================================"
echo " S3 bucket:  s3://$S3_BUCKET/raw-departures/"
echo " Lambda:     $FUNCTION_NAME"
echo " Schedule:   $SCHEDULE_RATE"
echo " Region:     $AWS_REGION"
echo ""
echo " Test manually:"
echo "   aws lambda invoke --function-name $FUNCTION_NAME /tmp/out.json && cat /tmp/out.json"
echo ""
echo " Next: configure Databricks to read from s3://$S3_BUCKET/raw-departures/"
echo "============================================"
