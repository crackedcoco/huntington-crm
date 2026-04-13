#!/bin/bash
# Deploy Huntington CRM stack to AWS
set -euo pipefail

STAGE="${1:-dev}"
STACK_NAME="huntington-crm-${STAGE}"
REGION="us-east-1"
TEMPLATE="$(dirname "$0")/../infrastructure/crm-stack.yaml"
BACKEND_DIR="$(dirname "$0")/../backend"

echo "=== Deploying ${STACK_NAME} ==="
echo "Region: ${REGION}"
echo "Template: ${TEMPLATE}"
echo

# Step 1: Deploy CloudFormation stack
echo "Step 1: Deploying CloudFormation stack..."
aws cloudformation deploy \
  --template-file "${TEMPLATE}" \
  --stack-name "${STACK_NAME}" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides Stage="${STAGE}" \
  --region "${REGION}" \
  --no-fail-on-empty-changeset

# Step 2: Get outputs
echo
echo "Step 2: Getting stack outputs..."
API_URL=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --region "${REGION}" \
  --query "Stacks[0].Outputs[?OutputKey=='ApiUrl'].OutputValue" \
  --output text)

POOL_ID=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --region "${REGION}" \
  --query "Stacks[0].Outputs[?OutputKey=='UserPoolId'].OutputValue" \
  --output text)

CLIENT_ID=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --region "${REGION}" \
  --query "Stacks[0].Outputs[?OutputKey=='UserPoolClientId'].OutputValue" \
  --output text)

echo "API URL:    ${API_URL}"
echo "Pool ID:    ${POOL_ID}"
echo "Client ID:  ${CLIENT_ID}"

# Step 3: Package and deploy Lambda code
echo
echo "Step 3: Packaging Lambda code..."
TEMP_DIR=$(mktemp -d)
cp -R "${BACKEND_DIR}/crm" "${TEMP_DIR}/"
cp -R "${BACKEND_DIR}/shared" "${TEMP_DIR}/"
cd "${TEMP_DIR}"
zip -r lambda.zip crm/ shared/ -x '*__pycache__*'
cd -

echo "Deploying Lambda code..."
aws lambda update-function-code \
  --function-name "huntington-crm-api-${STAGE}" \
  --zip-file "fileb://${TEMP_DIR}/lambda.zip" \
  --region "${REGION}" \
  --output text > /dev/null

rm -rf "${TEMP_DIR}"

echo
echo "=== Deployment complete ==="
echo "API:      ${API_URL}"
echo "Pool:     ${POOL_ID}"
echo "Client:   ${CLIENT_ID}"
echo "Cognito:  https://huntington-crm-${STAGE}.auth.${REGION}.amazoncognito.com"
