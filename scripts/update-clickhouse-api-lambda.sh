#!/bin/bash

# Update ClickHouse API Lambda deployment
# This script helps deploy the fixed Lambda function with mapping files included

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}📦 ClickHouse API Lambda Update Script${NC}"
echo "========================================="

# Get environment from argument or default to dev
ENVIRONMENT=${1:-dev}
echo -e "${YELLOW}Environment: ${ENVIRONMENT}${NC}"

# Check if AWS credentials are configured
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo -e "${RED}❌ AWS credentials not configured${NC}"
    echo "Please run 'aws configure' or set AWS credentials"
    exit 1
fi

# Navigate to infrastructure directory
cd "$(dirname "$0")/../infrastructure" || exit

echo -e "\n${YELLOW}📋 Pre-deployment checklist:${NC}"
echo "1. Mapping files exist at /mappings/canonical/"
echo "2. CDK stack updated to include mappings in bundling"
echo "3. schema-sync.js updated to handle Lambda paths"

# Check if mapping files exist
if [ ! -d "../mappings/canonical" ]; then
    echo -e "${RED}❌ Mapping files directory not found at /mappings/canonical${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Mapping files found${NC}"
ls -la ../mappings/canonical/*.json

# Deploy the web application stack
echo -e "\n${YELLOW}🚀 Deploying WebApplicationStack with CDK...${NC}"
cdk deploy AVESAWebApplication-${ENVIRONMENT} \
    --context environment=${ENVIRONMENT} \
    --require-approval never

# Get the Lambda function name from CloudFormation outputs
LAMBDA_FUNCTION_NAME=$(aws cloudformation describe-stacks \
    --stack-name AVESAWebApplication-${ENVIRONMENT} \
    --query "Stacks[0].Outputs[?ExportName=='ClickHouseAPI-Lambda-${ENVIRONMENT}'].OutputValue" \
    --output text)

if [ -z "$LAMBDA_FUNCTION_NAME" ]; then
    echo -e "${RED}❌ Could not find Lambda function name${NC}"
    exit 1
fi

echo -e "\n${GREEN}✅ Deployment successful!${NC}"
echo -e "Lambda function: ${LAMBDA_FUNCTION_NAME}"

# Test the Lambda deployment
echo -e "\n${YELLOW}🧪 Testing Lambda deployment...${NC}"

# Check Lambda environment
aws lambda get-function --function-name "$LAMBDA_FUNCTION_NAME" \
    --query 'Configuration.Environment.Variables' > /dev/null 2>&1

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Lambda function is accessible${NC}"
else
    echo -e "${RED}❌ Could not access Lambda function${NC}"
    exit 1
fi

# Get API Gateway URL
API_URL=$(aws cloudformation describe-stacks \
    --stack-name AVESAWebApplication-${ENVIRONMENT} \
    --query "Stacks[0].Outputs[?ExportName=='ClickHouseAPI-URL-${ENVIRONMENT}'].OutputValue" \
    --output text)

echo -e "\n${GREEN}📌 Deployment Summary:${NC}"
echo "========================"
echo "Environment: ${ENVIRONMENT}"
echo "Lambda Function: ${LAMBDA_FUNCTION_NAME}"
echo "API Gateway URL: ${API_URL}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Test the /health endpoint: curl ${API_URL}health"
echo "2. Test companies endpoint: curl ${API_URL}api/data/companies"
echo "3. Monitor CloudWatch logs for any errors"
echo ""
echo -e "${GREEN}✅ Update complete!${NC}"