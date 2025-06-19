#!/bin/bash
# Production Setup Validation Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}AVESA Production Setup Validation${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Configuration
PROD_ACCOUNT_ID="563583517998"
PROD_PROFILE="avesa-production"
REGION="us-east-2"

# Test 1: AWS CLI Profile
echo -e "${YELLOW}1. Testing AWS CLI Profile...${NC}"
if aws sts get-caller-identity --profile ${PROD_PROFILE} > /dev/null 2>&1; then
    CALLER_IDENTITY=$(aws sts get-caller-identity --profile ${PROD_PROFILE})
    ACCOUNT=$(echo "$CALLER_IDENTITY" | jq -r '.Account')
    USER_ARN=$(echo "$CALLER_IDENTITY" | jq -r '.Arn')
    
    if [ "$ACCOUNT" = "$PROD_ACCOUNT_ID" ]; then
        echo -e "${GREEN}✓ AWS CLI profile working correctly${NC}"
        echo -e "  Account: $ACCOUNT"
        echo -e "  User: $USER_ARN"
    else
        echo -e "${RED}❌ Account ID mismatch${NC}"
        echo -e "  Expected: $PROD_ACCOUNT_ID"
        echo -e "  Actual: $ACCOUNT"
        exit 1
    fi
else
    echo -e "${RED}❌ AWS CLI profile test failed${NC}"
    exit 1
fi

# Test 2: Environment Variables
echo -e "${YELLOW}2. Testing Environment Variables...${NC}"
export CDK_PROD_ACCOUNT="$PROD_ACCOUNT_ID"
export CDK_DEFAULT_REGION="$REGION"

if [ "$CDK_PROD_ACCOUNT" = "$PROD_ACCOUNT_ID" ] && [ "$CDK_DEFAULT_REGION" = "$REGION" ]; then
    echo -e "${GREEN}✓ Environment variables configured correctly${NC}"
    echo -e "  CDK_PROD_ACCOUNT: $CDK_PROD_ACCOUNT"
    echo -e "  CDK_DEFAULT_REGION: $CDK_DEFAULT_REGION"
else
    echo -e "${RED}❌ Environment variables not set correctly${NC}"
    exit 1
fi

# Test 3: CDK Installation
echo -e "${YELLOW}3. Testing CDK Installation...${NC}"
if command -v cdk >/dev/null 2>&1; then
    CDK_VERSION=$(cdk --version)
    echo -e "${GREEN}✓ CDK installed: $CDK_VERSION${NC}"
else
    echo -e "${RED}❌ CDK not installed${NC}"
    exit 1
fi

# Test 4: CDK Synthesis
echo -e "${YELLOW}4. Testing CDK Synthesis...${NC}"
cd infrastructure
if AWS_PROFILE=${PROD_PROFILE} cdk synth --context environment=prod > /dev/null 2>&1; then
    echo -e "${GREEN}✓ CDK synthesis successful for production${NC}"
else
    echo -e "${RED}❌ CDK synthesis failed${NC}"
    exit 1
fi
cd ..

# Test 5: AWS Permissions
echo -e "${YELLOW}5. Testing AWS Permissions...${NC}"

# Test S3 permissions
if aws s3 ls --profile ${PROD_PROFILE} --region ${REGION} > /dev/null 2>&1; then
    echo -e "${GREEN}✓ S3 permissions working${NC}"
else
    echo -e "${RED}❌ S3 permissions failed${NC}"
    exit 1
fi

# Test CloudFormation permissions
if aws cloudformation list-stacks --profile ${PROD_PROFILE} --region ${REGION} > /dev/null 2>&1; then
    echo -e "${GREEN}✓ CloudFormation permissions working${NC}"
else
    echo -e "${RED}❌ CloudFormation permissions failed${NC}"
    exit 1
fi

# Test Lambda permissions
if aws lambda list-functions --profile ${PROD_PROFILE} --region ${REGION} > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Lambda permissions working${NC}"
else
    echo -e "${RED}❌ Lambda permissions failed${NC}"
    exit 1
fi

# Test DynamoDB permissions
if aws dynamodb list-tables --profile ${PROD_PROFILE} --region ${REGION} > /dev/null 2>&1; then
    echo -e "${GREEN}✓ DynamoDB permissions working${NC}"
else
    echo -e "${RED}❌ DynamoDB permissions failed${NC}"
    exit 1
fi

# Test 6: CDK Bootstrap Status
echo -e "${YELLOW}6. Testing CDK Bootstrap Status...${NC}"
if aws cloudformation describe-stacks --stack-name CDKToolkit --profile ${PROD_PROFILE} --region ${REGION} > /dev/null 2>&1; then
    echo -e "${GREEN}✓ CDK already bootstrapped${NC}"
else
    echo -e "${YELLOW}⚠ CDK not yet bootstrapped (this is expected for initial setup)${NC}"
    echo -e "  Run: cdk bootstrap --profile ${PROD_PROFILE} --context environment=prod"
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Validation Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${GREEN}Production account setup is ready for Phase 3 deployment!${NC}"
echo ""
echo -e "${YELLOW}Next steps for Phase 3:${NC}"
echo -e "1. Bootstrap CDK (if not already done):"
echo -e "   cd infrastructure && cdk bootstrap --profile ${PROD_PROFILE} --context environment=prod"
echo ""
echo -e "2. Deploy infrastructure:"
echo -e "   ./scripts/deploy-prod.sh"
echo ""
echo -e "3. Migrate data:"
echo -e "   python3 scripts/migrate-production-data.py --dry-run"
echo -e "   python3 scripts/migrate-production-data.py --execute"
echo ""