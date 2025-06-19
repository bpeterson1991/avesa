#!/bin/bash

# AVESA Lambda Deployment Fix Script
# This script packages Lambda functions with shared modules and deploys them

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT="dev"
REGION="us-east-2"

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

# Function to show usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -e, --environment ENVIRONMENT    Environment to deploy (dev, staging, prod) [default: dev]"
    echo "  -r, --region REGION             AWS region [default: us-east-2]"
    echo "  -h, --help                      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --environment dev"
    echo "  $0 --environment prod --region us-west-2"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        -r|--region)
            REGION="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
    print_error "Invalid environment: $ENVIRONMENT. Must be dev, staging, or prod."
    exit 1
fi

print_header "AVESA Lambda Deployment Fix - $(echo $ENVIRONMENT | tr '[:lower:]' '[:upper:]')"
echo -e "Environment: ${ENVIRONMENT}"
echo -e "Region: ${REGION}"
echo ""

# Step 1: Package Lambda functions with shared modules
print_status "Step 1: Packaging Lambda functions with shared modules..."
python3 scripts/package-lambda-functions.py --function all --output-dir ./lambda-packages --clean
print_status "✓ Lambda functions packaged successfully"
echo ""

# Step 2: Deploy infrastructure using CDK
print_status "Step 2: Deploying infrastructure with CDK..."
cd infrastructure

# Check if CDK is bootstrapped
print_status "Checking CDK bootstrap status..."
if ! aws cloudformation describe-stacks --stack-name CDKToolkit --region "$REGION" &> /dev/null; then
    print_warning "CDK not bootstrapped in region $REGION. Bootstrapping now..."
    cdk bootstrap --region "$REGION"
    print_status "✓ CDK bootstrapped"
else
    print_status "✓ CDK already bootstrapped in region $REGION"
fi

# Deploy the stacks
print_status "Deploying CDK stacks..."
cdk deploy --context environment="$ENVIRONMENT" --all --require-approval never
print_status "✓ Infrastructure deployed"

cd ..
echo ""

# Step 3: Upload mapping files to S3
print_status "Step 3: Uploading mapping files to S3..."

# Determine bucket name based on environment
if [[ "$ENVIRONMENT" == "prod" ]]; then
    BUCKET_NAME="data-storage-msp"
else
    BUCKET_NAME="data-storage-msp-${ENVIRONMENT}"
fi

# Check if bucket exists
if ! aws s3 ls "s3://$BUCKET_NAME" --region "$REGION" > /dev/null 2>&1; then
    print_error "S3 bucket $BUCKET_NAME not found. Infrastructure deployment may have failed."
    exit 1
fi

# Upload canonical mappings
if [[ -d "mappings/canonical" ]]; then
    for mapping_file in mappings/canonical/*.json; do
        if [[ -f "$mapping_file" ]]; then
            filename=$(basename "$mapping_file")
            aws s3 cp "$mapping_file" "s3://$BUCKET_NAME/mappings/canonical/$filename" --region "$REGION"
            print_status "✓ Uploaded canonical mapping: $filename"
        fi
    done
else
    print_warning "mappings/canonical directory not found, skipping canonical mapping upload"
fi

# Upload other mapping files
if [[ -d "mappings" ]]; then
    for mapping_file in mappings/*.json; do
        if [[ -f "$mapping_file" ]]; then
            filename=$(basename "$mapping_file")
            aws s3 cp "$mapping_file" "s3://$BUCKET_NAME/mappings/$filename" --region "$REGION"
            print_status "✓ Uploaded mapping: $filename"
        fi
    done
fi

# Upload integration endpoint configurations
if [[ -d "mappings/integrations" ]]; then
    aws s3 sync mappings/integrations/ "s3://$BUCKET_NAME/mappings/integrations/" --region "$REGION"
    print_status "✓ Uploaded integration endpoint configurations"
else
    print_warning "mappings/integrations directory not found, skipping integration upload"
fi

print_status "✓ Mapping files uploaded"
echo ""

# Step 4: Test Lambda functions
print_status "Step 4: Testing Lambda functions..."

# Test ConnectWise Lambda
CONNECTWISE_FUNCTION="avesa-connectwise-ingestion-${ENVIRONMENT}"
print_status "Testing ConnectWise Lambda function: $CONNECTWISE_FUNCTION"

if aws lambda get-function --function-name "$CONNECTWISE_FUNCTION" --region "$REGION" > /dev/null 2>&1; then
    print_status "✓ ConnectWise Lambda function exists"
    
    # Test with a simple payload
    echo '{"test": true}' > /tmp/test-payload.json
    if aws lambda invoke --function-name "$CONNECTWISE_FUNCTION" --payload file:///tmp/test-payload.json /tmp/response.json --region "$REGION" > /dev/null 2>&1; then
        print_status "✓ ConnectWise Lambda function invocation successful"
    else
        print_warning "ConnectWise Lambda function invocation failed (this may be expected without proper tenant configuration)"
    fi
    rm -f /tmp/test-payload.json /tmp/response.json
else
    print_error "ConnectWise Lambda function not found: $CONNECTWISE_FUNCTION"
fi

# Test Canonical Transform Lambda
CANONICAL_FUNCTION="avesa-canonical-transform-tickets-${ENVIRONMENT}"
print_status "Testing Canonical Transform Lambda function: $CANONICAL_FUNCTION"

if aws lambda get-function --function-name "$CANONICAL_FUNCTION" --region "$REGION" > /dev/null 2>&1; then
    print_status "✓ Canonical Transform Lambda function exists"
    
    # Test with a simple payload
    echo '{"test": true}' > /tmp/test-payload.json
    if aws lambda invoke --function-name "$CANONICAL_FUNCTION" --payload file:///tmp/test-payload.json /tmp/response.json --region "$REGION" > /dev/null 2>&1; then
        print_status "✓ Canonical Transform Lambda function invocation successful"
    else
        print_warning "Canonical Transform Lambda function invocation failed (this may be expected without proper data)"
    fi
    rm -f /tmp/test-payload.json /tmp/response.json
else
    print_error "Canonical Transform Lambda function not found: $CANONICAL_FUNCTION"
fi

echo ""

# Step 5: Clean up
print_status "Step 5: Cleaning up..."
rm -rf ./lambda-packages
print_status "✓ Cleaned up temporary files"

echo ""
print_header "Deployment Complete!"
echo ""
print_status "Lambda functions have been successfully deployed with shared modules!"
echo ""
print_status "Next steps:"
echo ""
echo -e "${YELLOW}1. Create a tenant using the tenant setup script:${NC}"
echo "   python scripts/setup-tenant-only.py --tenant-id 'example-tenant' --company-name 'Example Company' --environment $ENVIRONMENT"
echo ""
echo -e "${YELLOW}2. Add services to the tenant (e.g., ConnectWise):${NC}"
echo "   python scripts/setup-service.py --tenant-id 'example-tenant' --service connectwise \\"
echo "     --connectwise-url 'https://api-na.myconnectwise.net' \\"
echo "     --company-id 'YourCompanyID' --public-key 'your-key' \\"
echo "     --private-key 'your-private-key' --client-id 'your-client-id' \\"
echo "     --environment $ENVIRONMENT"
echo ""
echo -e "${YELLOW}3. Test the pipeline with the configured tenant:${NC}"
echo "   python scripts/test-lambda-functions.py --environment $ENVIRONMENT --region $REGION"
echo ""
echo -e "${YELLOW}4. Monitor Lambda logs:${NC}"
echo "   aws logs tail /aws/lambda/avesa-connectwise-ingestion-${ENVIRONMENT} --follow --region $REGION"
echo ""