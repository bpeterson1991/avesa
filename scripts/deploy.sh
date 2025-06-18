#!/bin/bash

# ConnectWise Data Pipeline Deployment Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT="dev"
REGION="us-east-1"
PROFILE=""

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

# Function to show usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -e, --environment ENVIRONMENT    Environment to deploy (dev, staging, prod) [default: dev]"
    echo "  -r, --region REGION             AWS region [default: us-east-1]"
    echo "  -p, --profile PROFILE           AWS profile to use"
    echo "  -h, --help                      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --environment prod --region us-west-2"
    echo "  $0 -e staging -p my-aws-profile"
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
        -p|--profile)
            PROFILE="$2"
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

print_status "Starting deployment for environment: $ENVIRONMENT"
print_status "Region: $REGION"

# Set AWS profile if provided
if [[ -n "$PROFILE" ]]; then
    export AWS_PROFILE="$PROFILE"
    print_status "Using AWS profile: $PROFILE"
fi

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    print_error "AWS CLI is not installed. Please install it first."
    exit 1
fi

# Check if CDK is installed
if ! command -v cdk &> /dev/null; then
    print_error "AWS CDK is not installed. Please install it first: npm install -g aws-cdk"
    exit 1
fi

# Check if we're in the right directory
if [[ ! -f "infrastructure/app.py" ]]; then
    print_error "Please run this script from the project root directory."
    exit 1
fi

# Install Python dependencies
print_status "Installing Python dependencies..."
pip install -r requirements.txt

# Change to infrastructure directory
cd infrastructure

# Bootstrap CDK if needed
print_status "Checking CDK bootstrap status..."
if ! aws cloudformation describe-stacks --stack-name CDKToolkit --region "$REGION" &> /dev/null; then
    print_warning "CDK not bootstrapped in region $REGION. Bootstrapping now..."
    cdk bootstrap --region "$REGION"
else
    print_status "CDK already bootstrapped in region $REGION"
fi

# Set environment variables
export CDK_DEFAULT_REGION="$REGION"

# Synthesize the CDK app
print_status "Synthesizing CDK application..."
cdk synth --context environment="$ENVIRONMENT" --all

# Deploy the stacks
print_status "Deploying CDK stacks..."
cdk deploy --context environment="$ENVIRONMENT" --all --require-approval never

# Upload mapping files to S3
print_status "Uploading mapping files to S3..."
cd ..

# Get the bucket name from CDK output or use default
BUCKET_NAME="data-storage-msp"
if [[ "$ENVIRONMENT" != "prod" ]]; then
    BUCKET_NAME="data-storage-msp-$ENVIRONMENT"
fi

# Upload canonical mappings
aws s3 cp mappings/canonical_mappings.json "s3://$BUCKET_NAME/mappings/canonical_mappings.json" --region "$REGION"

print_status "Deployment completed successfully!"
print_status ""
print_status "Next steps:"
print_status "1. Configure tenant settings in DynamoDB table: TenantServices-$ENVIRONMENT"
print_status "2. Create ConnectWise API credentials in AWS Secrets Manager"
print_status "3. Test the pipeline with a sample tenant"
print_status ""
print_status "Useful commands:"
print_status "  aws dynamodb scan --table-name TenantServices-$ENVIRONMENT --region $REGION"
print_status "  aws logs tail /aws/lambda/connectwise-raw-ingestion-$ENVIRONMENT --follow --region $REGION"
print_status "  aws s3 ls s3://$BUCKET_NAME/ --recursive --region $REGION"