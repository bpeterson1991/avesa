#!/bin/bash

# AVESA Multi-Tenant Data Pipeline Unified Deployment Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT=""
REGION="us-east-2"
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
    echo "  -e, --environment ENVIRONMENT    Environment to deploy (dev, staging, prod) [REQUIRED]"
    echo "  -r, --region REGION             AWS region [default: us-east-2]"
    echo "  -p, --profile PROFILE           AWS profile to use [auto-detected based on environment]"
    echo "  -h, --help                      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --environment dev"
    echo "  $0 --environment prod --region us-west-2"
    echo "  $0 -e staging -p my-aws-profile"
    echo ""
    echo "Environment-specific defaults:"
    echo "  dev/staging: Uses AdministratorAccess-123938354448 profile or specified profile"
    echo "  prod:        Uses 'avesa-production' profile (can be overridden)"
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

# Validate required parameters
if [[ -z "$ENVIRONMENT" ]]; then
    print_error "Environment is required. Use -e or --environment to specify."
    usage
    exit 1
fi

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
    print_error "Invalid environment: $ENVIRONMENT. Must be dev, staging, or prod."
    exit 1
fi

# Set environment-specific configuration
case "$ENVIRONMENT" in
    "prod")
        AWS_PROFILE="${PROFILE:-avesa-production}"
        ACCOUNT_VAR="CDK_PROD_ACCOUNT"
        RESOURCE_SUFFIX=""
        ;;
    *)
        AWS_PROFILE="${PROFILE:-AdministratorAccess-123938354448}"
        ACCOUNT_VAR="CDK_DEFAULT_ACCOUNT"
        RESOURCE_SUFFIX="-${ENVIRONMENT}"
        ;;
esac

# Display deployment information
print_header "AVESA Deployment - $(echo $ENVIRONMENT | tr '[:lower:]' '[:upper:]')"
echo -e "Environment: ${ENVIRONMENT}"
echo -e "AWS Profile: ${AWS_PROFILE}"
echo -e "Region: ${REGION}"
echo -e "Resource Suffix: ${RESOURCE_SUFFIX:-none}"
echo ""

# Function to validate prerequisites
validate_prerequisites() {
    print_status "Validating prerequisites..."
    
    # Check if AWS CLI is installed
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    print_status "✓ AWS CLI installed"
    
    # Check if CDK is installed
    if ! command -v cdk &> /dev/null; then
        print_error "AWS CDK is not installed. Please install it first: npm install -g aws-cdk"
        exit 1
    fi
    print_status "✓ AWS CDK installed"
    
    # Check if we're in the right directory
    if [[ ! -f "infrastructure/app.py" ]]; then
        print_error "Please run this script from the project root directory."
        exit 1
    fi
    print_status "✓ Running from project root"
    
    # Check Python dependencies
    if [[ ! -f "requirements.txt" ]]; then
        print_warning "requirements.txt not found, skipping Python dependency check"
    else
        print_status "✓ Python requirements file found"
    fi
    
    print_status "✓ Prerequisites validated"
}

# Function to set environment variables and configure AWS profile
set_environment() {
    print_status "Configuring environment..."
    
    # Set AWS profile
    export AWS_PROFILE="$AWS_PROFILE"
    print_status "✓ AWS_PROFILE set to: $AWS_PROFILE"
    
    # Verify AWS profile access
    if ! aws sts get-caller-identity --profile "$AWS_PROFILE" > /dev/null 2>&1; then
        print_error "AWS profile '$AWS_PROFILE' not configured or not accessible"
        if [[ "$ENVIRONMENT" == "prod" ]]; then
            print_warning "For production deployment, ensure the profile is configured:"
            echo "  aws configure --profile $AWS_PROFILE"
            echo ""
            echo "Or set up cross-account role access:"
            echo "  aws configure set role_arn arn:aws:iam::PROD_ACCOUNT_ID:role/DeploymentRole --profile $AWS_PROFILE"
            echo "  aws configure set source_profile AdministratorAccess-123938354448 --profile $AWS_PROFILE"
        fi
        exit 1
    fi
    
    # Get and set account ID
    ACCOUNT_ID=$(aws sts get-caller-identity --profile "$AWS_PROFILE" --query Account --output text)
    export $ACCOUNT_VAR="$ACCOUNT_ID"
    print_status "✓ $ACCOUNT_VAR set to: $ACCOUNT_ID"
    
    # Set default region
    export CDK_DEFAULT_REGION="$REGION"
    print_status "✓ CDK_DEFAULT_REGION set to: $REGION"
    
    # Install Python dependencies if requirements.txt exists
    if [[ -f "requirements.txt" ]]; then
        print_status "Installing Python dependencies..."
        pip install -r requirements.txt
        print_status "✓ Python dependencies installed"
    fi
}

# Function to bootstrap CDK if needed
bootstrap_cdk() {
    print_status "Checking CDK bootstrap status..."
    
    if ! aws cloudformation describe-stacks --stack-name CDKToolkit --region "$REGION" --profile "$AWS_PROFILE" &> /dev/null; then
        print_warning "CDK not bootstrapped in region $REGION. Bootstrapping now..."
        cd infrastructure
        cdk bootstrap --region "$REGION"
        cd ..
        print_status "✓ CDK bootstrapped"
    else
        print_status "✓ CDK already bootstrapped in region $REGION"
    fi
}

# Function to deploy infrastructure
deploy_infrastructure() {
    print_status "Deploying infrastructure..."
    
    cd infrastructure
    
    # Synthesize the CDK app
    print_status "Synthesizing CDK application..."
    cdk synth --context environment="$ENVIRONMENT" --all
    print_status "✓ CDK synthesis successful"
    
    # Deploy the stacks
    print_status "Deploying CDK stacks..."
    cdk deploy --context environment="$ENVIRONMENT" --all --require-approval never
    print_status "✓ Infrastructure deployed"
    
    cd ..
}

# Function to upload mapping files to S3
upload_mappings() {
    print_status "Uploading mapping files to S3..."
    
    # Determine bucket name based on environment
    BUCKET_NAME="data-storage-msp${RESOURCE_SUFFIX}"
    
    # Check if bucket exists
    if ! aws s3 ls "s3://$BUCKET_NAME" --region "$REGION" --profile "$AWS_PROFILE" > /dev/null 2>&1; then
        print_error "S3 bucket $BUCKET_NAME not found. Infrastructure deployment may have failed."
        exit 1
    fi
    
    # Upload canonical mappings from the new canonical folder
    if [[ -d "mappings/canonical" ]]; then
        for mapping_file in mappings/canonical/*.json; do
            if [[ -f "$mapping_file" ]]; then
                filename=$(basename "$mapping_file")
                aws s3 cp "$mapping_file" "s3://$BUCKET_NAME/mappings/canonical/$filename" --region "$REGION" --profile "$AWS_PROFILE"
                print_status "✓ Uploaded canonical mapping: $filename"
            fi
        done
    else
        print_warning "mappings/canonical directory not found, skipping canonical mapping upload"
    fi
    
    # Upload other mapping files (backfill_config.json, etc.)
    if [[ -d "mappings" ]]; then
        for mapping_file in mappings/*.json; do
            if [[ -f "$mapping_file" ]]; then
                filename=$(basename "$mapping_file")
                aws s3 cp "$mapping_file" "s3://$BUCKET_NAME/mappings/$filename" --region "$REGION" --profile "$AWS_PROFILE"
                print_status "✓ Uploaded mapping: $filename"
            fi
        done
    fi
    
    # Upload integration endpoint configurations
    if [[ -d "mappings/integrations" ]]; then
        aws s3 sync mappings/integrations/ "s3://$BUCKET_NAME/mappings/integrations/" --region "$REGION" --profile "$AWS_PROFILE"
        print_status "✓ Uploaded integration endpoint configurations"
    else
        print_warning "mappings/integrations directory not found, skipping integration upload"
    fi
    
    print_status "✓ Mapping files uploaded"
}

# Function to validate deployment
validate_deployment() {
    print_status "Validating deployment..."
    
    # Check CloudFormation stacks
    print_status "Checking CloudFormation stacks..."
    STACKS=$(aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE --profile "$AWS_PROFILE" --region "$REGION" --query 'StackSummaries[?contains(StackName, `AVESA`)].StackName' --output text)
    
    if [[ -z "$STACKS" ]]; then
        print_error "❌ No AVESA stacks found"
        exit 1
    fi
    print_status "✓ Found CloudFormation stacks: $STACKS"
    
    # Check Lambda functions
    print_status "Checking Lambda functions..."
    if [[ "$ENVIRONMENT" == "prod" ]]; then
        FUNCTIONS=$(aws lambda list-functions --region "$REGION" --profile "$AWS_PROFILE" --query 'Functions[?contains(FunctionName, `avesa`) && contains(FunctionName, `prod`)].FunctionName' --output text)
    else
        FUNCTIONS=$(aws lambda list-functions --region "$REGION" --profile "$AWS_PROFILE" --query 'Functions[?contains(FunctionName, `avesa`) && contains(FunctionName, `'$ENVIRONMENT'`)].FunctionName' --output text)
    fi
    
    if [[ -z "$FUNCTIONS" ]]; then
        print_error "❌ No AVESA Lambda functions found for environment $ENVIRONMENT"
        exit 1
    fi
    print_status "✓ Found Lambda functions: $FUNCTIONS"
    
    # Check DynamoDB tables
    print_status "Checking DynamoDB tables..."
    TABLE_NAME="TenantServices${RESOURCE_SUFFIX}"
    
    if aws dynamodb describe-table --table-name "$TABLE_NAME" --region "$REGION" --profile "$AWS_PROFILE" > /dev/null 2>&1; then
        print_status "✓ $TABLE_NAME table exists"
    else
        print_error "❌ $TABLE_NAME table not found"
        exit 1
    fi
    
    # Check LastUpdated table
    LAST_UPDATED_TABLE="LastUpdated${RESOURCE_SUFFIX}"
    if aws dynamodb describe-table --table-name "$LAST_UPDATED_TABLE" --region "$REGION" --profile "$AWS_PROFILE" > /dev/null 2>&1; then
        print_status "✓ $LAST_UPDATED_TABLE table exists"
    else
        print_warning "LastUpdated table not found (may not be created yet)"
    fi
    
    # Check S3 bucket
    print_status "Checking S3 bucket..."
    BUCKET_NAME="data-storage-msp${RESOURCE_SUFFIX}"
    if aws s3 ls "s3://$BUCKET_NAME" --region "$REGION" --profile "$AWS_PROFILE" > /dev/null 2>&1; then
        print_status "✓ S3 bucket $BUCKET_NAME exists"
    else
        print_error "❌ S3 bucket $BUCKET_NAME not found"
        exit 1
    fi
    
    print_status "✓ Deployment validation successful"
}

# Function to show next steps
show_next_steps() {
    echo ""
    print_header "Deployment Complete!"
    echo ""
    print_status "Next steps:"
    echo ""
    
    if [[ "$ENVIRONMENT" == "prod" ]]; then
        echo -e "${YELLOW}1. Migrate production data:${NC}"
        echo "   python3 scripts/migrate-production-data.py --dry-run"
        echo "   python3 scripts/migrate-production-data.py --execute"
        echo ""
        echo -e "${YELLOW}2. Test Lambda functions:${NC}"
        echo "   aws lambda invoke --function-name avesa-connectwise-ingestion-prod --payload '{}' response.json --profile $AWS_PROFILE --region $REGION"
        echo ""
        echo -e "${YELLOW}3. Monitor logs:${NC}"
        echo "   aws logs tail /aws/lambda/avesa-connectwise-ingestion-prod --follow --profile $AWS_PROFILE --region $REGION"
        echo ""
        echo -e "${YELLOW}4. Update application configuration to use production account${NC}"
        echo ""
        echo -e "${YELLOW}5. Set up monitoring and alerting${NC}"
    else
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
        echo "   aws lambda invoke --function-name avesa-connectwise-ingestion$RESOURCE_SUFFIX \\"
        echo "     --payload '{\"tenant_id\": \"example-tenant\"}' response.json --region $REGION"
        if [[ -n "$AWS_PROFILE" && "$AWS_PROFILE" != "default" ]]; then
            echo "     --profile $AWS_PROFILE"
        fi
    fi
    
    echo ""
    echo -e "${YELLOW}Useful commands:${NC}"
    echo ""
    echo -e "${YELLOW}  # View tenant configurations${NC}"
    echo "  aws dynamodb scan --table-name TenantServices$RESOURCE_SUFFIX --region $REGION"
    if [[ -n "$AWS_PROFILE" && "$AWS_PROFILE" != "default" ]]; then
        echo "    --profile $AWS_PROFILE"
    fi
    echo ""
    echo -e "${YELLOW}  # View tenant secrets${NC}"
    echo "  aws secretsmanager list-secrets --filters Key=name,Values=tenant/ --region $REGION"
    if [[ -n "$AWS_PROFILE" && "$AWS_PROFILE" != "default" ]]; then
        echo "    --profile $AWS_PROFILE"
    fi
    echo ""
    echo -e "${YELLOW}  # Monitor Lambda logs${NC}"
    echo "  aws logs tail /aws/lambda/avesa-connectwise-ingestion$RESOURCE_SUFFIX --follow --region $REGION"
    if [[ -n "$AWS_PROFILE" && "$AWS_PROFILE" != "default" ]]; then
        echo "    --profile $AWS_PROFILE"
    fi
    echo ""
    echo -e "${YELLOW}  # Check S3 data${NC}"
    echo "  aws s3 ls s3://data-storage-msp$RESOURCE_SUFFIX/ --recursive --region $REGION"
    if [[ -n "$AWS_PROFILE" && "$AWS_PROFILE" != "default" ]]; then
        echo "    --profile $AWS_PROFILE"
    fi
    echo ""
}

# Main execution function
main() {
    validate_prerequisites
    set_environment
    bootstrap_cdk
    deploy_infrastructure
    upload_mappings
    validate_deployment
    show_next_steps
}

# Run main function
main