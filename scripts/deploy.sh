#!/bin/bash

# AVESA Deployment Script
# This script deploys the data pipeline infrastructure

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
PROFILE=""
SKIP_TESTS=false
DRY_RUN=false

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Deploy the AVESA infrastructure.

OPTIONS:
    -e, --environment ENV    Environment to deploy to (dev, staging, prod) [default: dev]
    -r, --region REGION      AWS region [default: us-east-2]
    -p, --profile PROFILE    AWS profile to use
    -s, --skip-tests         Skip pre-deployment tests
    -d, --dry-run           Show what would be deployed without actually deploying
    -h, --help              Show this help message

EXAMPLES:
    $0                                    # Deploy to dev environment
    $0 -e staging -p staging-profile     # Deploy to staging with specific profile
    $0 -e prod -r us-west-2             # Deploy to production in us-west-2

PREREQUISITES:
    - AWS CLI configured
    - CDK CLI installed (npm install -g aws-cdk)
    - Python 3.9+ with required dependencies
    - Docker (for Lambda packaging)

EOF
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
        -s|--skip-tests)
            SKIP_TESTS=true
            shift
            ;;
        -d|--dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
    print_error "Invalid environment: $ENVIRONMENT. Must be dev, staging, or prod."
    exit 1
fi

# Set AWS profile if provided
if [[ -n "$PROFILE" ]]; then
    export AWS_PROFILE="$PROFILE"
    print_status "Using AWS profile: $PROFILE"
fi

# Set AWS region
export CDK_DEFAULT_REGION="$REGION"
print_status "Using AWS region: $REGION"

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
INFRASTRUCTURE_DIR="$PROJECT_ROOT/infrastructure"

print_status "Starting AVESA deployment for environment: $ENVIRONMENT"

# Change to infrastructure directory
cd "$INFRASTRUCTURE_DIR"

# Check prerequisites
print_status "Checking prerequisites..."

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    print_error "AWS CLI is not installed. Please install it first."
    exit 1
fi

# Check CDK CLI
if ! command -v cdk &> /dev/null; then
    print_error "CDK CLI is not installed. Please install it with: npm install -g aws-cdk"
    exit 1
fi

# Check Python
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed. Please install Python 3.9 or later."
    exit 1
fi

# Check Docker
if ! command -v docker &> /dev/null; then
    print_warning "Docker is not installed. Lambda packaging may fail."
fi

# Verify AWS credentials
print_status "Verifying AWS credentials..."
if ! aws sts get-caller-identity &> /dev/null; then
    print_error "AWS credentials are not configured or invalid."
    exit 1
fi

AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
print_success "AWS credentials verified. Account: $AWS_ACCOUNT"

# Set CDK environment variables
export CDK_DEFAULT_ACCOUNT="$AWS_ACCOUNT"

# For production, check if production account is set
if [[ "$ENVIRONMENT" == "prod" ]]; then
    if [[ -z "$CDK_PROD_ACCOUNT" ]]; then
        print_error "CDK_PROD_ACCOUNT environment variable must be set for production deployment."
        print_error "Example: export CDK_PROD_ACCOUNT=987654321098"
        exit 1
    fi
    print_status "Production account: $CDK_PROD_ACCOUNT"
fi

# Install Python dependencies
print_status "Installing Python dependencies..."
if [[ -f "requirements.txt" ]]; then
    pip install -r requirements.txt
fi

# Run pre-deployment tests if not skipped
if [[ "$SKIP_TESTS" == false ]]; then
    print_status "Running pre-deployment tests..."
    
    # Check if test directory exists
    if [[ -d "$PROJECT_ROOT/tests" ]]; then
        cd "$PROJECT_ROOT"
        python3 -m pytest tests/ -v
        cd "$INFRASTRUCTURE_DIR"
    else
        print_warning "No tests directory found, skipping tests."
    fi
fi

# Package Lambda functions
print_status "Packaging Lambda functions..."

# Create lambda packages directory if it doesn't exist
mkdir -p "$PROJECT_ROOT/lambda-packages"

# Use the lightweight packaging script to ensure mapping files are included
print_status "Packaging canonical transform functions with mapping files..."
cd "$PROJECT_ROOT"
python3 scripts/package-lightweight-lambdas.py --function canonical --output-dir lambda-packages

# Package optimized functions (these will be updated to include mappings in future)
print_status "Packaging orchestrator..."
cd "$PROJECT_ROOT/src/optimized/orchestrator"
# Copy shared modules and mapping files to orchestrator
cp -r "$PROJECT_ROOT/src/shared"/* .
cp -r "$PROJECT_ROOT/mappings" .
zip -r "$PROJECT_ROOT/lambda-packages/optimized-orchestrator.zip" . -x "*.pyc" "__pycache__/*"
# Clean up copied files
rm -rf shared mappings *.py 2>/dev/null || true

print_status "Packaging processors..."
cd "$PROJECT_ROOT/src/optimized/processors"
# Copy shared modules and mapping files to processors
cp -r "$PROJECT_ROOT/src/shared"/* .
cp -r "$PROJECT_ROOT/mappings" .
zip -r "$PROJECT_ROOT/lambda-packages/optimized-processors.zip" . -x "*.pyc" "__pycache__/*"
# Clean up copied files
rm -rf shared mappings *.py 2>/dev/null || true

# Return to infrastructure directory
cd "$INFRASTRUCTURE_DIR"

# Bootstrap CDK if needed
print_status "Checking CDK bootstrap status..."
if ! cdk bootstrap aws://$AWS_ACCOUNT/$REGION --context environment=$ENVIRONMENT 2>/dev/null; then
    print_status "Bootstrapping CDK..."
    cdk bootstrap aws://$AWS_ACCOUNT/$REGION --context environment=$ENVIRONMENT
fi

# Synthesize CDK app
print_status "Synthesizing CDK application..."
if [[ "$DRY_RUN" == true ]]; then
    print_status "DRY RUN: Showing what would be deployed..."
    cdk synth --app "python3 app.py" --context environment=$ENVIRONMENT
    exit 0
fi

# Deploy the stacks
print_status "Deploying AVESA stacks..."

# Deploy available stacks (based on current app.py configuration)
STACKS_TO_DEPLOY=(
    "AVESAPerformanceOptimization-$ENVIRONMENT"
    "AVESABackfill-$ENVIRONMENT"
    "AVESAClickHouse-$ENVIRONMENT"
)

for stack in "${STACKS_TO_DEPLOY[@]}"; do
    print_status "Deploying stack: $stack"
    
    if cdk deploy "$stack" \
        --app "python3 app.py" \
        --context environment=$ENVIRONMENT \
        --require-approval never \
        --progress events; then
        print_success "Successfully deployed: $stack"
    else
        print_error "Failed to deploy: $stack"
        exit 1
    fi
done

# Post-deployment verification
print_status "Running post-deployment verification..."

# Check if Step Functions were created
print_status "Verifying Step Functions state machines..."
EXPECTED_STATE_MACHINES=(
    "PipelineOrchestrator-$ENVIRONMENT"
    "TenantProcessor-$ENVIRONMENT"
    "TableProcessor-$ENVIRONMENT"
)

for state_machine in "${EXPECTED_STATE_MACHINES[@]}"; do
    if aws stepfunctions describe-state-machine --state-machine-arn "arn:aws:states:$REGION:$AWS_ACCOUNT:stateMachine:$state_machine" &> /dev/null; then
        print_success "State machine verified: $state_machine"
    else
        print_warning "State machine not found: $state_machine"
    fi
done

# Check if Lambda functions were created
print_status "Verifying Lambda functions..."
EXPECTED_FUNCTIONS=(
    "avesa-pipeline-orchestrator-$ENVIRONMENT"
    "avesa-tenant-processor-$ENVIRONMENT"
    "avesa-table-processor-$ENVIRONMENT"
    "avesa-chunk-processor-$ENVIRONMENT"
)

for function in "${EXPECTED_FUNCTIONS[@]}"; do
    if aws lambda get-function --function-name "$function" &> /dev/null; then
        print_success "Lambda function verified: $function"
    else
        print_warning "Lambda function not found: $function"
    fi
done

# Check if DynamoDB tables were created
print_status "Verifying DynamoDB tables..."
EXPECTED_TABLES=(
    "ProcessingJobs-$ENVIRONMENT"
    "ChunkProgress-$ENVIRONMENT"
)

for table in "${EXPECTED_TABLES[@]}"; do
    if aws dynamodb describe-table --table-name "$table" &> /dev/null; then
        print_success "DynamoDB table verified: $table"
    else
        print_warning "DynamoDB table not found: $table"
    fi
done

print_success "AVESA deployment completed successfully!"
print_status "Environment: $ENVIRONMENT"
print_status "Region: $REGION"
print_status "Account: $AWS_ACCOUNT"

# Show next steps
cat << EOF

${GREEN}Next Steps:${NC}
1. Test the pipeline:
   aws stepfunctions start-execution \\
     --state-machine-arn "arn:aws:states:$REGION:$AWS_ACCOUNT:stateMachine:PipelineOrchestrator-$ENVIRONMENT" \\
     --input '{"tenant_id": "test-tenant"}'

2. Monitor execution in AWS Console:
   - Step Functions: https://$REGION.console.aws.amazon.com/states/home?region=$REGION
   - CloudWatch Dashboards: https://$REGION.console.aws.amazon.com/cloudwatch/home?region=$REGION#dashboards:

3. View logs in CloudWatch:
   - Log Groups: /aws/lambda/avesa-* and /aws/stepfunctions/*

4. Check performance metrics in the AVESA-Pipeline-$ENVIRONMENT dashboard

EOF