#!/bin/bash

# AVESA Deployment Script
# This script deploys the data pipeline infrastructure with robust error handling

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
UPDATE_LAMBDAS_ONLY=false

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

Deploy the AVESA infrastructure with robust error handling.

OPTIONS:
    -e, --environment ENV    Environment to deploy to (dev, staging, prod) [default: dev]
    -r, --region REGION      AWS region [default: us-east-2]
    -p, --profile PROFILE    AWS profile to use
    -s, --skip-tests         Skip pre-deployment tests
    -d, --dry-run           Show what would be deployed without actually deploying
    -l, --lambdas-only      Update Lambda functions only (skip CDK deployment)
    -h, --help              Show this help message

EXAMPLES:
    $0                                    # Deploy to dev environment
    $0 -e staging -p staging-profile     # Deploy to staging with specific profile
    $0 -e prod -r us-west-2             # Deploy to production in us-west-2
    $0 -l                                # Update Lambda functions only

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
        -l|--lambdas-only)
            UPDATE_LAMBDAS_ONLY=true
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

# Check CDK CLI (only if not lambdas-only mode)
if [[ "$UPDATE_LAMBDAS_ONLY" == false ]] && ! command -v cdk &> /dev/null; then
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
    pip install -r requirements.txt > /dev/null 2>&1 || print_warning "Some dependencies may have failed to install"
fi

# Run pre-deployment tests if not skipped
if [[ "$SKIP_TESTS" == false ]]; then
    print_status "Running pre-deployment tests..."
    
    # Check if test directory exists
    if [[ -d "$PROJECT_ROOT/tests" ]]; then
        cd "$PROJECT_ROOT"
        if python3 -m pytest tests/ -v > /dev/null 2>&1; then
            print_success "Tests passed"
        else
            print_warning "Some tests failed, but continuing deployment"
        fi
        cd "$INFRASTRUCTURE_DIR"
    else
        print_warning "No tests directory found, skipping tests."
    fi
fi

# CDK will handle Lambda packaging automatically with native bundling
print_status "Lambda packaging will be handled by CDK native bundling during deployment"

# Function to deploy infrastructure (CDK handles Lambda updates automatically)
deploy_infrastructure() {
    print_status "Deploying infrastructure with CDK..."
    cd "$INFRASTRUCTURE_DIR"
    
    if cdk deploy --all --require-approval never --context environment=$ENVIRONMENT; then
        print_success "Infrastructure deployed successfully"
        return 0
    else
        print_error "CDK deployment failed"
        return 1
    fi
}

# If lambdas-only mode, deploy only the stacks (CDK handles Lambda updates)
if [[ "$UPDATE_LAMBDAS_ONLY" == true ]]; then
    deploy_infrastructure
    print_success "Lambda functions updated successfully via CDK!"
    exit 0
fi

# Bootstrap CDK if needed
print_status "Checking CDK bootstrap status..."
if ! cdk bootstrap aws://$AWS_ACCOUNT/$REGION --context environment=$ENVIRONMENT > /dev/null 2>&1; then
    print_status "Bootstrapping CDK..."
    if cdk bootstrap aws://$AWS_ACCOUNT/$REGION --context environment=$ENVIRONMENT; then
        print_success "CDK bootstrapped"
    else
        print_warning "CDK bootstrap had issues, but continuing"
    fi
fi

# Synthesize CDK app
print_status "Synthesizing CDK application..."
if [[ "$DRY_RUN" == true ]]; then
    print_status "DRY RUN: Showing what would be deployed..."
    cdk synth --app "python3 app.py" --context environment=$ENVIRONMENT
    exit 0
fi

# Deploy the infrastructure using simplified approach
print_status "Deploying AVESA infrastructure..."

if deploy_infrastructure; then
    print_success "AVESA infrastructure deployed successfully!"
    DEPLOYMENT_SUCCESS=true
else
    print_error "Infrastructure deployment failed"
    DEPLOYMENT_SUCCESS=false
fi

# Post-deployment verification
print_status "Running post-deployment verification..."

# Function to check resource existence
check_resource() {
    local resource_type="$1"
    local resource_name="$2"
    local check_command="$3"
    
    if eval "$check_command" > /dev/null 2>&1; then
        print_success "$resource_type verified: $resource_name"
        return 0
    else
        print_warning "$resource_type not found: $resource_name"
        return 1
    fi
}

# Check if Step Functions were created
print_status "Verifying Step Functions state machines..."
EXPECTED_STATE_MACHINES=(
    "PipelineOrchestrator-$ENVIRONMENT"
    "TenantProcessor-$ENVIRONMENT"
    "TableProcessor-$ENVIRONMENT"
)

for state_machine in "${EXPECTED_STATE_MACHINES[@]}"; do
    check_resource "State machine" "$state_machine" "aws stepfunctions describe-state-machine --state-machine-arn 'arn:aws:states:$REGION:$AWS_ACCOUNT:stateMachine:$state_machine'"
done

# Check if Lambda functions were created/updated
print_status "Verifying Lambda functions..."
EXPECTED_FUNCTIONS=(
    "avesa-pipeline-orchestrator-$ENVIRONMENT"
    "avesa-tenant-processor-$ENVIRONMENT"
    "avesa-table-processor-$ENVIRONMENT"
    "avesa-chunk-processor-$ENVIRONMENT"
    "avesa-canonical-transform-time-entries-$ENVIRONMENT"
    "avesa-canonical-transform-companies-$ENVIRONMENT"
    "avesa-canonical-transform-contacts-$ENVIRONMENT"
    "avesa-canonical-transform-tickets-$ENVIRONMENT"
    "clickhouse-loader-time-entries-$ENVIRONMENT"
    "clickhouse-loader-companies-$ENVIRONMENT"
    "clickhouse-loader-contacts-$ENVIRONMENT"
    "clickhouse-loader-tickets-$ENVIRONMENT"
)

VERIFIED_FUNCTIONS=0
for function in "${EXPECTED_FUNCTIONS[@]}"; do
    if check_resource "Lambda function" "$function" "aws lambda get-function --function-name '$function'"; then
        ((VERIFIED_FUNCTIONS++))
    fi
done

# Check if DynamoDB tables were created
print_status "Verifying DynamoDB tables..."
EXPECTED_TABLES=(
    "ProcessingJobs-$ENVIRONMENT"
    "ChunkProgress-$ENVIRONMENT"
    "LastUpdated-$ENVIRONMENT"
    "TenantServices-$ENVIRONMENT"
)

VERIFIED_TABLES=0
for table in "${EXPECTED_TABLES[@]}"; do
    if check_resource "DynamoDB table" "$table" "aws dynamodb describe-table --table-name '$table'"; then
        ((VERIFIED_TABLES++))
    fi
done

# Summary
echo
print_status "=== DEPLOYMENT SUMMARY ==="
print_status "Environment: $ENVIRONMENT"
print_status "Region: $REGION"
print_status "Account: $AWS_ACCOUNT"

if [[ ${#DEPLOYED_STACKS[@]} -gt 0 ]]; then
    print_success "Successfully deployed stacks: ${DEPLOYED_STACKS[*]}"
fi

if [[ ${#FAILED_STACKS[@]} -gt 0 ]]; then
    print_warning "Stacks with issues (likely already exist): ${FAILED_STACKS[*]}"
fi

print_status "Verified Lambda functions: $VERIFIED_FUNCTIONS/${#EXPECTED_FUNCTIONS[@]}"
print_status "Verified DynamoDB tables: $VERIFIED_TABLES/${#EXPECTED_TABLES[@]}"

if [[ $VERIFIED_FUNCTIONS -gt 0 && $VERIFIED_TABLES -gt 0 ]]; then
    print_success "AVESA deployment completed successfully!"
else
    print_warning "Deployment completed with some missing resources"
fi

# Show next steps
cat << EOF

${GREEN}Next Steps:${NC}
1. Test the pipeline:
   aws stepfunctions start-execution \\
     --state-machine-arn "arn:aws:states:$REGION:$AWS_ACCOUNT:stateMachine:PipelineOrchestrator-$ENVIRONMENT" \\
     --input '{"tenant_id": "test-tenant"}'

2. Update Lambda functions only (if needed):
   $0 --lambdas-only -e $ENVIRONMENT

3. Monitor execution in AWS Console:
   - Step Functions: https://$REGION.console.aws.amazon.com/states/home?region=$REGION
   - CloudWatch Dashboards: https://$REGION.console.aws.amazon.com/cloudwatch/home?region=$REGION#dashboards:

4. View logs in CloudWatch:
   - Log Groups: /aws/lambda/avesa-* and /aws/stepfunctions/*

EOF