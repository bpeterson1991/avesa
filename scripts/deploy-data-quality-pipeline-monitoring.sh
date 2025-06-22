#!/bin/bash

# Deploy Universal Data Quality Pipeline Monitoring Infrastructure
# ================================================================
# This script deploys comprehensive monitoring for the entire canonical data pipeline:
# 1. Raw Data Ingestion Quality
# 2. Canonical Transformation Quality
# 3. ClickHouse Loading Quality
# 4. Final Data State Quality
#
# Monitors ALL canonical tables (companies, contacts, tickets, time_entries)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
INFRASTRUCTURE_DIR="$PROJECT_ROOT/infrastructure"

# Default values
ENVIRONMENT="dev"
EMAIL_ENDPOINT=""
DEPLOY_ONLY_MONITORING=false

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

Deploy Universal ClickHouse Monitoring Infrastructure

OPTIONS:
    -e, --environment ENV       Target environment (dev, staging, prod) [default: dev]
    -m, --email EMAIL          Email address for SNS alerts
    -o, --monitoring-only      Deploy only monitoring stack (skip other stacks)
    -h, --help                 Show this help message

EXAMPLES:
    # Deploy to dev environment with email alerts
    $0 --environment dev --email admin@company.com
    
    # Deploy only monitoring stack to production
    $0 --environment prod --email prod-alerts@company.com --monitoring-only
    
    # Deploy to staging without email alerts
    $0 --environment staging

PREREQUISITES:
    - AWS CLI configured with appropriate credentials
    - AWS CDK installed (npm install -g aws-cdk)
    - Python 3.9+ with required dependencies
    - ClickHouse connection secrets configured in AWS Secrets Manager

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        -m|--email)
            EMAIL_ENDPOINT="$2"
            shift 2
            ;;
        -o|--monitoring-only)
            DEPLOY_ONLY_MONITORING=true
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
    print_error "Invalid environment: $ENVIRONMENT. Must be one of: dev, staging, prod"
    exit 1
fi

print_status "Starting Universal Data Quality Pipeline Monitoring Deployment"
print_status "Environment: $ENVIRONMENT"
print_status "Deploy monitoring only: $DEPLOY_ONLY_MONITORING"
if [[ -n "$EMAIL_ENDPOINT" ]]; then
    print_status "Email alerts: $EMAIL_ENDPOINT"
else
    print_warning "No email endpoint specified - SNS topic will be created without email subscription"
fi

# Change to infrastructure directory
cd "$INFRASTRUCTURE_DIR"

# Check if CDK is installed
if ! command -v cdk &> /dev/null; then
    print_error "AWS CDK is not installed. Please install it with: npm install -g aws-cdk"
    exit 1
fi

# Check if Python dependencies are installed
print_status "Checking Python dependencies..."
if ! python3 -c "import aws_cdk" 2>/dev/null; then
    print_warning "Installing Python dependencies..."
    pip3 install -r requirements.txt
fi

# Validate AWS credentials
print_status "Validating AWS credentials..."
if ! aws sts get-caller-identity &>/dev/null; then
    print_error "AWS credentials not configured or invalid"
    print_error "Please run: aws configure"
    exit 1
fi

# Get current account and region
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region)
print_status "Deploying to account: $ACCOUNT_ID in region: $REGION"

# Bootstrap CDK if needed
print_status "Checking CDK bootstrap status..."
if ! aws cloudformation describe-stacks --stack-name CDKToolkit &>/dev/null; then
    print_warning "CDK not bootstrapped. Bootstrapping now..."
    cdk bootstrap aws://$ACCOUNT_ID/$REGION
    print_success "CDK bootstrap completed"
fi

# Function to deploy specific stack
deploy_stack() {
    local stack_name="$1"
    local stack_description="$2"
    
    print_status "Deploying $stack_description..."
    
    if cdk deploy "$stack_name" --context environment="$ENVIRONMENT" --require-approval never; then
        print_success "$stack_description deployed successfully"
    else
        print_error "Failed to deploy $stack_description"
        return 1
    fi
}

# Deploy stacks based on options
if [[ "$DEPLOY_ONLY_MONITORING" == "true" ]]; then
    # Deploy only monitoring stack
    MONITORING_STACK_NAME="AVESADataQualityPipelineMonitoring"
    case $ENVIRONMENT in
        dev) MONITORING_STACK_NAME="${MONITORING_STACK_NAME}Dev" ;;
        staging) MONITORING_STACK_NAME="${MONITORING_STACK_NAME}Staging" ;;
        prod) MONITORING_STACK_NAME="${MONITORING_STACK_NAME}Prod" ;;
    esac
    
    deploy_stack "$MONITORING_STACK_NAME" "Universal Data Quality Pipeline Monitoring Stack"
else
    # Deploy all stacks
    print_status "Deploying all infrastructure stacks..."
    
    # Get stack suffix from environment config
    STACK_SUFFIX=""
    case $ENVIRONMENT in
        dev) STACK_SUFFIX="Dev" ;;
        staging) STACK_SUFFIX="Staging" ;;
        prod) STACK_SUFFIX="Prod" ;;
    esac
    
    # Deploy in dependency order
    deploy_stack "AVESAPerformanceOptimization$STACK_SUFFIX" "Performance Optimization Stack" || exit 1
    deploy_stack "AVESABackfill$STACK_SUFFIX" "Backfill Stack" || exit 1
    deploy_stack "AVESAClickHouse$STACK_SUFFIX" "ClickHouse Stack" || exit 1
    deploy_stack "AVESADataQualityPipelineMonitoring$STACK_SUFFIX" "Universal Data Quality Pipeline Monitoring Stack" || exit 1
fi

# Configure email subscription if provided
if [[ -n "$EMAIL_ENDPOINT" ]]; then
    print_status "Configuring email subscription for SNS alerts..."
    
    # Get SNS topic ARN from CloudFormation outputs
    TOPIC_ARN=$(aws cloudformation describe-stacks \
        --stack-name "AVESAClickHouseMonitoring$STACK_SUFFIX" \
        --query 'Stacks[0].Outputs[?OutputKey==`AlertTopicArn`].OutputValue' \
        --output text 2>/dev/null || echo "")
    
    if [[ -n "$TOPIC_ARN" ]]; then
        print_status "Found SNS topic: $TOPIC_ARN"
        
        # Subscribe email to topic
        if aws sns subscribe \
            --topic-arn "$TOPIC_ARN" \
            --protocol email \
            --notification-endpoint "$EMAIL_ENDPOINT" &>/dev/null; then
            print_success "Email subscription created for $EMAIL_ENDPOINT"
            print_warning "Please check your email and confirm the subscription"
        else
            print_warning "Failed to create email subscription (may already exist)"
        fi
    else
        print_warning "Could not find SNS topic ARN - email subscription skipped"
    fi
fi

# Get deployment outputs
print_status "Retrieving deployment information..."

# Function to get stack output
get_stack_output() {
    local stack_name="$1"
    local output_key="$2"
    aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --query "Stacks[0].Outputs[?OutputKey=='$output_key'].OutputValue" \
        --output text 2>/dev/null || echo "Not found"
}

MONITORING_STACK_NAME="AVESADataQualityPipelineMonitoring$STACK_SUFFIX"
DASHBOARD_URL=$(get_stack_output "$MONITORING_STACK_NAME" "DashboardUrl")
LAMBDA_FUNCTION_NAME=$(get_stack_output "$MONITORING_STACK_NAME" "MonitoringLambdaName")

print_success "Universal ClickHouse Monitoring Deployment Complete!"
echo
echo "=== DEPLOYMENT SUMMARY ==="
echo "Environment: $ENVIRONMENT"
echo "Region: $REGION"
echo "Account: $ACCOUNT_ID"
echo
echo "=== MONITORING RESOURCES ==="
echo "CloudWatch Dashboard: $DASHBOARD_URL"
echo "Lambda Function: $LAMBDA_FUNCTION_NAME"
echo "SNS Topic: $TOPIC_ARN"
echo
echo "=== MONITORED TABLES ==="
echo "• Companies"
echo "• Contacts"
echo "• Tickets"
echo "• Time Entries"
echo
echo "=== NEXT STEPS ==="
echo "1. Confirm email subscription if configured"
echo "2. Review CloudWatch dashboard for current status"
echo "3. Test monitoring by running: aws lambda invoke --function-name $LAMBDA_FUNCTION_NAME /tmp/test-output.json"
echo "4. Configure additional email endpoints if needed"
echo
print_success "Deployment completed successfully!"