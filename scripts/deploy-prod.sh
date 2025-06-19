#!/bin/bash
# Production deployment script for hybrid AWS account strategy

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROD_PROFILE="avesa-production"
REGION="us-east-2"
ENVIRONMENT="prod"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}AVESA Production Deployment${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Environment: ${ENVIRONMENT}"
echo -e "AWS Profile: ${PROD_PROFILE}"
echo -e "Region: ${REGION}"
echo ""

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check AWS profile
check_aws_profile() {
    echo -e "${YELLOW}Checking AWS profile...${NC}"
    if ! aws sts get-caller-identity --profile ${PROD_PROFILE} > /dev/null 2>&1; then
        echo -e "${RED}Error: AWS profile '${PROD_PROFILE}' not configured or not accessible${NC}"
        echo -e "${YELLOW}Please configure the profile:${NC}"
        echo "  aws configure --profile ${PROD_PROFILE}"
        echo ""
        echo -e "${YELLOW}Or set up cross-account role access:${NC}"
        echo "  aws configure set role_arn arn:aws:iam::PROD_ACCOUNT_ID:role/DeploymentRole --profile ${PROD_PROFILE}"
        echo "  aws configure set source_profile default --profile ${PROD_PROFILE}"
        exit 1
    fi
    
    ACCOUNT_ID=$(aws sts get-caller-identity --profile ${PROD_PROFILE} --query Account --output text)
    echo -e "${GREEN}✓ AWS profile configured for account: ${ACCOUNT_ID}${NC}"
}

# Function to check prerequisites
check_prerequisites() {
    echo -e "${YELLOW}Checking prerequisites...${NC}"
    
    # Check if CDK is installed
    if ! command_exists cdk; then
        echo -e "${RED}Error: AWS CDK not installed${NC}"
        echo "Please install CDK: npm install -g aws-cdk"
        exit 1
    fi
    echo -e "${GREEN}✓ AWS CDK installed${NC}"
    
    # Check if we're in the right directory
    if [ ! -f "infrastructure/app.py" ]; then
        echo -e "${RED}Error: Must run from project root directory${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Running from project root${NC}"
    
    # Check if Python dependencies are installed
    if [ ! -d "infrastructure/.venv" ] && [ ! -f "infrastructure/requirements.txt" ]; then
        echo -e "${YELLOW}Warning: Python virtual environment not found${NC}"
        echo "Consider setting up: cd infrastructure && python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
    fi
    echo -e "${GREEN}✓ Prerequisites checked${NC}"
}

# Function to set environment variables
set_environment_variables() {
    echo -e "${YELLOW}Setting environment variables...${NC}"
    
    # Set production account ID
    if [ -z "$CDK_PROD_ACCOUNT" ]; then
        export CDK_PROD_ACCOUNT=$(aws sts get-caller-identity --profile ${PROD_PROFILE} --query Account --output text)
        echo -e "${GREEN}✓ CDK_PROD_ACCOUNT set to: ${CDK_PROD_ACCOUNT}${NC}"
    fi
    
    # Set default region
    export CDK_DEFAULT_REGION=${REGION}
    echo -e "${GREEN}✓ CDK_DEFAULT_REGION set to: ${CDK_DEFAULT_REGION}${NC}"
    
    # Set AWS profile
    export AWS_PROFILE=${PROD_PROFILE}
    echo -e "${GREEN}✓ AWS_PROFILE set to: ${AWS_PROFILE}${NC}"
}

# Function to bootstrap CDK (if needed)
bootstrap_cdk() {
    echo -e "${YELLOW}Checking CDK bootstrap...${NC}"
    
    # Check if CDK is already bootstrapped
    if aws cloudformation describe-stacks --stack-name CDKToolkit --profile ${PROD_PROFILE} --region ${REGION} > /dev/null 2>&1; then
        echo -e "${GREEN}✓ CDK already bootstrapped${NC}"
    else
        echo -e "${YELLOW}Bootstrapping CDK...${NC}"
        cd infrastructure
        cdk bootstrap --profile ${PROD_PROFILE} --context environment=${ENVIRONMENT}
        cd ..
        echo -e "${GREEN}✓ CDK bootstrapped${NC}"
    fi
}

# Function to deploy infrastructure
deploy_infrastructure() {
    echo -e "${YELLOW}Deploying infrastructure...${NC}"
    
    cd infrastructure
    
    # Synthesize first to check for errors
    echo -e "${YELLOW}Synthesizing CDK app...${NC}"
    cdk synth --context environment=${ENVIRONMENT} --profile ${PROD_PROFILE}
    echo -e "${GREEN}✓ CDK synthesis successful${NC}"
    
    # Deploy all stacks
    echo -e "${YELLOW}Deploying stacks...${NC}"
    cdk deploy --all --context environment=${ENVIRONMENT} --profile ${PROD_PROFILE} --require-approval never
    
    cd ..
    echo -e "${GREEN}✓ Infrastructure deployed${NC}"
}

# Function to validate deployment
validate_deployment() {
    echo -e "${YELLOW}Validating deployment...${NC}"
    
    # Check CloudFormation stacks
    echo -e "${YELLOW}Checking CloudFormation stacks...${NC}"
    STACKS=$(aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE --profile ${PROD_PROFILE} --region ${REGION} --query 'StackSummaries[?contains(StackName, `ConnectWise`)].StackName' --output text)
    
    if [ -z "$STACKS" ]; then
        echo -e "${RED}❌ No ConnectWise stacks found${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Found stacks: ${STACKS}${NC}"
    
    # Check Lambda functions
    echo -e "${YELLOW}Checking Lambda functions...${NC}"
    FUNCTIONS=$(aws lambda list-functions --profile ${PROD_PROFILE} --region ${REGION} --query 'Functions[?contains(FunctionName, `avesa`) && contains(FunctionName, `prod`)].FunctionName' --output text)
    
    if [ -z "$FUNCTIONS" ]; then
        echo -e "${RED}❌ No production Lambda functions found${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Found Lambda functions: ${FUNCTIONS}${NC}"
    
    # Check DynamoDB tables
    echo -e "${YELLOW}Checking DynamoDB tables...${NC}"
    if aws dynamodb describe-table --table-name TenantServices --profile ${PROD_PROFILE} --region ${REGION} > /dev/null 2>&1; then
        echo -e "${GREEN}✓ TenantServices table exists${NC}"
    else
        echo -e "${RED}❌ TenantServices table not found${NC}"
        exit 1
    fi
    
    if aws dynamodb describe-table --table-name LastUpdated --profile ${PROD_PROFILE} --region ${REGION} > /dev/null 2>&1; then
        echo -e "${GREEN}✓ LastUpdated table exists${NC}"
    else
        echo -e "${RED}❌ LastUpdated table not found${NC}"
        exit 1
    fi
    
    # Check S3 bucket
    echo -e "${YELLOW}Checking S3 bucket...${NC}"
    if aws s3 ls s3://data-storage-msp-prod --profile ${PROD_PROFILE} --region ${REGION} > /dev/null 2>&1; then
        echo -e "${GREEN}✓ S3 bucket data-storage-msp-prod exists${NC}"
    else
        echo -e "${RED}❌ S3 bucket data-storage-msp-prod not found${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Deployment validation successful${NC}"
}

# Function to show next steps
show_next_steps() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Deployment Complete!${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    echo -e "${GREEN}Next steps:${NC}"
    echo ""
    echo -e "${YELLOW}1. Migrate production data:${NC}"
    echo "   python3 scripts/migrate-production-data.py --dry-run"
    echo "   python3 scripts/migrate-production-data.py --execute"
    echo ""
    echo -e "${YELLOW}2. Test Lambda functions:${NC}"
    echo "   aws lambda invoke --function-name avesa-connectwise-ingestion-prod --payload '{}' response.json --profile ${PROD_PROFILE}"
    echo ""
    echo -e "${YELLOW}3. Monitor logs:${NC}"
    echo "   aws logs tail /aws/lambda/avesa-connectwise-ingestion-prod --follow --profile ${PROD_PROFILE}"
    echo ""
    echo -e "${YELLOW}4. Update application configuration to use production account${NC}"
    echo ""
    echo -e "${YELLOW}5. Set up monitoring and alerting${NC}"
    echo ""
}

# Main execution
main() {
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --profile)
                PROD_PROFILE="$2"
                shift 2
                ;;
            --region)
                REGION="$2"
                shift 2
                ;;
            --help)
                echo "Usage: $0 [--profile PROFILE] [--region REGION]"
                echo ""
                echo "Options:"
                echo "  --profile PROFILE    AWS profile for production account (default: avesa-production)"
                echo "  --region REGION      AWS region (default: us-east-1)"
                echo "  --help              Show this help message"
                exit 0
                ;;
            *)
                echo -e "${RED}Unknown option: $1${NC}"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
    
    # Execute deployment steps
    check_prerequisites
    check_aws_profile
    set_environment_variables
    bootstrap_cdk
    deploy_infrastructure
    validate_deployment
    show_next_steps
}

# Run main function
main "$@"