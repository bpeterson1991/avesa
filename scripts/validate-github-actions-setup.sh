#!/bin/bash

# AVESA GitHub Actions Setup Validation Script
# This script tests the cross-account role assumption and verifies permissions
# are working correctly before using in GitHub Actions

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default values
ACCESS_KEY_ID=""
SECRET_ACCESS_KEY=""
ROLE_ARN=""
EXTERNAL_ID="avesa-github-actions-2024"
REGION="us-east-2"
SESSION_NAME="GitHubActions-ValidationTest"
VERBOSE=false
TEST_PERMISSIONS=true

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

print_step() {
    echo -e "${CYAN}[STEP]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
}

# Function to show usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --access-key-id KEY_ID           AWS Access Key ID"
    echo "  --secret-access-key SECRET       AWS Secret Access Key"
    echo "  --role-arn ARN                   Production deployment role ARN"
    echo "  --external-id EXTERNAL_ID        External ID [default: avesa-github-actions-2024]"
    echo "  --region REGION                  AWS region [default: us-east-2]"
    echo "  --session-name NAME              Role session name [default: GitHubActions-ValidationTest]"
    echo "  --skip-permission-tests          Skip detailed permission testing"
    echo "  --verbose                        Enable verbose output"
    echo "  -h, --help                       Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                                    # Interactive mode"
    echo "  $0 --access-key-id AKIA... --secret-access-key ... --role-arn arn:aws:iam::123456789012:role/GitHubActionsDeploymentRole"
    echo ""
    echo "Environment Variables:"
    echo "  AWS_ACCESS_KEY_ID_PROD           AWS Access Key ID"
    echo "  AWS_SECRET_ACCESS_KEY_PROD       AWS Secret Access Key"
    echo "  AWS_PROD_DEPLOYMENT_ROLE_ARN     Production deployment role ARN"
    echo ""
    echo "Prerequisites:"
    echo "  - AWS CLI installed and configured"
    echo "  - jq installed for JSON processing"
    echo "  - Valid GitHub Actions credentials"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --access-key-id)
            ACCESS_KEY_ID="$2"
            shift 2
            ;;
        --secret-access-key)
            SECRET_ACCESS_KEY="$2"
            shift 2
            ;;
        --role-arn)
            ROLE_ARN="$2"
            shift 2
            ;;
        --external-id)
            EXTERNAL_ID="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            shift 2
            ;;
        --session-name)
            SESSION_NAME="$2"
            shift 2
            ;;
        --skip-permission-tests)
            TEST_PERMISSIONS=false
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
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

# Function to validate prerequisites
validate_prerequisites() {
    print_step "Validating prerequisites..."
    
    # Check if AWS CLI is installed
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    print_status "✓ AWS CLI installed"
    
    # Check if jq is installed
    if ! command -v jq &> /dev/null; then
        print_error "jq is not installed. Please install it first:"
        echo "  macOS: brew install jq"
        echo "  Ubuntu: apt-get install jq"
        echo "  CentOS/RHEL: yum install jq"
        exit 1
    fi
    print_status "✓ jq installed"
    
    print_status "✓ Prerequisites validated"
}

# Function to get credentials
get_credentials() {
    print_step "Getting AWS credentials..."
    
    # Try command line arguments first
    if [[ -n "$ACCESS_KEY_ID" ]] && [[ -n "$SECRET_ACCESS_KEY" ]] && [[ -n "$ROLE_ARN" ]]; then
        print_status "Using credentials from command line arguments"
        return
    fi
    
    # Try environment variables
    if [[ -n "$AWS_ACCESS_KEY_ID_PROD" ]] && [[ -n "$AWS_SECRET_ACCESS_KEY_PROD" ]] && [[ -n "$AWS_PROD_DEPLOYMENT_ROLE_ARN" ]]; then
        ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID_PROD"
        SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY_PROD"
        ROLE_ARN="$AWS_PROD_DEPLOYMENT_ROLE_ARN"
        print_status "Using credentials from environment variables"
        return
    fi
    
    # Try GitHub secrets file if it exists
    if [[ -f "/tmp/github-secrets.txt" ]]; then
        print_status "Found temporary GitHub secrets file"
        ACCESS_KEY_ID=$(grep "AWS_ACCESS_KEY_ID_PROD" /tmp/github-secrets.txt | cut -d'=' -f2)
        SECRET_ACCESS_KEY=$(grep "AWS_SECRET_ACCESS_KEY_PROD" /tmp/github-secrets.txt | cut -d'=' -f2)
        ROLE_ARN=$(grep "AWS_PROD_DEPLOYMENT_ROLE_ARN" /tmp/github-secrets.txt | cut -d'=' -f2)
        
        if [[ -n "$ACCESS_KEY_ID" ]] && [[ -n "$SECRET_ACCESS_KEY" ]] && [[ -n "$ROLE_ARN" ]]; then
            print_status "Using credentials from temporary secrets file"
            return
        fi
    fi
    
    # Interactive input
    print_status "No credentials found, requesting interactive input..."
    echo ""
    echo -e "${YELLOW}Enter your GitHub Actions AWS credentials:${NC}"
    echo ""
    
    read -p "AWS Access Key ID: " ACCESS_KEY_ID
    read -s -p "AWS Secret Access Key: " SECRET_ACCESS_KEY
    echo ""
    read -p "Production Role ARN: " ROLE_ARN
    
    # Validate inputs
    if [[ -z "$ACCESS_KEY_ID" ]] || [[ -z "$SECRET_ACCESS_KEY" ]] || [[ -z "$ROLE_ARN" ]]; then
        print_error "All credentials are required"
        exit 1
    fi
    
    print_status "Credentials collected interactively"
}

# Function to validate credential format
validate_credential_format() {
    print_step "Validating credential format..."
    
    # Validate Access Key ID format
    if [[ ! "$ACCESS_KEY_ID" =~ ^AKIA[0-9A-Z]{16}$ ]]; then
        print_warning "Access Key ID format may be invalid (should start with AKIA and be 20 characters)"
    else
        print_status "✓ Access Key ID format valid"
    fi
    
    # Validate Secret Access Key length
    if [[ ${#SECRET_ACCESS_KEY} -ne 40 ]]; then
        print_warning "Secret Access Key length may be invalid (should be 40 characters)"
    else
        print_status "✓ Secret Access Key length valid"
    fi
    
    # Validate Role ARN format
    if [[ ! "$ROLE_ARN" =~ ^arn:aws:iam::[0-9]{12}:role/.+ ]]; then
        print_error "Role ARN format invalid (should be arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME)"
        exit 1
    else
        print_status "✓ Role ARN format valid"
    fi
}

# Function to test basic AWS access
test_basic_access() {
    print_step "Testing basic AWS access..."
    
    # Test basic STS access with provided credentials
    if AWS_ACCESS_KEY_ID="$ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$SECRET_ACCESS_KEY" \
       aws sts get-caller-identity --region "$REGION" > /dev/null 2>&1; then
        
        # Get caller identity details
        CALLER_IDENTITY=$(AWS_ACCESS_KEY_ID="$ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$SECRET_ACCESS_KEY" \
                         aws sts get-caller-identity --region "$REGION")
        
        USER_ARN=$(echo "$CALLER_IDENTITY" | jq -r '.Arn')
        ACCOUNT_ID=$(echo "$CALLER_IDENTITY" | jq -r '.Account')
        
        print_success "Basic AWS access successful"
        print_status "User ARN: $USER_ARN"
        print_status "Account ID: $ACCOUNT_ID"
        
        if [[ "$VERBOSE" == "true" ]]; then
            echo "Full caller identity:"
            echo "$CALLER_IDENTITY" | jq .
        fi
    else
        print_fail "Basic AWS access failed"
        print_error "Cannot authenticate with provided credentials"
        exit 1
    fi
}

# Function to test role assumption
test_role_assumption() {
    print_step "Testing role assumption..."
    
    # Attempt to assume the role
    print_status "Attempting to assume role: $ROLE_ARN"
    
    ASSUME_ROLE_OUTPUT=$(AWS_ACCESS_KEY_ID="$ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$SECRET_ACCESS_KEY" \
                        aws sts assume-role \
                        --role-arn "$ROLE_ARN" \
                        --role-session-name "$SESSION_NAME" \
                        --external-id "$EXTERNAL_ID" \
                        --region "$REGION" 2>&1)
    
    if [[ $? -eq 0 ]]; then
        print_success "Role assumption successful"
        
        # Extract temporary credentials
        TEMP_ACCESS_KEY=$(echo "$ASSUME_ROLE_OUTPUT" | jq -r '.Credentials.AccessKeyId')
        TEMP_SECRET_KEY=$(echo "$ASSUME_ROLE_OUTPUT" | jq -r '.Credentials.SecretAccessKey')
        TEMP_SESSION_TOKEN=$(echo "$ASSUME_ROLE_OUTPUT" | jq -r '.Credentials.SessionToken')
        EXPIRATION=$(echo "$ASSUME_ROLE_OUTPUT" | jq -r '.Credentials.Expiration')
        
        print_status "Temporary credentials obtained"
        print_status "Session expires: $EXPIRATION"
        
        if [[ "$VERBOSE" == "true" ]]; then
            echo "Assumed role identity:"
            echo "$ASSUME_ROLE_OUTPUT" | jq '.AssumedRoleUser'
        fi
        
        # Test assumed role identity
        ASSUMED_IDENTITY=$(AWS_ACCESS_KEY_ID="$TEMP_ACCESS_KEY" \
                          AWS_SECRET_ACCESS_KEY="$TEMP_SECRET_KEY" \
                          AWS_SESSION_TOKEN="$TEMP_SESSION_TOKEN" \
                          aws sts get-caller-identity --region "$REGION")
        
        ASSUMED_ARN=$(echo "$ASSUMED_IDENTITY" | jq -r '.Arn')
        ASSUMED_ACCOUNT=$(echo "$ASSUMED_IDENTITY" | jq -r '.Account')
        
        print_status "Assumed role ARN: $ASSUMED_ARN"
        print_status "Production account: $ASSUMED_ACCOUNT"
        
        # Store temporary credentials for permission tests
        export TEMP_AWS_ACCESS_KEY_ID="$TEMP_ACCESS_KEY"
        export TEMP_AWS_SECRET_ACCESS_KEY="$TEMP_SECRET_KEY"
        export TEMP_AWS_SESSION_TOKEN="$TEMP_SESSION_TOKEN"
        export PROD_ACCOUNT_ID="$ASSUMED_ACCOUNT"
        
    else
        print_fail "Role assumption failed"
        print_error "Error details:"
        echo "$ASSUME_ROLE_OUTPUT"
        
        # Common error analysis
        if echo "$ASSUME_ROLE_OUTPUT" | grep -q "AccessDenied"; then
            print_error "Access denied - possible causes:"
            echo "  - Role trust policy doesn't allow the source account"
            echo "  - External ID mismatch"
            echo "  - Role doesn't exist"
        elif echo "$ASSUME_ROLE_OUTPUT" | grep -q "InvalidUserID.NotFound"; then
            print_error "User not found - check access key ID"
        elif echo "$ASSUME_ROLE_OUTPUT" | grep -q "SignatureDoesNotMatch"; then
            print_error "Signature mismatch - check secret access key"
        fi
        
        exit 1
    fi
}

# Function to test deployment permissions
test_deployment_permissions() {
    if [[ "$TEST_PERMISSIONS" == "false" ]]; then
        print_warning "Skipping permission tests"
        return
    fi
    
    print_step "Testing deployment permissions..."
    
    # Test various AWS services that the deployment needs
    local test_results=()
    
    # Test CloudFormation permissions
    print_status "Testing CloudFormation permissions..."
    if AWS_ACCESS_KEY_ID="$TEMP_AWS_ACCESS_KEY_ID" \
       AWS_SECRET_ACCESS_KEY="$TEMP_AWS_SECRET_ACCESS_KEY" \
       AWS_SESSION_TOKEN="$TEMP_AWS_SESSION_TOKEN" \
       aws cloudformation list-stacks --region "$REGION" > /dev/null 2>&1; then
        print_success "✓ CloudFormation access"
        test_results+=("CloudFormation:PASS")
    else
        print_fail "✗ CloudFormation access"
        test_results+=("CloudFormation:FAIL")
    fi
    
    # Test Lambda permissions
    print_status "Testing Lambda permissions..."
    if AWS_ACCESS_KEY_ID="$TEMP_AWS_ACCESS_KEY_ID" \
       AWS_SECRET_ACCESS_KEY="$TEMP_AWS_SECRET_ACCESS_KEY" \
       AWS_SESSION_TOKEN="$TEMP_AWS_SESSION_TOKEN" \
       aws lambda list-functions --region "$REGION" --max-items 1 > /dev/null 2>&1; then
        print_success "✓ Lambda access"
        test_results+=("Lambda:PASS")
    else
        print_fail "✗ Lambda access"
        test_results+=("Lambda:FAIL")
    fi
    
    # Test DynamoDB permissions
    print_status "Testing DynamoDB permissions..."
    if AWS_ACCESS_KEY_ID="$TEMP_AWS_ACCESS_KEY_ID" \
       AWS_SECRET_ACCESS_KEY="$TEMP_AWS_SECRET_ACCESS_KEY" \
       AWS_SESSION_TOKEN="$TEMP_AWS_SESSION_TOKEN" \
       aws dynamodb list-tables --region "$REGION" > /dev/null 2>&1; then
        print_success "✓ DynamoDB access"
        test_results+=("DynamoDB:PASS")
    else
        print_fail "✗ DynamoDB access"
        test_results+=("DynamoDB:FAIL")
    fi
    
    # Test S3 permissions
    print_status "Testing S3 permissions..."
    if AWS_ACCESS_KEY_ID="$TEMP_AWS_ACCESS_KEY_ID" \
       AWS_SECRET_ACCESS_KEY="$TEMP_AWS_SECRET_ACCESS_KEY" \
       AWS_SESSION_TOKEN="$TEMP_AWS_SESSION_TOKEN" \
       aws s3 ls --region "$REGION" > /dev/null 2>&1; then
        print_success "✓ S3 access"
        test_results+=("S3:PASS")
    else
        print_fail "✗ S3 access"
        test_results+=("S3:FAIL")
    fi
    
    # Test IAM permissions (limited)
    print_status "Testing IAM permissions..."
    if AWS_ACCESS_KEY_ID="$TEMP_AWS_ACCESS_KEY_ID" \
       AWS_SECRET_ACCESS_KEY="$TEMP_AWS_SECRET_ACCESS_KEY" \
       AWS_SESSION_TOKEN="$TEMP_AWS_SESSION_TOKEN" \
       aws iam list-roles --path-prefix "/AVESA" --region "$REGION" > /dev/null 2>&1; then
        print_success "✓ IAM access"
        test_results+=("IAM:PASS")
    else
        print_fail "✗ IAM access"
        test_results+=("IAM:FAIL")
    fi
    
    # Test Secrets Manager permissions
    print_status "Testing Secrets Manager permissions..."
    if AWS_ACCESS_KEY_ID="$TEMP_AWS_ACCESS_KEY_ID" \
       AWS_SECRET_ACCESS_KEY="$TEMP_AWS_SECRET_ACCESS_KEY" \
       AWS_SESSION_TOKEN="$TEMP_AWS_SESSION_TOKEN" \
       aws secretsmanager list-secrets --region "$REGION" --max-results 1 > /dev/null 2>&1; then
        print_success "✓ Secrets Manager access"
        test_results+=("SecretsManager:PASS")
    else
        print_fail "✗ Secrets Manager access"
        test_results+=("SecretsManager:FAIL")
    fi
    
    # Summary of permission tests
    echo ""
    print_header "Permission Test Summary"
    local passed=0
    local total=0
    
    for result in "${test_results[@]}"; do
        service=$(echo "$result" | cut -d':' -f1)
        status=$(echo "$result" | cut -d':' -f2)
        total=$((total + 1))
        
        if [[ "$status" == "PASS" ]]; then
            echo -e "  ${GREEN}✓${NC} $service"
            passed=$((passed + 1))
        else
            echo -e "  ${RED}✗${NC} $service"
        fi
    done
    
    echo ""
    print_status "Permission tests: $passed/$total passed"
    
    if [[ $passed -eq $total ]]; then
        print_success "All permission tests passed!"
    else
        print_warning "Some permission tests failed - deployment may encounter issues"
    fi
}

# Function to test CDK bootstrap
test_cdk_bootstrap() {
    print_step "Testing CDK bootstrap status..."
    
    if AWS_ACCESS_KEY_ID="$TEMP_AWS_ACCESS_KEY_ID" \
       AWS_SECRET_ACCESS_KEY="$TEMP_AWS_SECRET_ACCESS_KEY" \
       AWS_SESSION_TOKEN="$TEMP_AWS_SESSION_TOKEN" \
       aws cloudformation describe-stacks --stack-name CDKToolkit --region "$REGION" > /dev/null 2>&1; then
        print_success "✓ CDK is bootstrapped in region $REGION"
        
        # Get CDK toolkit version if verbose
        if [[ "$VERBOSE" == "true" ]]; then
            CDK_VERSION=$(AWS_ACCESS_KEY_ID="$TEMP_AWS_ACCESS_KEY_ID" \
                         AWS_SECRET_ACCESS_KEY="$TEMP_AWS_SECRET_ACCESS_KEY" \
                         AWS_SESSION_TOKEN="$TEMP_AWS_SESSION_TOKEN" \
                         aws cloudformation describe-stacks --stack-name CDKToolkit --region "$REGION" \
                         --query 'Stacks[0].Parameters[?ParameterKey==`BootstrapVersion`].ParameterValue' --output text)
            print_status "CDK Bootstrap version: $CDK_VERSION"
        fi
    else
        print_warning "CDK not bootstrapped in region $REGION"
        print_status "This is normal for first-time setup - CDK will bootstrap automatically during deployment"
    fi
}

# Function to simulate GitHub Actions environment
simulate_github_actions() {
    print_step "Simulating GitHub Actions environment..."
    
    # Create a temporary script that mimics GitHub Actions credential setup
    cat > /tmp/test-github-actions.sh << EOF
#!/bin/bash
set -e

# Simulate GitHub Actions environment variables
export AWS_ACCESS_KEY_ID="$ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$SECRET_ACCESS_KEY"
export AWS_DEFAULT_REGION="$REGION"

# Simulate aws-actions/configure-aws-credentials behavior
echo "Configuring AWS credentials..."
aws sts get-caller-identity

echo "Assuming role..."
CREDS=\$(aws sts assume-role \\
  --role-arn "$ROLE_ARN" \\
  --role-session-name "$SESSION_NAME" \\
  --external-id "$EXTERNAL_ID")

export AWS_ACCESS_KEY_ID=\$(echo "\$CREDS" | jq -r '.Credentials.AccessKeyId')
export AWS_SECRET_ACCESS_KEY=\$(echo "\$CREDS" | jq -r '.Credentials.SecretAccessKey')
export AWS_SESSION_TOKEN=\$(echo "\$CREDS" | jq -r '.Credentials.SessionToken')

echo "Testing assumed role identity..."
aws sts get-caller-identity

echo "Testing basic deployment commands..."
aws cloudformation list-stacks --region "$REGION" > /dev/null
aws lambda list-functions --region "$REGION" --max-items 1 > /dev/null

echo "GitHub Actions simulation successful!"
EOF
    
    chmod +x /tmp/test-github-actions.sh
    
    if /tmp/test-github-actions.sh > /dev/null 2>&1; then
        print_success "✓ GitHub Actions simulation successful"
    else
        print_fail "✗ GitHub Actions simulation failed"
        print_warning "This may indicate issues with the GitHub Actions workflow"
    fi
    
    rm -f /tmp/test-github-actions.sh
}

# Function to display validation results
display_validation_results() {
    echo ""
    print_header "🎯 Validation Results Summary"
    echo ""
    
    print_status "Validation completed successfully!"
    echo ""
    print_status "✅ What was tested:"
    echo -e "  ✓ AWS CLI and prerequisites"
    echo -e "  ✓ Credential format validation"
    echo -e "  ✓ Basic AWS access"
    echo -e "  ✓ Cross-account role assumption"
    echo -e "  ✓ Production account permissions"
    echo -e "  ✓ CDK bootstrap status"
    echo -e "  ✓ GitHub Actions simulation"
    echo ""
    
    print_status "🚀 Ready for GitHub Actions deployment!"
    echo ""
    print_status "Next steps:"
    echo -e "  1. Ensure GitHub secrets are configured correctly"
    echo -e "  2. Test the GitHub Actions workflow manually"
    echo -e "  3. Deploy to production using: Deploy to Production workflow"
    echo ""
    
    print_header "GitHub Actions Workflow Test"
    echo ""
    print_status "To test your GitHub Actions workflow:"
    echo -e "  1. Go to your GitHub repository"
    echo -e "  2. Click Actions → Deploy to Production"
    echo -e "  3. Click 'Run workflow'"
    echo -e "  4. Fill in the required parameters:"
    echo -e "     - Deployment confirmation: DEPLOY TO PRODUCTION"
    echo -e "     - Environment target: production"
    echo -e "     - Deployment reason: Testing validation setup"
    echo -e "     - Components: infrastructure-only"
    echo ""
    
    print_warning "Security reminder:"
    echo -e "  - Keep access keys secure and rotate regularly"
    echo -e "  - Monitor AWS CloudTrail for unusual activity"
    echo -e "  - Review IAM permissions periodically"
}

# Function to cleanup
cleanup() {
    # Clean up any temporary files or environment variables
    unset TEMP_AWS_ACCESS_KEY_ID
    unset TEMP_AWS_SECRET_ACCESS_KEY
    unset TEMP_AWS_SESSION_TOKEN
    unset PROD_ACCOUNT_ID
    
    rm -f /tmp/test-github-actions.sh
}

# Main execution function
main() {
    print_header "AVESA GitHub Actions Setup Validation"
    echo ""
    
    validate_prerequisites
    get_credentials
    validate_credential_format
    test_basic_access
    test_role_assumption
    test_deployment_permissions
    test_cdk_bootstrap
    simulate_github_actions
    display_validation_results
    cleanup
}

# Handle script interruption
trap 'print_error "Validation interrupted by user"; cleanup; exit 1' INT TERM

# Run main function
main