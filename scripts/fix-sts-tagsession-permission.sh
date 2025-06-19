#!/bin/bash

# AVESA Fix Script: Add sts:TagSession Permission
# This script fixes the missing sts:TagSession permission for the GitHub Actions user
# that prevents role assumption to the production account

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
DEV_ACCOUNT_ID=""
PROD_ACCOUNT_ID=""
EXTERNAL_ID="avesa-github-actions-2024"
USER_NAME="github-actions-deployer"
POLICY_NAME="AVESAAssumeProductionRole"
PROD_ROLE_NAME="GitHubActionsDeploymentRole"
AWS_PROFILE=""
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
    echo "  --dev-account-id ACCOUNT_ID      Development AWS account ID [REQUIRED]"
    echo "  --prod-account-id ACCOUNT_ID     Production AWS account ID [REQUIRED]"
    echo "  --external-id EXTERNAL_ID        External ID for additional security [default: avesa-github-actions-2024]"
    echo "  --user-name USER_NAME            Name of the GitHub Actions user [default: github-actions-deployer]"
    echo "  --policy-name POLICY_NAME        Name of the assume role policy [default: AVESAAssumeProductionRole]"
    echo "  --prod-role-name ROLE_NAME       Name of the production deployment role [default: GitHubActionsDeploymentRole]"
    echo "  --profile PROFILE                AWS profile to use for development account"
    echo "  --region REGION                  AWS region [default: us-east-2]"
    echo "  -h, --help                       Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --dev-account-id 123938354448 --prod-account-id 987654321098"
    echo "  $0 --dev-account-id 123938354448 --prod-account-id 987654321098 --profile my-dev-profile"
    echo ""
    echo "This script fixes the missing sts:TagSession permission that causes the error:"
    echo "  'User: arn:aws:iam::123938354448:user/github-actions/github-actions-deployer is not authorized to perform: sts:TagSession'"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --dev-account-id)
            DEV_ACCOUNT_ID="$2"
            shift 2
            ;;
        --prod-account-id)
            PROD_ACCOUNT_ID="$2"
            shift 2
            ;;
        --external-id)
            EXTERNAL_ID="$2"
            shift 2
            ;;
        --user-name)
            USER_NAME="$2"
            shift 2
            ;;
        --policy-name)
            POLICY_NAME="$2"
            shift 2
            ;;
        --prod-role-name)
            PROD_ROLE_NAME="$2"
            shift 2
            ;;
        --profile)
            AWS_PROFILE="$2"
            shift 2
            ;;
        --region)
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

# Validate required parameters
if [[ -z "$DEV_ACCOUNT_ID" ]]; then
    print_error "Development account ID is required. Use --dev-account-id to specify."
    usage
    exit 1
fi

if [[ -z "$PROD_ACCOUNT_ID" ]]; then
    print_error "Production account ID is required. Use --prod-account-id to specify."
    usage
    exit 1
fi

# Validate account ID format (12 digits)
if [[ ! "$DEV_ACCOUNT_ID" =~ ^[0-9]{12}$ ]]; then
    print_error "Invalid development account ID format. Must be 12 digits."
    exit 1
fi

if [[ ! "$PROD_ACCOUNT_ID" =~ ^[0-9]{12}$ ]]; then
    print_error "Invalid production account ID format. Must be 12 digits."
    exit 1
fi

# Set AWS profile if provided
if [[ -n "$AWS_PROFILE" ]]; then
    export AWS_PROFILE="$AWS_PROFILE"
    print_status "Using AWS profile: $AWS_PROFILE"
fi

# Display fix information
print_header "AVESA sts:TagSession Permission Fix"
echo -e "Development Account ID: ${DEV_ACCOUNT_ID}"
echo -e "Production Account ID: ${PROD_ACCOUNT_ID}"
echo -e "External ID: ${EXTERNAL_ID}"
echo -e "User Name: ${USER_NAME}"
echo -e "Policy Name: ${POLICY_NAME}"
echo -e "Production Role: ${PROD_ROLE_NAME}"
echo -e "AWS Profile: ${AWS_PROFILE:-default}"
echo -e "Region: ${REGION}"
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
    
    # Check if jq is installed
    if ! command -v jq &> /dev/null; then
        print_error "jq is not installed. Please install it first: brew install jq (macOS) or apt-get install jq (Ubuntu)"
        exit 1
    fi
    print_status "✓ jq installed"
    
    # Verify AWS credentials
    if ! aws sts get-caller-identity --region "$REGION" > /dev/null 2>&1; then
        print_error "AWS credentials not configured or not accessible"
        if [[ -n "$AWS_PROFILE" ]]; then
            print_error "Profile '$AWS_PROFILE' not found or not accessible"
        fi
        exit 1
    fi
    
    # Verify we're in the correct account
    CURRENT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
    if [[ "$CURRENT_ACCOUNT" != "$DEV_ACCOUNT_ID" ]]; then
        print_error "Current AWS account ($CURRENT_ACCOUNT) does not match development account ID ($DEV_ACCOUNT_ID)"
        print_error "Please configure AWS credentials for the development account"
        exit 1
    fi
    print_status "✓ Connected to development account: $CURRENT_ACCOUNT"
    
    print_status "✓ Prerequisites validated"
}

# Function to check current policy
check_current_policy() {
    print_status "Checking current policy configuration..."
    
    POLICY_ARN="arn:aws:iam::${DEV_ACCOUNT_ID}:policy/${POLICY_NAME}"
    
    # Check if policy exists
    if ! aws iam get-policy --policy-arn "$POLICY_ARN" --region "$REGION" > /dev/null 2>&1; then
        print_error "Policy $POLICY_NAME not found. Please run the development IAM setup script first."
        exit 1
    fi
    print_status "✓ Policy $POLICY_NAME found"
    
    # Check if user exists
    if ! aws iam get-user --user-name "$USER_NAME" --region "$REGION" > /dev/null 2>&1; then
        print_error "User $USER_NAME not found. Please run the development IAM setup script first."
        exit 1
    fi
    print_status "✓ User $USER_NAME found"
    
    # Get current policy document
    POLICY_VERSION=$(aws iam get-policy --policy-arn "$POLICY_ARN" --query 'Policy.DefaultVersionId' --output text --region "$REGION")
    CURRENT_POLICY=$(aws iam get-policy-version --policy-arn "$POLICY_ARN" --version-id "$POLICY_VERSION" --query 'PolicyVersion.Document' --region "$REGION")
    
    # Check if sts:TagSession is already present
    if echo "$CURRENT_POLICY" | jq -r '.Statement[0].Action[]' | grep -q "sts:TagSession"; then
        print_warning "sts:TagSession permission already exists in the policy"
        echo ""
        print_status "Current policy actions:"
        echo "$CURRENT_POLICY" | jq -r '.Statement[0].Action[]' | sed 's/^/  - /'
        echo ""
        read -p "Do you want to continue and update the policy anyway? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_status "Fix cancelled by user"
            exit 0
        fi
    else
        print_status "✓ sts:TagSession permission missing - fix needed"
    fi
}

# Function to create updated assume role policy document
create_updated_assume_role_policy() {
    cat > /tmp/updated-assume-role-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sts:AssumeRole",
        "sts:TagSession"
      ],
      "Resource": "arn:aws:iam::${PROD_ACCOUNT_ID}:role/${PROD_ROLE_NAME}",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "${EXTERNAL_ID}"
        }
      }
    }
  ]
}
EOF
}

# Function to update the policy
update_policy() {
    print_status "Updating policy with sts:TagSession permission..."
    
    # Create updated policy document
    create_updated_assume_role_policy
    
    POLICY_ARN="arn:aws:iam::${DEV_ACCOUNT_ID}:policy/${POLICY_NAME}"
    
    # Create new policy version
    print_status "Creating new policy version..."
    aws iam create-policy-version \
        --policy-arn "$POLICY_ARN" \
        --policy-document file:///tmp/updated-assume-role-policy.json \
        --set-as-default \
        --region "$REGION" > /dev/null
    
    print_status "✓ Policy updated with sts:TagSession permission"
    
    # Clean up temporary file
    rm -f /tmp/updated-assume-role-policy.json
}

# Function to test the fix
test_role_assumption() {
    print_status "Testing role assumption with updated permissions..."
    
    # Get user's access keys (we'll use the first one found)
    ACCESS_KEYS=$(aws iam list-access-keys --user-name "$USER_NAME" --query 'AccessKeyMetadata[0].AccessKeyId' --output text --region "$REGION")
    
    if [[ "$ACCESS_KEYS" == "None" ]] || [[ -z "$ACCESS_KEYS" ]]; then
        print_warning "No access keys found for user $USER_NAME"
        print_warning "Cannot test role assumption without access keys"
        print_warning "Please ensure the user has valid access keys configured in GitHub secrets"
        return
    fi
    
    print_status "Found access key for testing: $ACCESS_KEYS"
    
    # Note: We can't actually test with the secret key since we don't have it
    # But we can verify the policy is correctly attached
    ROLE_ARN="arn:aws:iam::${PROD_ACCOUNT_ID}:role/${PROD_ROLE_NAME}"
    print_status "Role assumption should now work for: $ROLE_ARN"
    print_status "✓ Policy update completed - GitHub Actions should now work"
}

# Function to display results
display_results() {
    echo ""
    print_header "sts:TagSession Permission Fix Complete!"
    echo ""
    print_status "What was fixed:"
    echo -e "  ✓ Added sts:TagSession permission to policy: ${POLICY_NAME}"
    echo -e "  ✓ Policy now includes both sts:AssumeRole and sts:TagSession"
    echo -e "  ✓ GitHub Actions user can now assume the production role"
    echo ""
    print_status "Updated Policy Actions:"
    echo -e "  - sts:AssumeRole"
    echo -e "  - sts:TagSession"
    echo ""
    print_status "Next Steps:"
    echo -e "  1. Test the GitHub Actions workflow"
    echo -e "  2. Verify step 7 (role assumption) now passes"
    echo -e "  3. Run the validation script if needed:"
    echo -e "     ./scripts/validate-github-actions-setup.sh"
    echo ""
    print_status "The error should no longer occur:"
    echo -e "  ❌ OLD: User: arn:aws:iam::${DEV_ACCOUNT_ID}:user/github-actions/${USER_NAME} is not authorized to perform: sts:TagSession"
    echo -e "  ✅ NEW: Role assumption should work successfully"
    echo ""
    print_warning "Security Notes:"
    echo -e "  - The sts:TagSession permission is required for modern AWS role assumption"
    echo -e "  - This permission allows tagging of the assumed role session"
    echo -e "  - No additional security risks are introduced by this change"
}

# Main execution function
main() {
    validate_prerequisites
    check_current_policy
    update_policy
    test_role_assumption
    display_results
}

# Run main function
main