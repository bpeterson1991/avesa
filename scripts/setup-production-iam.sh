#!/bin/bash

# AVESA Production Account IAM Setup Script
# This script creates the GitHubActionsDeploymentRole in the production account
# for cross-account access from the development account

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
PROD_ACCOUNT_ID=""
DEV_ACCOUNT_ID=""
EXTERNAL_ID="avesa-github-actions-2024"
ROLE_NAME="GitHubActionsDeploymentRole"
POLICY_NAME="AVESADeploymentPolicy"
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
    echo "  --prod-account-id ACCOUNT_ID     Production AWS account ID [REQUIRED]"
    echo "  --dev-account-id ACCOUNT_ID      Development AWS account ID [REQUIRED]"
    echo "  --external-id EXTERNAL_ID        External ID for additional security [default: avesa-github-actions-2024]"
    echo "  --role-name ROLE_NAME            Name of the deployment role [default: GitHubActionsDeploymentRole]"
    echo "  --profile PROFILE                AWS profile to use for production account"
    echo "  --region REGION                  AWS region [default: us-east-2]"
    echo "  -h, --help                       Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --prod-account-id 123456789012 --dev-account-id 987654321098"
    echo "  $0 --prod-account-id 123456789012 --dev-account-id 987654321098 --profile avesa-production"
    echo ""
    echo "Prerequisites:"
    echo "  - AWS CLI installed and configured"
    echo "  - Access to production AWS account"
    echo "  - Permissions to create IAM roles and policies"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --prod-account-id)
            PROD_ACCOUNT_ID="$2"
            shift 2
            ;;
        --dev-account-id)
            DEV_ACCOUNT_ID="$2"
            shift 2
            ;;
        --external-id)
            EXTERNAL_ID="$2"
            shift 2
            ;;
        --role-name)
            ROLE_NAME="$2"
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
if [[ -z "$PROD_ACCOUNT_ID" ]]; then
    print_error "Production account ID is required. Use --prod-account-id to specify."
    usage
    exit 1
fi

if [[ -z "$DEV_ACCOUNT_ID" ]]; then
    print_error "Development account ID is required. Use --dev-account-id to specify."
    usage
    exit 1
fi

# Validate account ID format (12 digits)
if [[ ! "$PROD_ACCOUNT_ID" =~ ^[0-9]{12}$ ]]; then
    print_error "Invalid production account ID format. Must be 12 digits."
    exit 1
fi

if [[ ! "$DEV_ACCOUNT_ID" =~ ^[0-9]{12}$ ]]; then
    print_error "Invalid development account ID format. Must be 12 digits."
    exit 1
fi

# Set AWS profile if provided
if [[ -n "$AWS_PROFILE" ]]; then
    export AWS_PROFILE="$AWS_PROFILE"
    print_status "Using AWS profile: $AWS_PROFILE"
fi

# Display setup information
print_header "AVESA Production IAM Setup"
echo -e "Production Account ID: ${PROD_ACCOUNT_ID}"
echo -e "Development Account ID: ${DEV_ACCOUNT_ID}"
echo -e "External ID: ${EXTERNAL_ID}"
echo -e "Role Name: ${ROLE_NAME}"
echo -e "Policy Name: ${POLICY_NAME}"
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
    if [[ "$CURRENT_ACCOUNT" != "$PROD_ACCOUNT_ID" ]]; then
        print_error "Current AWS account ($CURRENT_ACCOUNT) does not match production account ID ($PROD_ACCOUNT_ID)"
        print_error "Please configure AWS credentials for the production account"
        exit 1
    fi
    print_status "✓ Connected to production account: $CURRENT_ACCOUNT"
    
    print_status "✓ Prerequisites validated"
}

# Function to create trust policy document
create_trust_policy() {
    cat > /tmp/trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::${DEV_ACCOUNT_ID}:root"
      },
      "Action": "sts:AssumeRole",
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

# Function to create deployment policy document
create_deployment_policy() {
    cat > /tmp/deployment-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole",
        "iam:DeleteRole",
        "iam:AttachRolePolicy",
        "iam:DetachRolePolicy",
        "iam:PutRolePolicy",
        "iam:DeleteRolePolicy",
        "iam:GetRole",
        "iam:GetRolePolicy",
        "iam:ListRolePolicies",
        "iam:ListAttachedRolePolicies",
        "iam:PassRole",
        "iam:TagRole",
        "iam:UntagRole",
        "iam:CreateInstanceProfile",
        "iam:DeleteInstanceProfile",
        "iam:AddRoleToInstanceProfile",
        "iam:RemoveRoleFromInstanceProfile"
      ],
      "Resource": [
        "arn:aws:iam::${PROD_ACCOUNT_ID}:role/AVESA*",
        "arn:aws:iam::${PROD_ACCOUNT_ID}:instance-profile/AVESA*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "iam:CreatePolicy",
        "iam:DeletePolicy",
        "iam:GetPolicy",
        "iam:GetPolicyVersion",
        "iam:ListPolicyVersions",
        "iam:CreatePolicyVersion",
        "iam:DeletePolicyVersion",
        "iam:SetDefaultPolicyVersion"
      ],
      "Resource": "arn:aws:iam::${PROD_ACCOUNT_ID}:policy/AVESA*"
    }
  ]
}
EOF
}

# Function to create the deployment role
create_deployment_role() {
    print_status "Creating deployment role..."
    
    # Create trust policy
    create_trust_policy
    
    # Check if role already exists
    if aws iam get-role --role-name "$ROLE_NAME" --region "$REGION" > /dev/null 2>&1; then
        print_warning "Role $ROLE_NAME already exists. Updating trust policy..."
        aws iam update-assume-role-policy \
            --role-name "$ROLE_NAME" \
            --policy-document file:///tmp/trust-policy.json \
            --region "$REGION"
        print_status "✓ Trust policy updated"
    else
        # Create the role
        aws iam create-role \
            --role-name "$ROLE_NAME" \
            --assume-role-policy-document file:///tmp/trust-policy.json \
            --description "AVESA GitHub Actions deployment role for cross-account access" \
            --region "$REGION"
        print_status "✓ Role $ROLE_NAME created"
    fi
    
    # Clean up temporary file
    rm -f /tmp/trust-policy.json
}

# Function to attach policies to the role
attach_policies() {
    print_status "Attaching policies to deployment role..."
    
    # Attach PowerUserAccess for broad deployment permissions
    aws iam attach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn arn:aws:iam::aws:policy/PowerUserAccess \
        --region "$REGION"
    print_status "✓ PowerUserAccess policy attached"
    
    # Create and attach custom deployment policy for IAM permissions
    create_deployment_policy
    
    # Check if custom policy already exists
    POLICY_ARN="arn:aws:iam::${PROD_ACCOUNT_ID}:policy/${POLICY_NAME}"
    if aws iam get-policy --policy-arn "$POLICY_ARN" --region "$REGION" > /dev/null 2>&1; then
        print_warning "Policy $POLICY_NAME already exists. Creating new version..."
        
        # Create new policy version
        aws iam create-policy-version \
            --policy-arn "$POLICY_ARN" \
            --policy-document file:///tmp/deployment-policy.json \
            --set-as-default \
            --region "$REGION" > /dev/null
        print_status "✓ Policy $POLICY_NAME updated with new version"
    else
        # Create the custom policy
        aws iam create-policy \
            --policy-name "$POLICY_NAME" \
            --policy-document file:///tmp/deployment-policy.json \
            --description "AVESA deployment policy for additional IAM permissions" \
            --region "$REGION" > /dev/null
        print_status "✓ Custom policy $POLICY_NAME created"
    fi
    
    # Attach the custom policy
    aws iam attach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn "$POLICY_ARN" \
        --region "$REGION"
    print_status "✓ Custom deployment policy attached"
    
    # Clean up temporary file
    rm -f /tmp/deployment-policy.json
}

# Function to test role assumption
test_role_assumption() {
    print_status "Testing role assumption (this will fail if run from production account)..."
    
    # This test will only work if run from the development account
    # We'll just verify the role exists and has the correct trust policy
    ROLE_ARN="arn:aws:iam::${PROD_ACCOUNT_ID}:role/${ROLE_NAME}"
    
    if aws iam get-role --role-name "$ROLE_NAME" --region "$REGION" > /dev/null 2>&1; then
        print_status "✓ Role exists and is accessible"
        
        # Get trust policy and verify it contains the development account
        TRUST_POLICY=$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.AssumeRolePolicyDocument' --output json --region "$REGION")
        if echo "$TRUST_POLICY" | jq -r '.Statement[0].Principal.AWS' | grep -q "$DEV_ACCOUNT_ID"; then
            print_status "✓ Trust policy correctly configured for development account"
        else
            print_warning "Trust policy may not be correctly configured"
        fi
    else
        print_error "❌ Role not found or not accessible"
        exit 1
    fi
}

# Function to display setup results
display_results() {
    echo ""
    print_header "Production IAM Setup Complete!"
    echo ""
    print_status "GitHub Secrets Configuration:"
    echo ""
    echo -e "${YELLOW}Add this to your GitHub repository secrets:${NC}"
    echo ""
    echo -e "${BLUE}Secret Name:${NC} AWS_PROD_DEPLOYMENT_ROLE_ARN"
    echo -e "${BLUE}Secret Value:${NC} arn:aws:iam::${PROD_ACCOUNT_ID}:role/${ROLE_NAME}"
    echo ""
    print_status "Role Details:"
    echo -e "  Role Name: ${ROLE_NAME}"
    echo -e "  Role ARN: arn:aws:iam::${PROD_ACCOUNT_ID}:role/${ROLE_NAME}"
    echo -e "  External ID: ${EXTERNAL_ID}"
    echo -e "  Trusted Account: ${DEV_ACCOUNT_ID}"
    echo ""
    print_status "Next Steps:"
    echo -e "  1. Run the development account setup script:"
    echo -e "     ./scripts/setup-development-iam.sh --prod-account-id ${PROD_ACCOUNT_ID} --dev-account-id ${DEV_ACCOUNT_ID}"
    echo ""
    echo -e "  2. Or run the combined setup script:"
    echo -e "     ./scripts/setup-github-actions-aws.sh"
    echo ""
    print_warning "Security Notes:"
    echo -e "  - External ID '${EXTERNAL_ID}' provides additional security"
    echo -e "  - Role can only be assumed from development account ${DEV_ACCOUNT_ID}"
    echo -e "  - Review attached policies regularly"
    echo -e "  - Consider implementing policy conditions for additional security"
}

# Function to create rollback instructions
create_rollback_instructions() {
    cat > /tmp/rollback-production-iam.sh << 'EOF'
#!/bin/bash
# Rollback script for production IAM setup

set -e

ROLE_NAME="GitHubActionsDeploymentRole"
POLICY_NAME="AVESADeploymentPolicy"
PROD_ACCOUNT_ID="REPLACE_WITH_PROD_ACCOUNT_ID"

echo "Rolling back production IAM setup..."

# Detach policies
aws iam detach-role-policy --role-name "$ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/PowerUserAccess
aws iam detach-role-policy --role-name "$ROLE_NAME" --policy-arn "arn:aws:iam::${PROD_ACCOUNT_ID}:policy/${POLICY_NAME}"

# Delete custom policy
aws iam delete-policy --policy-arn "arn:aws:iam::${PROD_ACCOUNT_ID}:policy/${POLICY_NAME}"

# Delete role
aws iam delete-role --role-name "$ROLE_NAME"

echo "Rollback complete!"
EOF

    # Replace placeholder with actual account ID
    sed -i.bak "s/REPLACE_WITH_PROD_ACCOUNT_ID/${PROD_ACCOUNT_ID}/g" /tmp/rollback-production-iam.sh
    rm -f /tmp/rollback-production-iam.sh.bak
    
    chmod +x /tmp/rollback-production-iam.sh
    
    echo ""
    print_status "Rollback script created at: /tmp/rollback-production-iam.sh"
    print_warning "Save this script if you need to rollback the changes"
}

# Main execution function
main() {
    validate_prerequisites
    create_deployment_role
    attach_policies
    test_role_assumption
    create_rollback_instructions
    display_results
}

# Run main function
main