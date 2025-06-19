#!/bin/bash

# AVESA Development Account IAM Setup Script
# This script creates the github-actions-deployer IAM user in the development account
# and configures it to assume the production deployment role

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
    echo "  --prod-account-id ACCOUNT_ID     Production AWS account ID [REQUIRED]"
    echo "  --dev-account-id ACCOUNT_ID      Development AWS account ID [REQUIRED]"
    echo "  --external-id EXTERNAL_ID        External ID for additional security [default: avesa-github-actions-2024]"
    echo "  --user-name USER_NAME            Name of the GitHub Actions user [default: github-actions-deployer]"
    echo "  --prod-role-name ROLE_NAME       Name of the production deployment role [default: GitHubActionsDeploymentRole]"
    echo "  --profile PROFILE                AWS profile to use for development account"
    echo "  --region REGION                  AWS region [default: us-east-2]"
    echo "  -h, --help                       Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --prod-account-id 123456789012 --dev-account-id 987654321098"
    echo "  $0 --prod-account-id 123456789012 --dev-account-id 987654321098 --profile my-dev-profile"
    echo ""
    echo "Prerequisites:"
    echo "  - AWS CLI installed and configured"
    echo "  - Access to development AWS account"
    echo "  - Permissions to create IAM users and policies"
    echo "  - Production deployment role already created"
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
        --user-name)
            USER_NAME="$2"
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
print_header "AVESA Development IAM Setup"
echo -e "Production Account ID: ${PROD_ACCOUNT_ID}"
echo -e "Development Account ID: ${DEV_ACCOUNT_ID}"
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

# Function to create assume role policy document
create_assume_role_policy() {
    cat > /tmp/assume-role-policy.json << EOF
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

# Function to create the GitHub Actions user
create_github_actions_user() {
    print_status "Creating GitHub Actions IAM user..."
    
    # Check if user already exists
    if aws iam get-user --user-name "$USER_NAME" --region "$REGION" > /dev/null 2>&1; then
        print_warning "User $USER_NAME already exists. Skipping user creation..."
    else
        # Create the user
        aws iam create-user \
            --user-name "$USER_NAME" \
            --path "/github-actions/" \
            --tags Key=Purpose,Value=GitHubActions Key=Environment,Value=AVESA \
            --region "$REGION"
        print_status "✓ User $USER_NAME created"
    fi
}

# Function to create and attach assume role policy
create_and_attach_policy() {
    print_status "Creating assume role policy..."
    
    # Create assume role policy
    create_assume_role_policy
    
    # Check if policy already exists
    POLICY_ARN="arn:aws:iam::${DEV_ACCOUNT_ID}:policy/${POLICY_NAME}"
    if aws iam get-policy --policy-arn "$POLICY_ARN" --region "$REGION" > /dev/null 2>&1; then
        print_warning "Policy $POLICY_NAME already exists. Creating new version..."
        
        # Create new policy version
        aws iam create-policy-version \
            --policy-arn "$POLICY_ARN" \
            --policy-document file:///tmp/assume-role-policy.json \
            --set-as-default \
            --region "$REGION" > /dev/null
        print_status "✓ Policy $POLICY_NAME updated with new version"
    else
        # Create the policy
        aws iam create-policy \
            --policy-name "$POLICY_NAME" \
            --policy-document file:///tmp/assume-role-policy.json \
            --description "AVESA policy to assume production deployment role" \
            --path "/github-actions/" \
            --tags Key=Purpose,Value=GitHubActions Key=Environment,Value=AVESA \
            --region "$REGION" > /dev/null
        print_status "✓ Policy $POLICY_NAME created"
    fi
    
    # Attach policy to user
    aws iam attach-user-policy \
        --user-name "$USER_NAME" \
        --policy-arn "$POLICY_ARN" \
        --region "$REGION"
    print_status "✓ Policy attached to user"
    
    # Clean up temporary file
    rm -f /tmp/assume-role-policy.json
}

# Function to create access keys
create_access_keys() {
    print_status "Creating access keys..."
    
    # Check if user already has access keys
    EXISTING_KEYS=$(aws iam list-access-keys --user-name "$USER_NAME" --query 'AccessKeyMetadata[].AccessKeyId' --output text --region "$REGION")
    
    if [[ -n "$EXISTING_KEYS" ]]; then
        print_warning "User $USER_NAME already has access keys:"
        echo "$EXISTING_KEYS"
        echo ""
        read -p "Do you want to create new access keys? This will require manual cleanup of old keys. (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_status "Skipping access key creation. Using existing keys."
            return
        fi
    fi
    
    # Create new access keys
    ACCESS_KEY_OUTPUT=$(aws iam create-access-key --user-name "$USER_NAME" --region "$REGION")
    
    # Extract access key details
    ACCESS_KEY_ID=$(echo "$ACCESS_KEY_OUTPUT" | jq -r '.AccessKey.AccessKeyId')
    SECRET_ACCESS_KEY=$(echo "$ACCESS_KEY_OUTPUT" | jq -r '.AccessKey.SecretAccessKey')
    
    # Store keys in temporary file for display
    cat > /tmp/github-secrets.txt << EOF
# GitHub Secrets Configuration
# Add these secrets to your GitHub repository:

AWS_ACCESS_KEY_ID_PROD=${ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY_PROD=${SECRET_ACCESS_KEY}
AWS_PROD_DEPLOYMENT_ROLE_ARN=arn:aws:iam::${PROD_ACCOUNT_ID}:role/${PROD_ROLE_NAME}
EOF
    
    print_status "✓ Access keys created successfully"
}

# Function to test role assumption
test_role_assumption() {
    print_status "Testing role assumption..."
    
    # Read the access keys from the temporary file if it exists
    if [[ -f "/tmp/github-secrets.txt" ]]; then
        ACCESS_KEY_ID=$(grep "AWS_ACCESS_KEY_ID_PROD" /tmp/github-secrets.txt | cut -d'=' -f2)
        SECRET_ACCESS_KEY=$(grep "AWS_SECRET_ACCESS_KEY_PROD" /tmp/github-secrets.txt | cut -d'=' -f2)
        
        # Test role assumption using the new credentials
        ROLE_ARN="arn:aws:iam::${PROD_ACCOUNT_ID}:role/${PROD_ROLE_NAME}"
        
        print_status "Attempting to assume role: $ROLE_ARN"
        
        # Use temporary credentials to test role assumption
        if AWS_ACCESS_KEY_ID="$ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$SECRET_ACCESS_KEY" \
           aws sts assume-role \
           --role-arn "$ROLE_ARN" \
           --role-session-name "test-session" \
           --external-id "$EXTERNAL_ID" \
           --region "$REGION" > /dev/null 2>&1; then
            print_status "✓ Role assumption test successful"
        else
            print_warning "Role assumption test failed. This may be expected if:"
            print_warning "  - Production role doesn't exist yet"
            print_warning "  - Trust policy not configured correctly"
            print_warning "  - External ID mismatch"
            print_warning "Please run the production setup script first if not done already"
        fi
    else
        print_warning "No new access keys created, skipping role assumption test"
    fi
}

# Function to display setup results
display_results() {
    echo ""
    print_header "Development IAM Setup Complete!"
    echo ""
    
    if [[ -f "/tmp/github-secrets.txt" ]]; then
        print_status "GitHub Secrets Configuration:"
        echo ""
        echo -e "${YELLOW}Add these secrets to your GitHub repository:${NC}"
        echo ""
        cat /tmp/github-secrets.txt | grep -E "^AWS_" | while read line; do
            SECRET_NAME=$(echo "$line" | cut -d'=' -f1)
            SECRET_VALUE=$(echo "$line" | cut -d'=' -f2)
            echo -e "${BLUE}Secret Name:${NC} $SECRET_NAME"
            echo -e "${BLUE}Secret Value:${NC} $SECRET_VALUE"
            echo ""
        done
        
        print_warning "IMPORTANT: Save these credentials securely!"
        print_warning "The secret access key will not be shown again."
        echo ""
        print_status "Credentials saved to: /tmp/github-secrets.txt"
        print_warning "Delete this file after copying the secrets to GitHub"
    fi
    
    print_status "User Details:"
    echo -e "  User Name: ${USER_NAME}"
    echo -e "  User ARN: arn:aws:iam::${DEV_ACCOUNT_ID}:user/github-actions/${USER_NAME}"
    echo -e "  Policy Name: ${POLICY_NAME}"
    echo -e "  Production Role: arn:aws:iam::${PROD_ACCOUNT_ID}:role/${PROD_ROLE_NAME}"
    echo ""
    print_status "Next Steps:"
    echo -e "  1. Add the GitHub secrets shown above to your repository"
    echo -e "  2. Test the GitHub Actions workflow"
    echo -e "  3. Run the validation script:"
    echo -e "     ./scripts/validate-github-actions-setup.sh"
    echo ""
    print_warning "Security Notes:"
    echo -e "  - Store access keys securely in GitHub secrets only"
    echo -e "  - Rotate access keys regularly (every 90 days)"
    echo -e "  - Monitor CloudTrail for unusual activity"
    echo -e "  - Delete /tmp/github-secrets.txt after use"
}

# Function to create rollback instructions
create_rollback_instructions() {
    cat > /tmp/rollback-development-iam.sh << 'EOF'
#!/bin/bash
# Rollback script for development IAM setup

set -e

USER_NAME="github-actions-deployer"
POLICY_NAME="AVESAAssumeProductionRole"
DEV_ACCOUNT_ID="REPLACE_WITH_DEV_ACCOUNT_ID"

echo "Rolling back development IAM setup..."

# List and delete access keys
ACCESS_KEYS=$(aws iam list-access-keys --user-name "$USER_NAME" --query 'AccessKeyMetadata[].AccessKeyId' --output text)
for key in $ACCESS_KEYS; do
    aws iam delete-access-key --user-name "$USER_NAME" --access-key-id "$key"
    echo "Deleted access key: $key"
done

# Detach policy from user
aws iam detach-user-policy --user-name "$USER_NAME" --policy-arn "arn:aws:iam::${DEV_ACCOUNT_ID}:policy/${POLICY_NAME}"

# Delete policy
aws iam delete-policy --policy-arn "arn:aws:iam::${DEV_ACCOUNT_ID}:policy/${POLICY_NAME}"

# Delete user
aws iam delete-user --user-name "$USER_NAME"

echo "Rollback complete!"
EOF

    # Replace placeholder with actual account ID
    sed -i.bak "s/REPLACE_WITH_DEV_ACCOUNT_ID/${DEV_ACCOUNT_ID}/g" /tmp/rollback-development-iam.sh
    rm -f /tmp/rollback-development-iam.sh.bak
    
    chmod +x /tmp/rollback-development-iam.sh
    
    echo ""
    print_status "Rollback script created at: /tmp/rollback-development-iam.sh"
    print_warning "Save this script if you need to rollback the changes"
}

# Function to cleanup temporary files
cleanup() {
    print_status "Cleaning up temporary files..."
    rm -f /tmp/assume-role-policy.json
    
    # Ask user if they want to keep the secrets file
    if [[ -f "/tmp/github-secrets.txt" ]]; then
        echo ""
        read -p "Delete the temporary secrets file /tmp/github-secrets.txt? (Y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]] || [[ -z $REPLY ]]; then
            rm -f /tmp/github-secrets.txt
            print_status "✓ Temporary secrets file deleted"
        else
            print_warning "Temporary secrets file kept at: /tmp/github-secrets.txt"
            print_warning "Remember to delete it manually after copying secrets to GitHub"
        fi
    fi
}

# Main execution function
main() {
    validate_prerequisites
    create_github_actions_user
    create_and_attach_policy
    create_access_keys
    test_role_assumption
    create_rollback_instructions
    display_results
    cleanup
}

# Run main function
main