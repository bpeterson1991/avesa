#!/bin/bash

# AVESA Combined GitHub Actions AWS Setup Script
# This script orchestrates the complete setup of cross-account IAM roles and users
# for GitHub Actions production deployment

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default values
PROD_ACCOUNT_ID=""
DEV_ACCOUNT_ID=""
EXTERNAL_ID="avesa-github-actions-2024"
PROD_PROFILE=""
DEV_PROFILE=""
REGION="us-east-2"
SKIP_VALIDATION=false
INTERACTIVE=true

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

# Function to show usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --prod-account-id ACCOUNT_ID     Production AWS account ID"
    echo "  --dev-account-id ACCOUNT_ID      Development AWS account ID"
    echo "  --external-id EXTERNAL_ID        External ID for additional security [default: avesa-github-actions-2024]"
    echo "  --prod-profile PROFILE           AWS profile for production account"
    echo "  --dev-profile PROFILE            AWS profile for development account"
    echo "  --region REGION                  AWS region [default: us-east-2]"
    echo "  --non-interactive                Run in non-interactive mode (requires all account IDs)"
    echo "  --skip-validation                Skip validation steps"
    echo "  -h, --help                       Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                                    # Interactive mode"
    echo "  $0 --prod-account-id 123456789012 --dev-account-id 987654321098"
    echo "  $0 --non-interactive --prod-account-id 123456789012 --dev-account-id 987654321098"
    echo ""
    echo "Prerequisites:"
    echo "  - AWS CLI installed and configured"
    echo "  - jq installed for JSON processing"
    echo "  - Access to both production and development AWS accounts"
    echo "  - Permissions to create IAM roles, users, and policies"
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
        --prod-profile)
            PROD_PROFILE="$2"
            shift 2
            ;;
        --dev-profile)
            DEV_PROFILE="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            shift 2
            ;;
        --non-interactive)
            INTERACTIVE=false
            shift
            ;;
        --skip-validation)
            SKIP_VALIDATION=true
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
    
    # Check if setup scripts exist
    if [[ ! -f "scripts/setup-production-iam.sh" ]]; then
        print_error "Production setup script not found: scripts/setup-production-iam.sh"
        exit 1
    fi
    print_status "✓ Production setup script found"
    
    if [[ ! -f "scripts/setup-development-iam.sh" ]]; then
        print_error "Development setup script not found: scripts/setup-development-iam.sh"
        exit 1
    fi
    print_status "✓ Development setup script found"
    
    # Make scripts executable
    chmod +x scripts/setup-production-iam.sh
    chmod +x scripts/setup-development-iam.sh
    print_status "✓ Scripts made executable"
    
    print_status "✓ Prerequisites validated"
}

# Function to get account information interactively
get_account_info() {
    if [[ "$INTERACTIVE" == "true" ]]; then
        print_header "AVESA GitHub Actions AWS Setup"
        echo ""
        echo "This script will set up cross-account IAM roles and users for GitHub Actions"
        echo "to deploy to your production AWS account from your development account."
        echo ""
        
        # Get production account ID
        if [[ -z "$PROD_ACCOUNT_ID" ]]; then
            echo -e "${YELLOW}Enter your production AWS account ID (12 digits):${NC}"
            read -p "> " PROD_ACCOUNT_ID
            
            # Validate format
            if [[ ! "$PROD_ACCOUNT_ID" =~ ^[0-9]{12}$ ]]; then
                print_error "Invalid account ID format. Must be 12 digits."
                exit 1
            fi
        fi
        
        # Get development account ID
        if [[ -z "$DEV_ACCOUNT_ID" ]]; then
            echo -e "${YELLOW}Enter your development AWS account ID (12 digits):${NC}"
            read -p "> " DEV_ACCOUNT_ID
            
            # Validate format
            if [[ ! "$DEV_ACCOUNT_ID" =~ ^[0-9]{12}$ ]]; then
                print_error "Invalid account ID format. Must be 12 digits."
                exit 1
            fi
        fi
        
        # Get AWS profiles
        if [[ -z "$PROD_PROFILE" ]]; then
            echo -e "${YELLOW}Enter AWS profile for production account (or press Enter for default):${NC}"
            read -p "> " PROD_PROFILE
        fi
        
        if [[ -z "$DEV_PROFILE" ]]; then
            echo -e "${YELLOW}Enter AWS profile for development account (or press Enter for default):${NC}"
            read -p "> " DEV_PROFILE
        fi
        
        # Confirm external ID
        echo -e "${YELLOW}External ID for additional security [${EXTERNAL_ID}]:${NC}"
        read -p "> " INPUT_EXTERNAL_ID
        if [[ -n "$INPUT_EXTERNAL_ID" ]]; then
            EXTERNAL_ID="$INPUT_EXTERNAL_ID"
        fi
        
        # Display configuration summary
        echo ""
        print_header "Configuration Summary"
        echo -e "Production Account ID: ${PROD_ACCOUNT_ID}"
        echo -e "Development Account ID: ${DEV_ACCOUNT_ID}"
        echo -e "External ID: ${EXTERNAL_ID}"
        echo -e "Production Profile: ${PROD_PROFILE:-default}"
        echo -e "Development Profile: ${DEV_PROFILE:-default}"
        echo -e "Region: ${REGION}"
        echo ""
        
        read -p "Continue with this configuration? (Y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Nn]$ ]]; then
            print_status "Setup cancelled by user"
            exit 0
        fi
    else
        # Non-interactive mode - validate required parameters
        if [[ -z "$PROD_ACCOUNT_ID" ]] || [[ -z "$DEV_ACCOUNT_ID" ]]; then
            print_error "In non-interactive mode, both --prod-account-id and --dev-account-id are required"
            usage
            exit 1
        fi
    fi
}

# Function to validate AWS access
validate_aws_access() {
    if [[ "$SKIP_VALIDATION" == "true" ]]; then
        print_warning "Skipping AWS access validation"
        return
    fi
    
    print_step "Validating AWS access..."
    
    # Test production account access
    print_status "Testing production account access..."
    if [[ -n "$PROD_PROFILE" ]]; then
        if ! AWS_PROFILE="$PROD_PROFILE" aws sts get-caller-identity --region "$REGION" > /dev/null 2>&1; then
            print_error "Cannot access production account with profile: $PROD_PROFILE"
            print_error "Please configure the profile or check your credentials"
            exit 1
        fi
        
        CURRENT_PROD_ACCOUNT=$(AWS_PROFILE="$PROD_PROFILE" aws sts get-caller-identity --query Account --output text --region "$REGION")
        if [[ "$CURRENT_PROD_ACCOUNT" != "$PROD_ACCOUNT_ID" ]]; then
            print_error "Production profile points to account $CURRENT_PROD_ACCOUNT, expected $PROD_ACCOUNT_ID"
            exit 1
        fi
        print_status "✓ Production account access verified"
    else
        print_warning "No production profile specified, skipping production account validation"
    fi
    
    # Test development account access
    print_status "Testing development account access..."
    if [[ -n "$DEV_PROFILE" ]]; then
        if ! AWS_PROFILE="$DEV_PROFILE" aws sts get-caller-identity --region "$REGION" > /dev/null 2>&1; then
            print_error "Cannot access development account with profile: $DEV_PROFILE"
            print_error "Please configure the profile or check your credentials"
            exit 1
        fi
        
        CURRENT_DEV_ACCOUNT=$(AWS_PROFILE="$DEV_PROFILE" aws sts get-caller-identity --query Account --output text --region "$REGION")
        if [[ "$CURRENT_DEV_ACCOUNT" != "$DEV_ACCOUNT_ID" ]]; then
            print_error "Development profile points to account $CURRENT_DEV_ACCOUNT, expected $DEV_ACCOUNT_ID"
            exit 1
        fi
        print_status "✓ Development account access verified"
    else
        print_warning "No development profile specified, skipping development account validation"
    fi
}

# Function to setup production account
setup_production_account() {
    print_step "Setting up production account IAM role..."
    
    # Build command arguments
    PROD_ARGS="--prod-account-id $PROD_ACCOUNT_ID --dev-account-id $DEV_ACCOUNT_ID --external-id $EXTERNAL_ID --region $REGION"
    
    if [[ -n "$PROD_PROFILE" ]]; then
        PROD_ARGS="$PROD_ARGS --profile $PROD_PROFILE"
    fi
    
    print_status "Running: ./scripts/setup-production-iam.sh $PROD_ARGS"
    
    if ./scripts/setup-production-iam.sh $PROD_ARGS; then
        print_status "✓ Production account setup completed successfully"
    else
        print_error "❌ Production account setup failed"
        exit 1
    fi
}

# Function to setup development account
setup_development_account() {
    print_step "Setting up development account IAM user..."
    
    # Build command arguments
    DEV_ARGS="--prod-account-id $PROD_ACCOUNT_ID --dev-account-id $DEV_ACCOUNT_ID --external-id $EXTERNAL_ID --region $REGION"
    
    if [[ -n "$DEV_PROFILE" ]]; then
        DEV_ARGS="$DEV_ARGS --profile $DEV_PROFILE"
    fi
    
    print_status "Running: ./scripts/setup-development-iam.sh $DEV_ARGS"
    
    if ./scripts/setup-development-iam.sh $DEV_ARGS; then
        print_status "✓ Development account setup completed successfully"
    else
        print_error "❌ Development account setup failed"
        exit 1
    fi
}

# Function to collect GitHub secrets
collect_github_secrets() {
    print_step "Collecting GitHub secrets configuration..."
    
    # Check if secrets file was created by development setup
    if [[ -f "/tmp/github-secrets.txt" ]]; then
        print_status "GitHub secrets found in temporary file"
        cp /tmp/github-secrets.txt /tmp/final-github-secrets.txt
    else
        print_warning "No temporary secrets file found, creating manual configuration"
        
        cat > /tmp/final-github-secrets.txt << EOF
# GitHub Secrets Configuration
# Add these secrets to your GitHub repository:

AWS_ACCESS_KEY_ID_PROD=<ACCESS_KEY_FROM_DEVELOPMENT_SETUP>
AWS_SECRET_ACCESS_KEY_PROD=<SECRET_KEY_FROM_DEVELOPMENT_SETUP>
AWS_PROD_DEPLOYMENT_ROLE_ARN=arn:aws:iam::${PROD_ACCOUNT_ID}:role/GitHubActionsDeploymentRole
EOF
    fi
}

# Function to display final results
display_final_results() {
    echo ""
    print_header "🎉 GitHub Actions AWS Setup Complete!"
    echo ""
    
    print_status "Setup Summary:"
    echo -e "  ✓ Production deployment role created in account: ${PROD_ACCOUNT_ID}"
    echo -e "  ✓ Development IAM user created in account: ${DEV_ACCOUNT_ID}"
    echo -e "  ✓ Cross-account trust relationship established"
    echo -e "  ✓ GitHub secrets configuration prepared"
    echo ""
    
    print_header "GitHub Repository Configuration"
    echo ""
    print_status "Add these secrets to your GitHub repository:"
    echo -e "${YELLOW}Repository Settings → Secrets and variables → Actions → New repository secret${NC}"
    echo ""
    
    if [[ -f "/tmp/final-github-secrets.txt" ]]; then
        cat /tmp/final-github-secrets.txt | grep -E "^AWS_" | while read line; do
            SECRET_NAME=$(echo "$line" | cut -d'=' -f1)
            SECRET_VALUE=$(echo "$line" | cut -d'=' -f2)
            echo -e "${BLUE}Secret Name:${NC} $SECRET_NAME"
            echo -e "${BLUE}Secret Value:${NC} $SECRET_VALUE"
            echo ""
        done
    fi
    
    print_header "Next Steps"
    echo ""
    print_status "1. Add GitHub Secrets:"
    echo -e "   - Go to your GitHub repository"
    echo -e "   - Navigate to Settings → Secrets and variables → Actions"
    echo -e "   - Add the three secrets shown above"
    echo ""
    print_status "2. Test the Setup:"
    echo -e "   - Run the validation script: ./scripts/validate-github-actions-setup.sh"
    echo -e "   - Or trigger a GitHub Actions workflow manually"
    echo ""
    print_status "3. Deploy to Production:"
    echo -e "   - Use the GitHub Actions workflow: Deploy to Production"
    echo -e "   - Or run locally: ./scripts/deploy.sh --environment prod"
    echo ""
    
    print_header "Security Recommendations"
    echo ""
    print_warning "Important Security Notes:"
    echo -e "  🔒 Store access keys only in GitHub secrets"
    echo -e "  🔄 Rotate access keys every 90 days"
    echo -e "  📊 Monitor CloudTrail for unusual activity"
    echo -e "  🗑️  Delete temporary files after copying secrets"
    echo -e "  👥 Limit repository access to authorized personnel"
    echo ""
    
    print_status "Temporary Files Created:"
    echo -e "  - /tmp/final-github-secrets.txt (contains sensitive data)"
    echo -e "  - /tmp/rollback-production-iam.sh"
    echo -e "  - /tmp/rollback-development-iam.sh"
    echo ""
    print_warning "Remember to delete sensitive temporary files after use!"
    
    echo ""
    print_header "Support"
    echo ""
    print_status "For additional help:"
    echo -e "  - Validation: ./scripts/validate-github-actions-setup.sh"
    echo -e "  - Documentation: docs/AWS_CREDENTIALS_SETUP_GUIDE.md"
    echo -e "  - Quick Setup: docs/GITHUB_SECRETS_QUICK_SETUP.md"
}

# Function to cleanup and exit
cleanup_and_exit() {
    print_step "Cleaning up..."
    
    # Ask user about temporary files
    if [[ "$INTERACTIVE" == "true" ]]; then
        echo ""
        read -p "Delete temporary secrets file? (Y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]] || [[ -z $REPLY ]]; then
            rm -f /tmp/final-github-secrets.txt /tmp/github-secrets.txt
            print_status "✓ Temporary secrets files deleted"
        else
            print_warning "Temporary secrets files kept - remember to delete manually"
        fi
    else
        print_warning "Non-interactive mode: temporary files kept at /tmp/"
    fi
    
    print_status "Setup completed successfully!"
}

# Main execution function
main() {
    validate_prerequisites
    get_account_info
    validate_aws_access
    setup_production_account
    setup_development_account
    collect_github_secrets
    display_final_results
    cleanup_and_exit
}

# Handle script interruption
trap 'print_error "Setup interrupted by user"; exit 1' INT TERM

# Run main function
main