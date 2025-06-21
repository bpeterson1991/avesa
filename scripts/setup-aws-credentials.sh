#!/bin/bash

# AWS Credentials Setup Script for AVESA ClickHouse
# This script helps users configure AWS credentials for the ClickHouse servers

set -e

echo "🔧 AWS CREDENTIALS SETUP FOR AVESA CLICKHOUSE"
echo "=============================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Required profile for AVESA
TARGET_PROFILE="AdministratorAccess-123938354448"
TARGET_REGION="us-east-2"

# Function to check if AWS CLI is installed
check_aws_cli() {
    print_status "Checking AWS CLI installation..."
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed"
        echo ""
        echo "Please install AWS CLI first:"
        echo "  macOS: brew install awscli"
        echo "  Linux: sudo apt-get install awscli"
        echo "  Windows: Download from https://aws.amazon.com/cli/"
        exit 1
    fi
    
    AWS_VERSION=$(aws --version 2>&1)
    print_success "AWS CLI installed: $AWS_VERSION"
}

# Function to check current credentials
check_current_credentials() {
    print_status "Checking current AWS credentials..."
    
    if aws sts get-caller-identity > /dev/null 2>&1; then
        IDENTITY=$(aws sts get-caller-identity 2>/dev/null)
        print_success "Current credentials are working:"
        echo "$IDENTITY" | jq -r '"   Account: " + .Account + "\n   User: " + .Arn' 2>/dev/null || echo "$IDENTITY"
        
        # Check if it's the right account
        ACCOUNT=$(echo "$IDENTITY" | jq -r '.Account' 2>/dev/null || echo "$IDENTITY" | grep -o '"Account": "[^"]*"' | cut -d'"' -f4)
        if [ "$ACCOUNT" = "123938354448" ]; then
            print_success "You're already connected to the correct AWS account!"
            return 0
        else
            print_warning "You're connected to account $ACCOUNT, but need account 123938354448"
            return 1
        fi
    else
        print_warning "No working AWS credentials found"
        return 1
    fi
}

# Function to list available profiles
list_profiles() {
    print_status "Available AWS profiles:"
    PROFILES=$(aws configure list-profiles 2>/dev/null)
    if [ $? -eq 0 ] && [ ! -z "$PROFILES" ]; then
        echo "$PROFILES" | while read profile; do
            if [ "$profile" = "$TARGET_PROFILE" ]; then
                echo -e "   ${GREEN}• $profile${NC} (required)"
            else
                echo "   • $profile"
            fi
        done
        return 0
    else
        print_warning "No AWS profiles configured"
        return 1
    fi
}

# Function to test target profile
test_target_profile() {
    print_status "Testing target profile: $TARGET_PROFILE"
    
    if ! aws configure list-profiles 2>/dev/null | grep -q "^$TARGET_PROFILE$"; then
        print_error "Required profile '$TARGET_PROFILE' not found"
        return 1
    fi
    
    if AWS_PROFILE=$TARGET_PROFILE aws sts get-caller-identity > /dev/null 2>&1; then
        IDENTITY=$(AWS_PROFILE=$TARGET_PROFILE aws sts get-caller-identity 2>/dev/null)
        print_success "Target profile works:"
        echo "$IDENTITY" | jq -r '"   Account: " + .Account + "\n   User: " + .Arn' 2>/dev/null || echo "$IDENTITY"
        return 0
    else
        print_error "Target profile exists but credentials are not working"
        return 1
    fi
}

# Function to configure AWS profile
configure_profile() {
    echo ""
    print_status "Setting up AWS profile: $TARGET_PROFILE"
    echo ""
    echo "Choose your setup method:"
    echo "  1) AWS SSO (Single Sign-On) - Recommended"
    echo "  2) Access Key & Secret Key"
    echo "  3) Skip profile setup (use environment variables)"
    echo ""
    read -p "Enter your choice (1-3): " choice
    
    case $choice in
        1)
            setup_sso
            ;;
        2)
            setup_access_keys
            ;;
        3)
            setup_env_vars
            ;;
        *)
            print_error "Invalid choice"
            configure_profile
            ;;
    esac
}

# Function to setup AWS SSO
setup_sso() {
    print_status "Setting up AWS SSO..."
    echo ""
    echo "You'll need:"
    echo "  • SSO start URL (e.g., https://your-org.awsapps.com/start)"
    echo "  • SSO region (usually us-east-1)"
    echo "  • Account ID: 123938354448"
    echo "  • Role name: AdministratorAccess"
    echo ""
    
    aws configure sso --profile $TARGET_PROFILE
    
    if [ $? -eq 0 ]; then
        print_success "SSO profile configured"
        print_status "Logging in to SSO..."
        aws sso login --profile $TARGET_PROFILE
    else
        print_error "SSO configuration failed"
        return 1
    fi
}

# Function to setup access keys
setup_access_keys() {
    print_status "Setting up access keys..."
    echo ""
    print_warning "Note: SSO is more secure than long-term access keys"
    echo ""
    
    aws configure --profile $TARGET_PROFILE
    
    # Set the region if not set
    aws configure set region $TARGET_REGION --profile $TARGET_PROFILE
}

# Function to setup environment variables
setup_env_vars() {
    print_status "Setting up environment variables..."
    echo ""
    echo "You can set AWS credentials using environment variables:"
    echo ""
    echo "export AWS_ACCESS_KEY_ID=your_access_key"
    echo "export AWS_SECRET_ACCESS_KEY=your_secret_key"
    echo "export AWS_SESSION_TOKEN=your_session_token  # If using temporary credentials"
    echo "export AWS_REGION=$TARGET_REGION"
    echo ""
    echo "Or simply set the profile:"
    echo "export AWS_PROFILE=$TARGET_PROFILE"
    echo ""
    read -p "Press Enter to continue..."
}

# Function to set environment variables
set_environment() {
    print_status "Setting AWS_PROFILE environment variable..."
    
    export AWS_PROFILE=$TARGET_PROFILE
    print_success "AWS_PROFILE set to: $AWS_PROFILE"
    
    # Add to shell profile for persistence
    SHELL_PROFILE=""
    if [ -f ~/.zshrc ]; then
        SHELL_PROFILE=~/.zshrc
    elif [ -f ~/.bashrc ]; then
        SHELL_PROFILE=~/.bashrc
    elif [ -f ~/.bash_profile ]; then
        SHELL_PROFILE=~/.bash_profile
    fi
    
    if [ ! -z "$SHELL_PROFILE" ]; then
        echo ""
        read -p "Add AWS_PROFILE to $SHELL_PROFILE for persistence? (y/n): " add_to_profile
        if [ "$add_to_profile" = "y" ] || [ "$add_to_profile" = "Y" ]; then
            if ! grep -q "export AWS_PROFILE=$TARGET_PROFILE" "$SHELL_PROFILE"; then
                echo "export AWS_PROFILE=$TARGET_PROFILE" >> "$SHELL_PROFILE"
                print_success "Added AWS_PROFILE to $SHELL_PROFILE"
                print_status "Run 'source $SHELL_PROFILE' or restart your terminal to apply"
            else
                print_status "AWS_PROFILE already exists in $SHELL_PROFILE"
            fi
        fi
    fi
}

# Function to test final setup
test_final_setup() {
    print_status "Testing final AWS setup..."
    
    if aws sts get-caller-identity > /dev/null 2>&1; then
        IDENTITY=$(aws sts get-caller-identity 2>/dev/null)
        print_success "AWS credentials are working!"
        echo "$IDENTITY" | jq -r '"   Account: " + .Account + "\n   User: " + .Arn' 2>/dev/null || echo "$IDENTITY"
        
        # Test Secrets Manager access
        print_status "Testing Secrets Manager access..."
        SECRET_NAME="arn:aws:secretsmanager:us-east-2:123938354448:secret:clickhouse-connection-dev-V9zSgO"
        if aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --region us-east-2 > /dev/null 2>&1; then
            print_success "Secrets Manager access confirmed"
        else
            print_warning "Cannot access Secrets Manager (this might be a permissions issue)"
        fi
        
        return 0
    else
        print_error "AWS credentials are still not working"
        return 1
    fi
}

# Function to show next steps
show_next_steps() {
    echo ""
    print_success "🎉 AWS CREDENTIALS SETUP COMPLETE!"
    echo ""
    echo "Next steps:"
    echo "  1. Start the ClickHouse servers:"
    echo "     ./scripts/start-real-clickhouse-servers.sh"
    echo ""
    echo "  2. Or run the diagnostic script to verify:"
    echo "     ./scripts/diagnose-aws-credentials.sh"
    echo ""
    echo "  3. If you have issues, check the setup guide:"
    echo "     docs/AWS_CREDENTIALS_SETUP_GUIDE.md"
    echo ""
}

# Main execution flow
main() {
    check_aws_cli
    echo ""
    
    if check_current_credentials; then
        echo ""
        set_environment
        test_final_setup && show_next_steps
        exit 0
    fi
    
    echo ""
    list_profiles
    echo ""
    
    if test_target_profile; then
        set_environment
        test_final_setup && show_next_steps
        exit 0
    fi
    
    # Need to configure profile
    configure_profile
    
    # Test the setup
    if test_target_profile; then
        set_environment
        test_final_setup && show_next_steps
    else
        print_error "Setup failed. Please check your configuration and try again."
        echo ""
        echo "For help, see: docs/AWS_CREDENTIALS_SETUP_GUIDE.md"
        exit 1
    fi
}

# Run main function
main "$@"