#!/bin/bash
# Production Account Setup Script
# Run this after creating the new AWS production account

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}AVESA Production Account Setup${NC}"
echo -e "${BLUE}========================================${NC}"

# Function to prompt for input
prompt_for_input() {
    local prompt="$1"
    local var_name="$2"
    local default_value="$3"
    
    if [ -n "$default_value" ]; then
        read -p "$prompt [$default_value]: " input
        if [ -z "$input" ]; then
            input="$default_value"
        fi
    else
        read -p "$prompt: " input
        while [ -z "$input" ]; do
            echo -e "${RED}This field is required.${NC}"
            read -p "$prompt: " input
        done
    fi
    
    eval "$var_name='$input'"
}

# Production account details (configured for Phase 2)
echo -e "${YELLOW}Configuring production account details:${NC}"
echo ""

PROD_ACCOUNT_ID="563583517998"
AWS_REGION="us-east-2"
IAM_USER="bpeterson"

echo -e "Production Account ID: ${PROD_ACCOUNT_ID}"
echo -e "AWS Region: ${AWS_REGION}"
echo -e "IAM User: ${IAM_USER}"
echo ""

# Check if AWS CLI is already configured with credentials
echo -e "${YELLOW}Checking existing AWS credentials...${NC}"
if aws sts get-caller-identity > /dev/null 2>&1; then
    CURRENT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
    CURRENT_USER=$(aws sts get-caller-identity --query Arn --output text)
    echo -e "${GREEN}✓ Current AWS credentials found${NC}"
    echo -e "  Current Account: ${CURRENT_ACCOUNT}"
    echo -e "  Current User: ${CURRENT_USER}"
    
    # Check if using SSO
    if echo "$CURRENT_USER" | grep -q "assumed-role.*SSO"; then
        echo -e "${YELLOW}Detected SSO-based credentials${NC}"
        echo -e "${YELLOW}Setting up production profile for SSO access...${NC}"
        
        # For SSO, we'll configure the profile to use the same SSO session but target the production account
        # This assumes the user has access to both accounts through the same SSO setup
        echo -e "${BLUE}Note: You'll need to ensure your SSO session has access to account ${PROD_ACCOUNT_ID}${NC}"
        
        # Configure basic profile settings (SSO configuration will be handled separately)
        aws configure set region "$AWS_REGION" --profile avesa-production
        aws configure set output json --profile avesa-production
        
        echo -e "${GREEN}✓ Basic profile configuration completed${NC}"
        echo -e "${YELLOW}For SSO access, you may need to run:${NC}"
        echo -e "  aws sso login --profile avesa-production"
        echo -e "${YELLOW}Or configure SSO settings manually in ~/.aws/config${NC}"
        
        # Skip credential-based setup for SSO
        SKIP_CREDENTIAL_TEST=true
        
    else
        # Use existing credentials for production profile since same IAM user exists in both accounts
        PROD_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
        PROD_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
        
        if [ -z "$PROD_ACCESS_KEY_ID" ] || [ -z "$PROD_SECRET_ACCESS_KEY" ]; then
            echo -e "${RED}❌ Could not retrieve existing credentials${NC}"
            echo -e "${YELLOW}Please ensure AWS CLI is configured with valid credentials${NC}"
            echo -e "Run: aws configure"
            exit 1
        fi
        
        echo -e "${GREEN}✓ Using existing credentials for production profile${NC}"
        SKIP_CREDENTIAL_TEST=false
    fi
else
    echo -e "${RED}❌ No AWS credentials found${NC}"
    echo -e "${YELLOW}Please configure AWS CLI first:${NC}"
    echo -e "  aws configure"
    echo -e "  OR"
    echo -e "  aws sso login"
    echo -e "Then run this script again."
    exit 1
fi

echo ""
echo -e "${YELLOW}Setting up AWS CLI profile...${NC}"

# Configure AWS CLI profile for production
if [ "$SKIP_CREDENTIAL_TEST" != "true" ]; then
    aws configure set aws_access_key_id "$PROD_ACCESS_KEY_ID" --profile avesa-production
    aws configure set aws_secret_access_key "$PROD_SECRET_ACCESS_KEY" --profile avesa-production
    aws configure set region "$AWS_REGION" --profile avesa-production
    aws configure set output json --profile avesa-production
    echo -e "${GREEN}✓ AWS CLI profile 'avesa-production' configured with credentials${NC}"
else
    # For SSO, just ensure region and output are set
    aws configure set region "$AWS_REGION" --profile avesa-production
    aws configure set output json --profile avesa-production
    echo -e "${GREEN}✓ AWS CLI profile 'avesa-production' configured for SSO${NC}"
fi

# Test the profile
echo -e "${YELLOW}Testing AWS CLI profile...${NC}"
if [ "$SKIP_CREDENTIAL_TEST" = "true" ]; then
    echo -e "${YELLOW}Skipping automatic profile test for SSO configuration${NC}"
    echo -e "${BLUE}To test the profile manually, run:${NC}"
    echo -e "  aws sts get-caller-identity --profile avesa-production"
    echo -e "${BLUE}If this fails, you may need to configure SSO access to account ${PROD_ACCOUNT_ID}${NC}"
else
    if aws sts get-caller-identity --profile avesa-production > /dev/null 2>&1; then
        CALLER_IDENTITY=$(aws sts get-caller-identity --profile avesa-production)
        ACCOUNT=$(echo "$CALLER_IDENTITY" | jq -r '.Account')
        USER_ARN=$(echo "$CALLER_IDENTITY" | jq -r '.Arn')
        
        echo -e "${GREEN}✓ Profile test successful${NC}"
        echo -e "  Account: $ACCOUNT"
        echo -e "  User: $USER_ARN"
        
        if [ "$ACCOUNT" != "$PROD_ACCOUNT_ID" ]; then
            echo -e "${RED}❌ Warning: Account ID mismatch!${NC}"
            echo -e "  Expected: $PROD_ACCOUNT_ID"
            echo -e "  Actual: $ACCOUNT"
            exit 1
        fi
    else
        echo -e "${RED}❌ Profile test failed${NC}"
        echo -e "Please check your credentials and try again"
        exit 1
    fi
fi

# Set environment variables for CDK
echo -e "${YELLOW}Setting up environment variables...${NC}"
echo ""
echo "# Add these to your shell profile (.bashrc, .zshrc, etc.):"
echo "export CDK_PROD_ACCOUNT=$PROD_ACCOUNT_ID"
echo "export CDK_DEFAULT_REGION=$AWS_REGION"
echo ""

# Export for current session
export CDK_PROD_ACCOUNT="$PROD_ACCOUNT_ID"
export CDK_DEFAULT_REGION="$AWS_REGION"

echo -e "${GREEN}✓ Environment variables set for current session${NC}"

# Create basic security setup script
echo -e "${YELLOW}Creating security setup commands...${NC}"

cat > setup-production-security.sh << EOF
#!/bin/bash
# Production Account Security Setup
# Run these commands in your production account

echo "Setting up production account security..."

# Enable CloudTrail
aws cloudtrail create-trail \\
    --name avesa-production-trail \\
    --s3-bucket-name avesa-production-cloudtrail-\${RANDOM} \\
    --include-global-service-events \\
    --is-multi-region-trail \\
    --profile avesa-production

# Enable GuardDuty
aws guardduty create-detector \\
    --enable \\
    --profile avesa-production

# Create billing alarm
aws cloudwatch put-metric-alarm \\
    --alarm-name "AVESA-Production-Billing-Alert" \\
    --alarm-description "Alert when monthly charges exceed \$1000" \\
    --metric-name EstimatedCharges \\
    --namespace AWS/Billing \\
    --statistic Maximum \\
    --period 86400 \\
    --threshold 1000 \\
    --comparison-operator GreaterThanThreshold \\
    --dimensions Name=Currency,Value=USD \\
    --evaluation-periods 1 \\
    --profile avesa-production

echo "Security setup complete!"
EOF

chmod +x setup-production-security.sh

echo -e "${GREEN}✓ Security setup script created: setup-production-security.sh${NC}"

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Setup Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${GREEN}Next steps:${NC}"
echo ""
echo -e "${YELLOW}1. Add environment variables to your shell profile:${NC}"
echo "   echo 'export CDK_PROD_ACCOUNT=$PROD_ACCOUNT_ID' >> ~/.bashrc"
echo "   echo 'export CDK_DEFAULT_REGION=$AWS_REGION' >> ~/.bashrc"
echo "   source ~/.bashrc"
echo ""
echo -e "${YELLOW}2. (Optional) Run security setup:${NC}"
echo "   ./setup-production-security.sh"
echo ""
echo -e "${YELLOW}3. Deploy infrastructure to production:${NC}"
echo "   ./scripts/deploy-prod.sh"
echo ""
echo -e "${YELLOW}4. Migrate production data:${NC}"
echo "   python3 scripts/migrate-production-data.py --dry-run"
echo "   python3 scripts/migrate-production-data.py --execute"
echo ""