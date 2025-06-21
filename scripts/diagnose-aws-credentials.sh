#!/bin/bash

# AWS Credentials Diagnostic Script
# This script helps diagnose AWS credential configuration issues

echo "🔍 AWS CREDENTIALS DIAGNOSTIC REPORT"
echo "===================================="
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

# 1. Check AWS CLI installation
print_status "Checking AWS CLI installation..."
if command -v aws &> /dev/null; then
    AWS_VERSION=$(aws --version 2>&1)
    print_success "AWS CLI installed: $AWS_VERSION"
else
    print_error "AWS CLI not installed"
    echo "Install with: brew install awscli"
    exit 1
fi

echo ""

# 2. Check available profiles
print_status "Checking available AWS profiles..."
PROFILES=$(aws configure list-profiles 2>/dev/null)
if [ $? -eq 0 ] && [ ! -z "$PROFILES" ]; then
    print_success "Available profiles:"
    echo "$PROFILES" | while read profile; do
        echo "   • $profile"
    done
else
    print_warning "No AWS profiles configured"
fi

echo ""

# 3. Check current AWS environment variables
print_status "Checking AWS environment variables..."
echo "   AWS_PROFILE: ${AWS_PROFILE:-'(not set)'}"
echo "   AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:+***set***}"
echo "   AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:+***set***}"
echo "   AWS_SESSION_TOKEN: ${AWS_SESSION_TOKEN:+***set***}"
echo "   AWS_REGION: ${AWS_REGION:-'(not set)'}"
echo "   AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION:-'(not set)'}"

echo ""

# 4. Test default credentials
print_status "Testing default AWS credentials..."
if aws sts get-caller-identity > /dev/null 2>&1; then
    IDENTITY=$(aws sts get-caller-identity 2>/dev/null)
    print_success "Default credentials work:"
    echo "$IDENTITY" | jq -r '"   Account: " + .Account + "\n   User: " + .Arn' 2>/dev/null || echo "$IDENTITY"
else
    print_error "Default credentials not working"
fi

echo ""

# 5. Test specific profile used by script
TARGET_PROFILE="AdministratorAccess-123938354448"
print_status "Testing target profile: $TARGET_PROFILE..."
if AWS_PROFILE=$TARGET_PROFILE aws sts get-caller-identity > /dev/null 2>&1; then
    IDENTITY=$(AWS_PROFILE=$TARGET_PROFILE aws sts get-caller-identity 2>/dev/null)
    print_success "Target profile works:"
    echo "$IDENTITY" | jq -r '"   Account: " + .Account + "\n   User: " + .Arn' 2>/dev/null || echo "$IDENTITY"
else
    print_error "Target profile '$TARGET_PROFILE' not working"
fi

echo ""

# 6. Test AWS SDK for Node.js (simulating what the API does)
print_status "Testing AWS SDK credential resolution..."
node -e "
const AWS = require('aws-sdk');
AWS.config.update({region: 'us-east-2'});

// Test default credential chain
const sts = new AWS.STS();
sts.getCallerIdentity({}, (err, data) => {
    if (err) {
        console.log('❌ AWS SDK default credentials failed:', err.message);
    } else {
        console.log('✅ AWS SDK default credentials work');
        console.log('   Account:', data.Account);
        console.log('   User:', data.Arn);
    }
});

// Test with specific profile
process.env.AWS_PROFILE = '$TARGET_PROFILE';
const stsWithProfile = new AWS.STS();
stsWithProfile.getCallerIdentity({}, (err, data) => {
    if (err) {
        console.log('❌ AWS SDK with profile failed:', err.message);
    } else {
        console.log('✅ AWS SDK with profile works');
        console.log('   Account:', data.Account);
        console.log('   User:', data.Arn);
    }
});
" 2>/dev/null || print_warning "Node.js AWS SDK test skipped (dependencies not available)"

echo ""

# 7. Test Secrets Manager access
print_status "Testing AWS Secrets Manager access..."
SECRET_NAME="arn:aws:secretsmanager:us-east-2:123938354448:secret:clickhouse-connection-dev-V9zSgO"
if AWS_PROFILE=$TARGET_PROFILE aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --region us-east-2 > /dev/null 2>&1; then
    print_success "Secrets Manager access works"
else
    print_error "Cannot access Secrets Manager secret: $SECRET_NAME"
fi

echo ""

# 8. Recommendations
print_status "RECOMMENDATIONS:"
echo ""

if [ -z "$AWS_PROFILE" ]; then
    print_warning "Set AWS_PROFILE environment variable:"
    echo "   export AWS_PROFILE=$TARGET_PROFILE"
fi

echo "🔧 To fix AWS credentials issues:"
echo "   1. Set the AWS profile: export AWS_PROFILE=$TARGET_PROFILE"
echo "   2. Or run: aws configure --profile $TARGET_PROFILE"
echo "   3. Or use AWS SSO: aws sso login --profile $TARGET_PROFILE"
echo ""
echo "🚀 To run the ClickHouse servers:"
echo "   export AWS_PROFILE=$TARGET_PROFILE"
echo "   ./scripts/start-real-clickhouse-servers.sh"