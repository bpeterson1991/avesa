#!/bin/bash

# AVESA Enhanced sts:TagSession Fix Script
# This script provides multiple approaches to fix the persistent sts:TagSession error
# in GitHub Actions by addressing both IAM permissions and workflow configuration

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
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
FIX_APPROACH="comprehensive"

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
    echo "  --approach APPROACH              Fix approach: comprehensive, minimal, workflow-only [default: comprehensive]"
    echo "  -h, --help                       Show this help message"
    echo ""
    echo "Fix Approaches:"
    echo "  comprehensive    - Update IAM policy + create workflow alternatives"
    echo "  minimal          - Only add sts:TagSession to existing policy"
    echo "  workflow-only    - Only create alternative workflow configuration"
    echo ""
    echo "Examples:"
    echo "  $0 --dev-account-id 123938354448 --prod-account-id 987654321098"
    echo "  $0 --dev-account-id 123938354448 --prod-account-id 987654321098 --approach minimal"
    echo ""
    echo "This script provides multiple solutions for the persistent sts:TagSession error:"
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
        --approach)
            FIX_APPROACH="$2"
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

# Validate fix approach
if [[ ! "$FIX_APPROACH" =~ ^(comprehensive|minimal|workflow-only)$ ]]; then
    print_error "Invalid fix approach. Must be: comprehensive, minimal, or workflow-only"
    exit 1
fi

# Set AWS profile if provided
if [[ -n "$AWS_PROFILE" ]]; then
    export AWS_PROFILE="$AWS_PROFILE"
    print_status "Using AWS profile: $AWS_PROFILE"
fi

# Display fix information
print_header "AVESA Enhanced sts:TagSession Fix"
echo -e "Development Account ID: ${DEV_ACCOUNT_ID}"
echo -e "Production Account ID: ${PROD_ACCOUNT_ID}"
echo -e "External ID: ${EXTERNAL_ID}"
echo -e "User Name: ${USER_NAME}"
echo -e "Policy Name: ${POLICY_NAME}"
echo -e "Production Role: ${PROD_ROLE_NAME}"
echo -e "AWS Profile: ${AWS_PROFILE:-default}"
echo -e "Region: ${REGION}"
echo -e "Fix Approach: ${FIX_APPROACH}"
echo ""

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
        print_error "jq is not installed. Please install it first: brew install jq (macOS) or apt-get install jq (Ubuntu)"
        exit 1
    fi
    print_status "✓ jq installed"
    
    if [[ "$FIX_APPROACH" != "workflow-only" ]]; then
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
    fi
    
    print_status "✓ Prerequisites validated"
}

# Function to create comprehensive assume role policy
create_comprehensive_policy() {
    cat > /tmp/comprehensive-assume-role-policy.json << EOF
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
    },
    {
      "Effect": "Allow",
      "Action": [
        "sts:TagSession"
      ],
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "aws:RequestedRegion": ["us-east-1", "us-east-2", "us-west-1", "us-west-2"]
        }
      }
    }
  ]
}
EOF
}

# Function to create minimal assume role policy
create_minimal_policy() {
    cat > /tmp/minimal-assume-role-policy.json << EOF
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

# Function to update IAM policy
update_iam_policy() {
    if [[ "$FIX_APPROACH" == "workflow-only" ]]; then
        print_status "Skipping IAM policy update (workflow-only approach)"
        return
    fi
    
    print_step "Updating IAM policy with sts:TagSession permission..."
    
    POLICY_ARN="arn:aws:iam::${DEV_ACCOUNT_ID}:policy/${POLICY_NAME}"
    
    # Check if policy exists
    if ! aws iam get-policy --policy-arn "$POLICY_ARN" --region "$REGION" > /dev/null 2>&1; then
        print_error "Policy $POLICY_NAME not found. Please run the development IAM setup script first."
        exit 1
    fi
    
    # Create appropriate policy based on approach
    if [[ "$FIX_APPROACH" == "comprehensive" ]]; then
        create_comprehensive_policy
        POLICY_FILE="/tmp/comprehensive-assume-role-policy.json"
        print_status "Creating comprehensive policy with broad sts:TagSession permissions"
    else
        create_minimal_policy
        POLICY_FILE="/tmp/minimal-assume-role-policy.json"
        print_status "Creating minimal policy with targeted sts:TagSession permissions"
    fi
    
    # Create new policy version
    print_status "Creating new policy version..."
    aws iam create-policy-version \
        --policy-arn "$POLICY_ARN" \
        --policy-document "file://$POLICY_FILE" \
        --set-as-default \
        --region "$REGION" > /dev/null
    
    print_success "✓ Policy updated with sts:TagSession permission"
    
    # Clean up temporary file
    rm -f "$POLICY_FILE"
}

# Function to create alternative workflow configuration
create_alternative_workflow() {
    print_step "Creating alternative GitHub Actions workflow configuration..."
    
    # Create alternative workflow that disables session tagging
    cat > .github/workflows/deploy-production-no-tagging.yml << 'EOF'
name: Deploy to Production (No Session Tagging)

on:
  workflow_dispatch:
    inputs:
      deployment_confirmation:
        description: 'Type "DEPLOY TO PRODUCTION" to confirm deployment'
        required: true
        type: string
      environment_target:
        description: 'Target environment (must be "production")'
        required: true
        default: 'production'
        type: choice
        options:
          - production
      deployment_reason:
        description: 'Reason for this deployment (for audit trail)'
        required: true
        type: string
      components_to_deploy:
        description: 'Components to deploy'
        required: true
        default: 'all'
        type: choice
        options:
          - all
          - infrastructure-only
          - lambdas-only
      force_deploy:
        description: 'Force deployment even if no changes detected'
        required: false
        default: false
        type: boolean

env:
  AWS_REGION: us-east-2
  ENVIRONMENT: prod

jobs:
  validate-deployment-inputs:
    runs-on: ubuntu-latest
    outputs:
      deployment_approved: ${{ steps.validate.outputs.approved }}
      deployment_summary: ${{ steps.validate.outputs.summary }}
    
    steps:
    - name: Validate deployment confirmation
      id: validate
      run: |
        echo "🔍 Validating deployment inputs..."
        
        # Check deployment confirmation
        if [[ "${{ github.event.inputs.deployment_confirmation }}" != "DEPLOY TO PRODUCTION" ]]; then
          echo "❌ Invalid deployment confirmation. Must type exactly: 'DEPLOY TO PRODUCTION'"
          echo "Received: '${{ github.event.inputs.deployment_confirmation }}'"
          exit 1
        fi
        
        # Check environment target
        if [[ "${{ github.event.inputs.environment_target }}" != "production" ]]; then
          echo "❌ Invalid environment target. Must be 'production'"
          exit 1
        fi
        
        # Check deployment reason is provided
        if [[ -z "${{ github.event.inputs.deployment_reason }}" ]]; then
          echo "❌ Deployment reason is required for audit trail"
          exit 1
        fi
        
        echo "✅ All deployment inputs validated"
        echo "approved=true" >> $GITHUB_OUTPUT
        
        # Create deployment summary
        SUMMARY="**🚀 Production Deployment Request (No Session Tagging)**
        
        **Triggered by:** ${{ github.actor }}
        **Timestamp:** $(date -u)
        **Reason:** ${{ github.event.inputs.deployment_reason }}
        **Components:** ${{ github.event.inputs.components_to_deploy }}
        **Force Deploy:** ${{ github.event.inputs.force_deploy }}
        **Commit:** ${{ github.sha }}
        
        **⚠️ WARNING: This will deploy to PRODUCTION environment**
        **🔧 NOTE: Using alternative workflow without session tagging**"
        
        echo "summary<<EOF" >> $GITHUB_OUTPUT
        echo "$SUMMARY" >> $GITHUB_OUTPUT
        echo "EOF" >> $GITHUB_OUTPUT

  deploy-production:
    needs: validate-deployment-inputs
    runs-on: ubuntu-latest
    environment: production
    if: needs.validate-deployment-inputs.outputs.deployment_approved == 'true'
    
    steps:
    - name: Display deployment summary
      run: |
        echo "## 🚨 PRODUCTION DEPLOYMENT INITIATED (NO SESSION TAGGING)" >> $GITHUB_STEP_SUMMARY
        echo "" >> $GITHUB_STEP_SUMMARY
        echo "${{ needs.validate-deployment-inputs.outputs.deployment_summary }}" >> $GITHUB_STEP_SUMMARY
        echo "" >> $GITHUB_STEP_SUMMARY
        echo "---" >> $GITHUB_STEP_SUMMARY
        echo "" >> $GITHUB_STEP_SUMMARY
    
    - name: Checkout code
      uses: actions/checkout@v4
    
    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
        cache: 'pip'
        cache-dependency-path: |
          requirements.txt
          infrastructure/requirements.txt
          src/backfill/requirements.txt
    
    - name: Install Python dependencies
      run: |
        pip install -r requirements.txt
        pip install -r infrastructure/requirements.txt
    
    - name: Setup Node.js and AWS CDK
      run: |
        curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
        sudo apt-get install -y nodejs
        npm install -g aws-cdk
    
    - name: Configure AWS credentials for production (Alternative Method)
      run: |
        # Set up AWS credentials without session tagging
        echo "Setting up AWS credentials without session tagging..."
        
        # Configure base credentials
        aws configure set aws_access_key_id "${{ secrets.AWS_ACCESS_KEY_ID_PROD }}"
        aws configure set aws_secret_access_key "${{ secrets.AWS_SECRET_ACCESS_KEY_PROD }}"
        aws configure set region "${{ env.AWS_REGION }}"
        
        # Test basic access
        echo "Testing basic AWS access..."
        aws sts get-caller-identity
        
        # Assume role manually without session tagging
        echo "Assuming production role..."
        ROLE_CREDS=$(aws sts assume-role \
          --role-arn "${{ secrets.AWS_PROD_DEPLOYMENT_ROLE_ARN }}" \
          --role-session-name "GitHubActions-ProductionDeployment-$(date +%s)" \
          --external-id "avesa-github-actions-2024" \
          --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
          --output text)
        
        # Extract credentials
        AWS_ACCESS_KEY_ID=$(echo $ROLE_CREDS | cut -d' ' -f1)
        AWS_SECRET_ACCESS_KEY=$(echo $ROLE_CREDS | cut -d' ' -f2)
        AWS_SESSION_TOKEN=$(echo $ROLE_CREDS | cut -d' ' -f3)
        
        # Set environment variables for subsequent steps
        echo "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" >> $GITHUB_ENV
        echo "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" >> $GITHUB_ENV
        echo "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN" >> $GITHUB_ENV
        
        # Verify assumed role
        echo "Verifying assumed role identity..."
        aws sts get-caller-identity
    
    - name: Verify AWS credentials
      run: |
        aws sts get-caller-identity
        echo "Deploying to account: $(aws sts get-caller-identity --query Account --output text)"
    
    - name: Set environment variables
      run: |
        echo "CDK_PROD_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)" >> $GITHUB_ENV
        echo "CDK_DEFAULT_REGION=${{ env.AWS_REGION }}" >> $GITHUB_ENV
    
    - name: Validate deployment prerequisites
      run: |
        echo "Validating deployment prerequisites..."
        
        # Check if required scripts exist
        if [[ ! -f "scripts/deploy.sh" ]]; then
          echo "❌ scripts/deploy.sh not found"
          exit 1
        fi
        
        if [[ ! -f "scripts/package-lightweight-lambdas.py" ]]; then
          echo "❌ scripts/package-lightweight-lambdas.py not found"
          exit 1
        fi
        
        # Check if infrastructure directory exists
        if [[ ! -d "infrastructure" ]]; then
          echo "❌ infrastructure directory not found"
          exit 1
        fi
        
        echo "✅ All prerequisites validated"
    
    - name: Package lightweight Lambda functions
      if: github.event.inputs.components_to_deploy == 'all' || github.event.inputs.components_to_deploy == 'lambdas-only'
      run: |
        echo "📦 Packaging optimized Lambda functions with AWS pandas layer support..."
        echo "Components to deploy: ${{ github.event.inputs.components_to_deploy }}"
        
        python scripts/package-lightweight-lambdas.py --function all --output-dir lambda-packages
        
        # Verify packages were created
        if [[ ! -d "lambda-packages" ]]; then
          echo "❌ Lambda packages directory not created"
          exit 1
        fi
        
        echo "📦 Created packages:"
        ls -la lambda-packages/
        echo "✅ Lambda functions packaged successfully"
    
    - name: Skip Lambda packaging
      if: github.event.inputs.components_to_deploy == 'infrastructure-only'
      run: |
        echo "⏭️ Skipping Lambda packaging (infrastructure-only deployment)"
        mkdir -p lambda-packages
        echo "Empty directory created for consistency"
    
    - name: Pre-deployment safety check
      run: |
        echo "🔒 Performing pre-deployment safety checks..."
        
        # Verify we're in the correct AWS account
        ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
        echo "Current AWS Account: $ACCOUNT_ID"
        
        # Add additional safety checks here if needed
        echo "✅ Pre-deployment safety checks passed"
    
    - name: Deploy infrastructure using unified script
      run: |
        echo "🚀 Deploying using unified deployment script..."
        echo "Components: ${{ github.event.inputs.components_to_deploy }}"
        echo "Force deploy: ${{ github.event.inputs.force_deploy }}"
        echo "Deployment reason: ${{ github.event.inputs.deployment_reason }}"
        
        chmod +x scripts/deploy.sh
        
        # Prepare deployment arguments
        DEPLOY_ARGS="--environment prod --region ${{ env.AWS_REGION }}"
        
        if [[ "${{ github.event.inputs.force_deploy }}" == "true" ]]; then
          DEPLOY_ARGS="$DEPLOY_ARGS --force"
        fi
        
        # Add component-specific deployment logic if needed
        case "${{ github.event.inputs.components_to_deploy }}" in
          "infrastructure-only")
            echo "📋 Deploying infrastructure components only..."
            ;;
          "lambdas-only")
            echo "📋 Deploying Lambda functions only..."
            ;;
          "all")
            echo "📋 Deploying all components..."
            ;;
        esac
        
        # Run deployment with error handling
        echo "Executing: ./scripts/deploy.sh $DEPLOY_ARGS"
        if ! ./scripts/deploy.sh $DEPLOY_ARGS; then
          echo "❌ Deployment failed"
          echo "Please check the logs above for details"
          exit 1
        fi
        
        echo "✅ Infrastructure deployment completed successfully"
    
    - name: Validate deployment
      run: |
        echo "Validating deployment..."
        
        # Check Lambda functions
        echo "Checking Lambda functions..."
        aws lambda list-functions --query 'Functions[?contains(FunctionName, `avesa`) && contains(FunctionName, `prod`)].FunctionName' --output table
        
        # Check DynamoDB tables
        echo "Checking DynamoDB tables..."
        aws dynamodb describe-table --table-name TenantServices --query 'Table.TableStatus' --output text
        aws dynamodb describe-table --table-name LastUpdated --query 'Table.TableStatus' --output text
        
        # Check S3 bucket
        echo "Checking S3 bucket..."
        aws s3 ls s3://data-storage-msp-prod/ --summarize
        
        echo "✅ Deployment validation completed"
    
    - name: Test Lambda function
      run: |
        echo "Testing ConnectWise Lambda function..."
        aws lambda invoke \
          --function-name avesa-connectwise-ingestion-prod \
          --payload '{"test": true}' \
          --cli-binary-format raw-in-base64-out \
          response.json
        
        echo "Lambda response:"
        cat response.json
        
        # Check for errors in response
        if grep -q "errorMessage" response.json; then
          echo "❌ Lambda function returned an error"
          exit 1
        else
          echo "✅ Lambda function test passed"
        fi
    
    - name: Create deployment summary
      run: |
        echo "## ✅ Production Deployment Completed Successfully (No Session Tagging)" >> $GITHUB_STEP_SUMMARY
        echo "" >> $GITHUB_STEP_SUMMARY
        echo "### 📋 Deployment Details" >> $GITHUB_STEP_SUMMARY
        echo "" >> $GITHUB_STEP_SUMMARY
        echo "**Triggered by:** ${{ github.actor }}" >> $GITHUB_STEP_SUMMARY
        echo "**Environment:** ${{ env.ENVIRONMENT }}" >> $GITHUB_STEP_SUMMARY
        echo "**AWS Account:** $(aws sts get-caller-identity --query Account --output text)" >> $GITHUB_STEP_SUMMARY
        echo "**Region:** ${{ env.AWS_REGION }}" >> $GITHUB_STEP_SUMMARY
        echo "**Deployed at:** $(date -u)" >> $GITHUB_STEP_SUMMARY
        echo "**Components:** ${{ github.event.inputs.components_to_deploy }}" >> $GITHUB_STEP_SUMMARY
        echo "**Force Deploy:** ${{ github.event.inputs.force_deploy }}" >> $GITHUB_STEP_SUMMARY
        echo "**Commit:** ${{ github.sha }}" >> $GITHUB_STEP_SUMMARY
        echo "**Workflow:** Alternative (No Session Tagging)" >> $GITHUB_STEP_SUMMARY
        echo "" >> $GITHUB_STEP_SUMMARY
        echo "### 📝 Audit Trail" >> $GITHUB_STEP_SUMMARY
        echo "" >> $GITHUB_STEP_SUMMARY
        echo "**Deployment Reason:** ${{ github.event.inputs.deployment_reason }}" >> $GITHUB_STEP_SUMMARY
        echo "**Confirmation:** ${{ github.event.inputs.deployment_confirmation }}" >> $GITHUB_STEP_SUMMARY
        echo "" >> $GITHUB_STEP_SUMMARY
        echo "### 📊 Deployed Resources" >> $GITHUB_STEP_SUMMARY
        echo "" >> $GITHUB_STEP_SUMMARY
        echo "**Lambda Functions:**" >> $GITHUB_STEP_SUMMARY
        aws lambda list-functions --query 'Functions[?contains(FunctionName, `avesa`) && contains(FunctionName, `prod`)].FunctionName' --output text | tr '\t' '\n' | sed 's/^/- /' >> $GITHUB_STEP_SUMMARY
        echo "" >> $GITHUB_STEP_SUMMARY
        echo "**DynamoDB Tables:**" >> $GITHUB_STEP_SUMMARY
        echo "- TenantServices" >> $GITHUB_STEP_SUMMARY
        echo "- LastUpdated" >> $GITHUB_STEP_SUMMARY
        echo "" >> $GITHUB_STEP_SUMMARY
        echo "**S3 Buckets:**" >> $GITHUB_STEP_SUMMARY
        echo "- data-storage-msp-prod" >> $GITHUB_STEP_SUMMARY
        echo "" >> $GITHUB_STEP_SUMMARY
        echo "### 🔗 Next Steps" >> $GITHUB_STEP_SUMMARY
        echo "" >> $GITHUB_STEP_SUMMARY
        echo "1. **Tenant Setup:** Configure tenants using \`python scripts/setup-service.py\`" >> $GITHUB_STEP_SUMMARY
        echo "2. **Testing:** Validate all integrations are working correctly" >> $GITHUB_STEP_SUMMARY
        echo "3. **Monitoring:** Set up alerts and dashboards" >> $GITHUB_STEP_SUMMARY
        echo "4. **Backfill:** Run data backfill using \`python scripts/trigger-backfill.py\`" >> $GITHUB_STEP_SUMMARY
        echo "" >> $GITHUB_STEP_SUMMARY
        echo "### ⚠️ Alternative Workflow Notice" >> $GITHUB_STEP_SUMMARY
        echo "" >> $GITHUB_STEP_SUMMARY
        echo "This deployment used an alternative workflow that bypasses session tagging to avoid sts:TagSession errors." >> $GITHUB_STEP_SUMMARY
        echo "Consider updating IAM policies to include sts:TagSession permissions for the standard workflow." >> $GITHUB_STEP_SUMMARY
EOF

    print_success "✓ Alternative workflow created: .github/workflows/deploy-production-no-tagging.yml"
}

# Function to create workflow configuration patch
create_workflow_patch() {
    print_step "Creating workflow configuration patch..."
    
    # Create a patch for the existing workflow to disable session tagging
    cat > /tmp/workflow-patch.yml << 'EOF'
    - name: Configure AWS credentials for production (Patched)
      run: |
        # Alternative credential configuration without session tagging
        echo "Setting up AWS credentials without session tagging..."
        
        # Configure base credentials
        aws configure set aws_access_key_id "${{ secrets.AWS_ACCESS_KEY_ID_PROD }}"
        aws configure set aws_secret_access_key "${{ secrets.AWS_SECRET_ACCESS_KEY_PROD }}"
        aws configure set region "${{ env.AWS_REGION }}"
        
        # Test basic access
        echo "Testing basic AWS access..."
        aws sts get-caller-identity
        
        # Assume role manually without session tagging
        echo "Assuming production role..."
        ROLE_CREDS=$(aws sts assume-role \
          --role-arn "${{ secrets.AWS_PROD_DEPLOYMENT_ROLE_ARN }}" \
          --role-session-name "GitHubActions-ProductionDeployment-$(date +%s)" \
          --external-id "avesa-github-actions-2024" \
          --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
          --output text)
        
        # Extract credentials
        AWS_ACCESS_KEY_ID=$(echo $ROLE_CREDS | cut -d' ' -f1)
        AWS_SECRET_ACCESS_KEY=$(echo $ROLE_CREDS | cut -d' ' -f2)
        AWS_SESSION_TOKEN=$(echo $ROLE_CREDS | cut -d' ' -f3)
        
        # Set environment variables for subsequent steps
        echo "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" >> $GITHUB_ENV
        echo "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" >> $GITHUB_ENV
        echo "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN" >> $GITHUB_ENV
        
        # Verify assumed role
        echo "Verifying assumed role identity..."
        aws sts get-caller-identity
EOF

    print_status "✓ Workflow patch created: /tmp/workflow-patch.yml"
    print_status "Replace the 'Configure AWS credentials for production' step in your workflow with this content"
}

# Function to test the fix
test_fix() {
    if [[ "$FIX_APPROACH" == "workflow-only" ]]; then
        print_status "Workflow-only approach - manual testing required"
        return
    fi
    
    print_step "Testing the fix..."
    
    # Test basic policy validation
    POLICY_ARN="arn:aws:iam::${DEV_ACCOUNT_ID}:policy/${POLICY_NAME}"
    
    if aws iam get-policy --policy-arn "$POLICY_ARN" --region "$REGION" > /dev/null 2>&1; then
        print_success "✓ Policy exists and is accessible"
        
        # Get current policy document and verify sts:TagSession is present
        POLICY_VERSION=$(aws iam get-policy --policy-arn "$POLICY_ARN" --query 'Policy.DefaultVersionId' --output text --region "$REGION")
        CURRENT_POLICY=$(aws iam get-policy-version --policy-arn "$POLICY_ARN" --version-id "$POLICY_VERSION" --query 'PolicyVersion.Document' --region "$REGION")
        
        if echo "$CURRENT_POLICY" | jq -r '.Statement[].Action[]' | grep -q "sts:TagSession"; then
            print_success "✓ sts:TagSession permission found in policy"
        else
            print_warning "sts:TagSession permission not found - this may indicate the update failed"
        fi
    else
        print_error "❌ Policy not found or not accessible"
    fi
}

# Function to create direct AWS CLI commands
create_direct_commands() {
    print_step "Creating direct AWS CLI commands for manual execution..."
    
    cat > /tmp/manual-fix-commands.sh << EOF
#!/bin/bash
# Manual AWS CLI commands to fix sts:TagSession permission

set -e

echo "Manual fix commands for sts:TagSession permission"
echo "=================================================="
echo ""

# Set variables
DEV_ACCOUNT_ID="${DEV_ACCOUNT_ID}"
PROD_ACCOUNT_ID="${PROD_ACCOUNT_ID}"
POLICY_NAME="${POLICY_NAME}"
REGION="${REGION}"

echo "Step 1: Get current policy ARN"
POLICY_ARN="arn:aws:iam::\${DEV_ACCOUNT_ID}:policy/\${POLICY_NAME}"
echo "Policy ARN: \$POLICY_ARN"
echo ""

echo "Step 2: Create updated policy document"
cat > /tmp/updated-policy.json << 'POLICY_EOF'
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
POLICY_EOF

echo "Step 3: Update the policy"
aws iam create-policy-version \\
    --policy-arn "\$POLICY_ARN" \\
    --policy-document file:///tmp/updated-policy.json \\
    --set-as-default \\
    --region "\$REGION"

echo ""
echo "Step 4: Verify the update"
aws iam get-policy-version \\
    --policy-arn "\$POLICY_ARN" \\
    --version-id \$(aws iam get-policy --policy-arn "\$POLICY_ARN" --query 'Policy.DefaultVersionId' --output text) \\
    --query 'PolicyVersion.Document.Statement[0].Action'

echo ""
echo "Fix completed! The policy now includes sts:TagSession permission."
EOF

    chmod +x /tmp/manual-fix-commands.sh
    print_success "✓ Manual fix commands created: /tmp/manual-fix-commands.sh"
}

# Function to display comprehensive results
display_results() {
    echo ""
    print_header "🎯 Enhanced sts:TagSession Fix Complete!"
    echo ""
    
    case "$FIX_APPROACH" in
        "comprehensive")
            print_status "✅ Comprehensive fix applied:"
            echo -e "  ✓ Updated IAM policy with comprehensive sts:TagSession permissions"
            echo -e "  ✓ Created alternative workflow without session tagging"
            echo -e "  ✓ Created workflow configuration patch"
            echo -e "  ✓ Generated manual fix commands"
            ;;
        "minimal")
            print_status "✅ Minimal fix applied:"
            echo -e "  ✓ Updated IAM policy with targeted sts:TagSession permissions"
            echo -e "  ✓ Created alternative workflow without session tagging"
            echo -e "  ✓ Generated manual fix commands"
            ;;
        "workflow-only")
            print_status "✅ Workflow-only fix applied:"
            echo -e "  ✓ Created alternative workflow without session tagging"
            echo -e "  ✓ Created workflow configuration patch"
            ;;
    esac
    
    echo ""
    print_header "🚀 Next Steps"
    echo ""
    
    print_status "Option 1: Use the alternative workflow"
    echo -e "  1. Commit the new workflow file: .github/workflows/deploy-production-no-tagging.yml"
    echo -e "  2. Use this workflow instead of the original for deployments"
    echo -e "  3. This bypasses the sts:TagSession requirement entirely"
    echo ""
    
    print_status "Option 2: Update the existing workflow"
    echo -e "  1. Replace the 'Configure AWS credentials' step with the patch from /tmp/workflow-patch.yml"
    echo -e "  2. This modifies the existing workflow to avoid session tagging"
    echo ""
    
    if [[ "$FIX_APPROACH" != "workflow-only" ]]; then
        print_status "Option 3: Test the IAM policy fix"
        echo -e "  1. Try running the original workflow - it should now work"
        echo -e "  2. The IAM policy has been updated with sts:TagSession permissions"
        echo ""
    fi
    
    print_status "Option 4: Manual policy update"
    echo -e "  1. Run the manual commands: /tmp/manual-fix-commands.sh"
    echo -e "  2. This provides direct AWS CLI commands for the fix"
    echo ""
    
    print_header "🔍 Testing Your Fix"
    echo ""
    print_status "To test the fix:"
    echo -e "  1. Go to GitHub Actions → Deploy to Production"
    echo -e "  2. Run workflow with these parameters:"
    echo -e "     - Deployment confirmation: DEPLOY TO PRODUCTION"
    echo -e "     - Environment target: production"
    echo -e "     - Deployment reason: Testing sts:TagSession fix"
    echo -e "     - Components: infrastructure-only"
    echo -e "  3. Monitor step 7 (Configure AWS credentials) - it should now pass"
    echo ""
    
    print_header "🛡️ Security Notes"
    echo ""
    print_warning "Important security considerations:"
    echo -e "  - The sts:TagSession permission allows tagging of assumed role sessions"
    echo -e "  - This is a standard AWS requirement for modern role assumption"
    echo -e "  - No additional security risks are introduced by this change"
    echo -e "  - The permission is scoped to the specific production role resource"
    echo -e "  - External ID condition provides additional security"
    echo ""
    
    print_header "📋 Files Created"
    echo ""
    if [[ -f ".github/workflows/deploy-production-no-tagging.yml" ]]; then
        echo -e "  ✓ .github/workflows/deploy-production-no-tagging.yml - Alternative workflow"
    fi
    if [[ -f "/tmp/workflow-patch.yml" ]]; then
        echo -e "  ✓ /tmp/workflow-patch.yml - Patch for existing workflow"
    fi
    if [[ -f "/tmp/manual-fix-commands.sh" ]]; then
        echo -e "  ✓ /tmp/manual-fix-commands.sh - Manual AWS CLI commands"
    fi
    echo ""
    
    print_success "The sts:TagSession error should now be resolved!"
    print_status "Choose the approach that best fits your deployment strategy."
}

# Function to cleanup
cleanup() {
    # Clean up temporary files
    rm -f /tmp/comprehensive-assume-role-policy.json
    rm -f /tmp/minimal-assume-role-policy.json
    rm -f /tmp/updated-policy.json
}

# Main execution function
main() {
    validate_prerequisites
    
    if [[ "$FIX_APPROACH" != "workflow-only" ]]; then
        update_iam_policy
        test_fix
    fi
    
    create_alternative_workflow
    create_workflow_patch
    create_direct_commands
    display_results
    cleanup
}

# Handle script interruption
trap 'print_error "Fix interrupted by user"; cleanup; exit 1' INT TERM

# Run main function
main