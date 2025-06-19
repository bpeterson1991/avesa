# Production Account Setup Guide

This guide walks you through creating and configuring a dedicated AWS account for AVESA production workloads.

## Prerequisites

- Administrative access to create new AWS accounts
- AWS CLI installed and configured
- CDK installed (`npm install -g aws-cdk`)
- Python 3.9+ installed

## Step 1: Create Production AWS Account

### 1.1 Account Creation

1. **Go to AWS Account Creation:**
   - Visit: https://aws.amazon.com/
   - Click "Create an AWS Account"

2. **Account Details:**
   - **Email:** Use a dedicated email (e.g., `avesa-production@yourcompany.com`)
   - **Account Name:** `AVESA Production`
   - **Password:** Use a strong, unique password

3. **Contact Information:**
   - Choose "Business" account type
   - Fill in your company information

4. **Payment Information:**
   - Add a valid payment method
   - Complete phone verification

5. **Support Plan:**
   - Choose appropriate support plan (Basic is fine for now)

### 1.2 Initial Security Setup

**⚠️ CRITICAL: Do these steps immediately after account creation**

1. **Enable MFA for Root Account:**
   ```bash
   # In AWS Console:
   # 1. Click account name → Security credentials
   # 2. Enable MFA for root account
   # 3. Use authenticator app (Google Authenticator, Authy, etc.)
   ```

2. **Create Administrative IAM User:**
   ```bash
   # In AWS Console → IAM:
   # 1. Create user: avesa-admin
   # 2. Attach policy: AdministratorAccess
   # 3. Create access keys for programmatic access
   # 4. Enable MFA for this user too
   ```

3. **Note Important Information:**
   - **Account ID:** (12-digit number, found in top-right of console)
   - **Admin Access Key ID**
   - **Admin Secret Access Key**

## Step 2: Configure Local Environment

### 2.1 Run Setup Script

```bash
# Make the setup script executable
chmod +x scripts/setup-production-account.sh

# Run the setup script
./scripts/setup-production-account.sh
```

The script will prompt you for:
- Production AWS Account ID
- AWS Region (default: us-east-1)
- Production AWS Access Key ID
- Production AWS Secret Access Key

### 2.2 Verify Setup

```bash
# Test the AWS profile
aws sts get-caller-identity --profile avesa-production

# Should return something like:
# {
#     "UserId": "AIDACKCEVSQ6C2EXAMPLE",
#     "Account": "123456789012",
#     "Arn": "arn:aws:iam::123456789012:user/avesa-admin"
# }
```

### 2.3 Set Environment Variables

Add these to your shell profile (`.bashrc`, `.zshrc`, etc.):

```bash
export CDK_PROD_ACCOUNT=123456789012  # Your actual account ID
export CDK_DEFAULT_REGION=us-east-1
```

Then reload your shell:
```bash
source ~/.bashrc  # or ~/.zshrc
```

## Step 3: Deploy Infrastructure

### 3.1 Bootstrap CDK

```bash
# Navigate to infrastructure directory
cd infrastructure

# Bootstrap CDK in production account
cdk bootstrap --profile avesa-production --context environment=prod
```

### 3.2 Deploy All Stacks

```bash
# Use the deployment script
./scripts/deploy-prod.sh

# Or manually:
cd infrastructure
cdk deploy --all --context environment=prod --profile avesa-production
```

Expected stacks to be deployed:
- `ConnectWiseDataPipeline-prod`
- `ConnectWiseBackfill-prod`
- `ConnectWiseMonitoring-prod`
- `ConnectWiseCrossAccountMonitoring-prod`

### 3.3 Verify Deployment

```bash
# Check CloudFormation stacks
aws cloudformation list-stacks --profile avesa-production

# Check Lambda functions
aws lambda list-functions --profile avesa-production

# Check DynamoDB tables
aws dynamodb list-tables --profile avesa-production

# Check S3 bucket
aws s3 ls --profile avesa-production
```

## Step 4: Data Migration

### 4.1 Dry Run Migration

```bash
# Test migration without making changes
python3 scripts/migrate-production-data.py --dry-run
```

### 4.2 Execute Migration

```bash
# Perform actual migration
python3 scripts/migrate-production-data.py --execute
```

This will migrate:
- **DynamoDB Data:** `TenantServices-prod` → `TenantServices`, `LastUpdated-prod` → `LastUpdated`
- **S3 Data:** `data-storage-msp` → `data-storage-msp-prod`
- **Secrets:** All production tenant secrets

### 4.3 Validate Migration

```bash
# Run validation script
python3 scripts/validate-hybrid-setup.py --environment prod
```

## Step 5: Security Hardening (Optional but Recommended)

### 5.1 Enable Additional Security Services

```bash
# Run the security setup script (created by setup-production-account.sh)
./setup-production-security.sh
```

This enables:
- CloudTrail for audit logging
- GuardDuty for threat detection
- Billing alarms

### 5.2 Additional Security Measures

1. **Enable AWS Config:**
   ```bash
   aws configservice put-configuration-recorder \
     --configuration-recorder name=default,roleARN=arn:aws:iam::ACCOUNT:role/aws-service-role/config.amazonaws.com/AWSServiceRoleForConfig \
     --profile avesa-production
   ```

2. **Set up VPC Flow Logs** (if using VPC)

3. **Configure AWS Security Hub**

## Step 6: Monitoring Setup

### 6.1 Subscribe to Alerts

```bash
# Get the SNS topic ARN from CDK output
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:ACCOUNT:avesa-alerts-prod \
  --protocol email \
  --notification-endpoint your-email@company.com \
  --profile avesa-production
```

### 6.2 Access CloudWatch Dashboard

1. Go to AWS Console → CloudWatch → Dashboards
2. Open "AVESA-DataPipeline-PROD"
3. Pin to favorites for easy access

## Step 7: Testing and Validation

### 7.1 Test Lambda Functions

```bash
# Test ingestion function
aws lambda invoke \
  --function-name avesa-connectwise-ingestion-prod \
  --payload '{"test": true}' \
  response.json \
  --profile avesa-production

# Check response
cat response.json
```

### 7.2 Monitor Logs

```bash
# Tail Lambda logs
aws logs tail /aws/lambda/avesa-connectwise-ingestion-prod \
  --follow \
  --profile avesa-production
```

### 7.3 Validate Data Pipeline

```bash
# Run comprehensive validation
python3 scripts/validate-hybrid-setup.py --environment prod
```

## Troubleshooting

### Common Issues

1. **CDK Bootstrap Fails:**
   ```bash
   # Ensure you have the right permissions
   aws sts get-caller-identity --profile avesa-production
   
   # Try bootstrapping with explicit account/region
   cdk bootstrap aws://ACCOUNT-ID/us-east-1 --profile avesa-production
   ```

2. **Migration Script Fails:**
   ```bash
   # Check if source tables exist
   aws dynamodb list-tables
   
   # Check if destination profile is configured
   aws sts get-caller-identity --profile avesa-production
   ```

3. **Lambda Functions Not Working:**
   ```bash
   # Check function configuration
   aws lambda get-function --function-name FUNCTION-NAME --profile avesa-production
   
   # Check logs for errors
   aws logs filter-log-events \
     --log-group-name /aws/lambda/FUNCTION-NAME \
     --profile avesa-production
   ```

### Getting Help

- Check CloudWatch logs for detailed error messages
- Review CloudFormation events for deployment issues
- Use the validation script to identify configuration problems
- Refer to the main implementation plan: `docs/AWS_ACCOUNT_ISOLATION_IMPLEMENTATION_PLAN.md`

## Next Steps After Setup

1. **Update Application Configuration** to point to production account
2. **Set up CI/CD Pipeline** for production deployments
3. **Configure Backup and Recovery** procedures
4. **Plan Production Cutover** from current account
5. **Set up Compliance Monitoring** if required

## Security Checklist

- [ ] Root account MFA enabled
- [ ] Administrative IAM user created with MFA
- [ ] CloudTrail enabled in all regions
- [ ] GuardDuty enabled
- [ ] Billing alerts configured
- [ ] AWS Config enabled (optional)
- [ ] VPC Flow Logs enabled (if using VPC)
- [ ] Security Hub configured (optional)
- [ ] SNS alerts subscribed
- [ ] Regular access review scheduled

## Cost Optimization

- Monitor AWS Cost Explorer regularly
- Set up additional billing alerts at different thresholds
- Review resource utilization monthly
- Consider Reserved Instances for predictable workloads
- Use AWS Trusted Advisor recommendations

---

**Important:** Keep your account credentials secure and follow your organization's security policies for AWS account management.