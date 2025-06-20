# AVESA Scripts Directory

This directory contains automated scripts for setting up and managing the AVESA Multi-Tenant Data Pipeline optimized infrastructure.

**Last Updated:** December 19, 2025
**Status:** Final cleanup completed - optimized for day-to-day operations

## Currently Active Scripts

### Core Deployment Scripts

#### [`deploy.sh`](deploy.sh) ⭐ **PRIMARY DEPLOYMENT SCRIPT**
Deployment script for all environments using the parallel processing architecture.

```bash
# Deploy to development
./scripts/deploy.sh --environment dev

# Deploy to production
./scripts/deploy.sh --environment prod --profile avesa-production
```

**Features:**
- Deploys infrastructure using [`infrastructure/app.py`](../infrastructure/app.py)
- Includes Step Functions state machines for parallel processing
- Enhanced Lambda functions with lightweight packaging
- Comprehensive monitoring and dashboards

## AWS IAM Setup Scripts for GitHub Actions

These scripts automate the setup of cross-account IAM roles and users needed for GitHub Actions production deployment with the optimized architecture.

### Quick Start

For a complete automated setup, run:

```bash
./scripts/setup-github-actions-aws.sh
```

This interactive script will guide you through the entire process.

### Individual Scripts

#### 1. Production Account Setup

[`setup-production-iam.sh`](setup-production-iam.sh) - Creates the deployment role in your production AWS account for optimized architecture deployment.

```bash
./scripts/setup-production-iam.sh \
  --prod-account-id 123456789012 \
  --dev-account-id 987654321098
```

**What it creates:**
- `GitHubActionsDeploymentRole` - Cross-account deployment role
- Trust policy allowing development account access
- PowerUserAccess + custom IAM permissions
- External ID for additional security

#### 2. Development Account Setup

[`setup-development-iam.sh`](setup-development-iam.sh) - Creates the GitHub Actions user in your development account for optimized deployments.

```bash
./scripts/setup-development-iam.sh \
  --prod-account-id 123456789012 \
  --dev-account-id 987654321098
```

**What it creates:**
- `github-actions-deployer` IAM user
- Access keys for GitHub secrets
- Policy to assume production role
- Automatic role assumption testing

#### 3. Combined Setup

[`setup-github-actions-aws.sh`](setup-github-actions-aws.sh) - Interactive script that runs both setups.

```bash
./scripts/setup-github-actions-aws.sh
```

**Features:**
- Interactive prompts for account IDs
- Validates AWS CLI configuration
- Runs both production and development setup
- Provides GitHub secrets configuration
- Creates rollback scripts

#### 4. Validation Script

[`validate-github-actions-setup.sh`](validate-github-actions-setup.sh) - Tests the complete setup.

```bash
./scripts/validate-github-actions-setup.sh
```

**What it tests:**
- Cross-account role assumption
- AWS service permissions
- CDK bootstrap status
- GitHub Actions simulation

#### 5. sts:TagSession Permission Fix

[`fix-sts-tagsession-permission.sh`](fix-sts-tagsession-permission.sh) - Fixes missing sts:TagSession permission.

```bash
./scripts/fix-sts-tagsession-permission.sh \
  --dev-account-id 123938354448 \
  --prod-account-id 987654321098
```

**What it fixes:**
- Adds missing `sts:TagSession` permission to existing policies
- Resolves "User is not authorized to perform: sts:TagSession" errors
- Updates the `AVESAAssumeProductionRole` policy
- Enables successful GitHub Actions role assumption

### Prerequisites

Before running these scripts, ensure you have:

1. **AWS CLI** installed and configured
2. **jq** installed for JSON processing
3. **Access to both AWS accounts** with appropriate permissions
4. **IAM permissions** to create roles, users, and policies

#### Installation Commands

```bash
# macOS
brew install awscli jq

# Ubuntu/Debian
sudo apt-get install awscli jq

# CentOS/RHEL
sudo yum install awscli jq
```

### Usage Examples

#### Complete Setup (Recommended)

```bash
# Interactive setup - will prompt for all required information
./scripts/setup-github-actions-aws.sh

# Non-interactive setup
./scripts/setup-github-actions-aws.sh \
  --prod-account-id 123456789012 \
  --dev-account-id 987654321098 \
  --non-interactive
```

#### Individual Component Setup

```bash
# Setup production account only
./scripts/setup-production-iam.sh \
  --prod-account-id 123456789012 \
  --dev-account-id 987654321098 \
  --profile avesa-production

# Setup development account only
./scripts/setup-development-iam.sh \
  --prod-account-id 123456789012 \
  --dev-account-id 987654321098 \
  --profile my-dev-profile

# Validate the complete setup
./scripts/validate-github-actions-setup.sh \
  --access-key-id AKIA... \
  --secret-access-key ... \
  --role-arn arn:aws:iam::123456789012:role/GitHubActionsDeploymentRole
```

### GitHub Secrets Configuration

After running the setup scripts, add these secrets to your GitHub repository:

| Secret Name | Description | Example Value |
|-------------|-------------|---------------|
| `AWS_ACCESS_KEY_ID_PROD` | Development account access key | `AKIA...` |
| `AWS_SECRET_ACCESS_KEY_PROD` | Development account secret key | `wJalrXUtnFEMI/K7MDENG...` |
| `AWS_PROD_DEPLOYMENT_ROLE_ARN` | Production deployment role ARN | `arn:aws:iam::123456789012:role/GitHubActionsDeploymentRole` |

### Security Features

#### Cross-Account Security
- **External ID**: Additional security layer for role assumption
- **Least Privilege**: Minimal required permissions
- **Account Isolation**: Development and production account separation

#### Credential Management
- **Temporary Credentials**: GitHub Actions uses temporary role credentials
- **No Long-term Production Access**: Development account cannot directly access production
- **Automatic Rotation**: Support for regular access key rotation

### Troubleshooting

#### Common Issues

**❌ "User is not authorized to perform: sts:TagSession"**
```bash
# Fix missing sts:TagSession permission
./scripts/fix-sts-tagsession-permission.sh \
  --dev-account-id YOUR_DEV_ACCOUNT \
  --prod-account-id YOUR_PROD_ACCOUNT
```

**❌ "Could not assume role"**
```bash
# Check role ARN and trust policy
aws iam get-role --role-name GitHubActionsDeploymentRole
```

**❌ "Access Denied"**
```bash
# Verify IAM permissions
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::ACCOUNT:user/github-actions-deployer \
  --action-names sts:AssumeRole sts:TagSession
```

**❌ "Invalid credentials"**
```bash
# Test basic access
aws sts get-caller-identity
```

#### Validation Commands

```bash
# Test role assumption manually
aws sts assume-role \
  --role-arn arn:aws:iam::PROD_ACCOUNT:role/GitHubActionsDeploymentRole \
  --role-session-name test-session \
  --external-id avesa-github-actions-2024

# Validate GitHub Actions setup
./scripts/validate-github-actions-setup.sh --verbose
```

### Rollback Procedures

The setup scripts create rollback scripts in `/tmp/`:

```bash
# Rollback production setup
/tmp/rollback-production-iam.sh

# Rollback development setup
/tmp/rollback-development-iam.sh
```

**Manual Rollback:**

```bash
# Remove development user
aws iam delete-access-key --user-name github-actions-deployer --access-key-id AKIA...
aws iam detach-user-policy --user-name github-actions-deployer --policy-arn arn:aws:iam::DEV_ACCOUNT:policy/AVESAAssumeProductionRole
aws iam delete-policy --policy-arn arn:aws:iam::DEV_ACCOUNT:policy/AVESAAssumeProductionRole
aws iam delete-user --user-name github-actions-deployer

# Remove production role
aws iam detach-role-policy --role-name GitHubActionsDeploymentRole --policy-arn arn:aws:iam::aws:policy/PowerUserAccess
aws iam detach-role-policy --role-name GitHubActionsDeploymentRole --policy-arn arn:aws:iam::PROD_ACCOUNT:policy/AVESADeploymentPolicy
aws iam delete-policy --policy-arn arn:aws:iam::PROD_ACCOUNT:policy/AVESADeploymentPolicy
aws iam delete-role --role-name GitHubActionsDeploymentRole
```

## Service Management Scripts

### Lambda Function Management

#### [`package-lightweight-lambdas.py`](package-lightweight-lambdas.py) ⭐ **OPTIMIZED PACKAGING**
Package optimized Lambda functions with lightweight dependencies.

```bash
# Package all optimized functions
python scripts/package-lightweight-lambdas.py --function all

# Package specific optimized function
python scripts/package-lightweight-lambdas.py --function optimized-orchestrator
python scripts/package-lightweight-lambdas.py --function optimized-processors
```

**Features:**
- 99.9% package size reduction using AWS Pandas layers
- Optimized for the new parallel processing architecture
- Supports all optimized Lambda components

### Tenant and Service Management

#### [`setup-service.py`](setup-service.py)
Configure tenant services for the optimized pipeline.

```bash
# Setup ConnectWise for a tenant (works with optimized architecture)
python scripts/setup-service.py \
  --tenant-id "example-tenant" \
  --service connectwise \
  --environment dev
```

#### [`trigger-backfill.py`](trigger-backfill.py)
Trigger data backfill for tenants using optimized processing.

```bash
# Trigger backfill for specific tenant (uses optimized pipeline)
python scripts/trigger-backfill.py \
  --tenant-id "example-tenant" \
  --environment dev
```

### Testing and Validation Scripts

#### [`test-end-to-end-pipeline.py`](test-end-to-end-pipeline.py) ⭐ **OPTIMIZED TESTING**
Test the complete optimized pipeline end-to-end.

```bash
# Test optimized pipeline
python scripts/test-end-to-end-pipeline.py --environment dev --region us-east-2
```

#### [`test-lambda-functions.py`](test-lambda-functions.py)
Test individual Lambda functions (updated for optimized architecture).

```bash
# Test optimized Lambda functions
python scripts/test-lambda-functions.py --environment dev --region us-east-2 --verbose
```

### Maintenance Scripts

#### [`cleanup-stuck-jobs.py`](cleanup-stuck-jobs.py)
Clean up stuck processing jobs in the optimized pipeline.

```bash
# Clean up stuck jobs
python scripts/cleanup-stuck-jobs.py --environment dev
```

## Script Dependencies

### Python Scripts
- Python 3.9+
- boto3
- Required packages in [`requirements.txt`](../requirements.txt)
- AWS Pandas layers (for optimized Lambda functions)

### Bash Scripts
- AWS CLI v2
- jq (JSON processor)
- Standard Unix utilities (grep, sed, awk)

## Environment Variables

### AWS Configuration
```bash
export AWS_PROFILE=your-profile
export AWS_DEFAULT_REGION=us-east-2
```

### GitHub Actions Secrets (for optimized deployments)
```bash
export AWS_ACCESS_KEY_ID_PROD=AKIA...
export AWS_SECRET_ACCESS_KEY_PROD=...
export AWS_PROD_DEPLOYMENT_ROLE_ARN=arn:aws:iam::123456789012:role/GitHubActionsDeploymentRole
```

### Optimized Architecture Variables
```bash
# For optimized CDK deployment
export CDK_DEFAULT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export CDK_PROD_ACCOUNT=your-prod-account-id
```

## Best Practices

### Security
1. **Rotate access keys** every 90 days
2. **Use least privilege** IAM policies
3. **Monitor CloudTrail** for unusual activity
4. **Store secrets securely** in GitHub only
5. **Use optimized architecture** for enhanced security

### Operations
1. **Test in development** before production using optimized pipeline
2. **Use validation scripts** before deployment
3. **Document all changes** in commit messages
4. **Monitor optimized pipeline** performance via CloudWatch dashboards

### Monitoring
1. **Enable CloudTrail** logging
2. **Set up CloudWatch** alarms for optimized components
3. **Monitor GitHub Actions** logs for optimized deployments
4. **Review IAM policies** regularly
5. **Use optimized monitoring dashboards** for real-time insights

## Support

For issues with these scripts:

1. **Check prerequisites** are installed
2. **Validate AWS credentials** and permissions
3. **Run validation script** for detailed diagnostics
4. **Review troubleshooting section** above
5. **Check project documentation** in [`docs/`](../docs/)

### Related Documentation

- [Deployment Guide (Updated)](../docs/DEPLOYMENT.md) - Optimized deployment procedures
- [Dev Environment Setup (Updated)](../docs/DEV_ENVIRONMENT_SETUP_GUIDE.md) - Optimized development setup
- [Migration Strategy (Completed)](../docs/MIGRATION_STRATEGY.md) - Migration completion details
- [Phase 1 Implementation (Completed)](../docs/PHASE_1_IMPLEMENTATION_README.md) - Implementation status
- [AWS Credentials Setup Guide](../docs/AWS_CREDENTIALS_SETUP_GUIDE.md)
- [GitHub Secrets Quick Setup](../docs/GITHUB_SECRETS_QUICK_SETUP.md)
- [sts:TagSession Permission Fix](../docs/STS_TAGSESSION_FIX.md)
- [Production Environment Setup](../docs/PROD_ENVIRONMENT_SETUP_GUIDE.md)

## Contributing

When adding new scripts:

1. **Follow naming conventions** (`kebab-case.sh` or `snake_case.py`)
2. **Include usage documentation** in script headers
3. **Add error handling** and validation
4. **Update this README** with new script information
5. **Test thoroughly** in development environment with optimized architecture
6. **Ensure compatibility** with optimized infrastructure components

### Script Categories

**✅ Active Scripts** (in `scripts/` directory):
- Deployment and infrastructure management
- Service and tenant configuration
- Testing and validation
- Maintenance and cleanup

## License

These scripts are part of the AVESA project and follow the same licensing terms.