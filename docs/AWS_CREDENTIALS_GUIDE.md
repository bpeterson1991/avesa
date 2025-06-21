# AWS Credentials Comprehensive Guide

This guide provides complete instructions for configuring AWS credentials to work with the AVESA ClickHouse servers and data pipeline, including setup methods, troubleshooting, and best practices.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Problem Overview](#problem-overview)
3. [Setup Methods](#setup-methods)
4. [Environment-Specific Configurations](#environment-specific-configurations)
5. [Required AWS Resources](#required-aws-resources)
6. [Troubleshooting Workflow](#troubleshooting-workflow)
7. [Security Best Practices](#security-best-practices)
8. [Integration Points](#integration-points)
9. [Common Commands](#common-commands)
10. [Support Resources](#support-resources)

## Quick Start

If you're getting "AWS credentials not configured" errors, run:

```bash
export AWS_PROFILE=AdministratorAccess-123938354448
./scripts/start-real-clickhouse-servers.sh
```

## Problem Overview

The AVESA ClickHouse servers require AWS credentials to:
- Access AWS Secrets Manager for ClickHouse connection credentials
- Connect to S3 for data storage
- Use other AWS services in the pipeline

## Setup Methods

### Method 1: Using AWS Profile (Recommended)

1. **Check available profiles:**
   ```bash
   aws configure list-profiles
   ```

2. **Set the AWS profile:**
   ```bash
   export AWS_PROFILE=AdministratorAccess-123938354448
   ```

3. **Make it permanent (add to your shell profile):**
   ```bash
   echo 'export AWS_PROFILE=AdministratorAccess-123938354448' >> ~/.zshrc
   source ~/.zshrc
   ```

### Method 2: AWS SSO Login

If using AWS SSO (Single Sign-On):

1. **Login to AWS SSO:**
   ```bash
   aws sso login --profile AdministratorAccess-123938354448
   ```

2. **Set the profile:**
   ```bash
   export AWS_PROFILE=AdministratorAccess-123938354448
   ```

### Method 3: Configure New Profile

If the required profile doesn't exist:

1. **Configure the profile:**
   ```bash
   aws configure --profile AdministratorAccess-123938354448
   ```

2. **Enter your credentials when prompted:**
   - AWS Access Key ID
   - AWS Secret Access Key
   - Default region: `us-east-2`
   - Default output format: `json`

### Method 4: Environment Variables

Set AWS credentials directly as environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_SESSION_TOKEN=your_session_token  # If using temporary credentials
export AWS_REGION=us-east-2
```

## Environment-Specific Configurations

### Development Environment
```bash
export AWS_PROFILE=AdministratorAccess-123938354448
export AWS_REGION=us-east-2
export AWS_SDK_LOAD_CONFIG=1
```

### Production Environment
```bash
export AWS_PROFILE=avesa-production
export AWS_REGION=us-east-2
export AWS_SDK_LOAD_CONFIG=1
```

## Required AWS Resources

### Secrets Manager
- **Secret Name:** `arn:aws:secretsmanager:us-east-2:123938354448:secret:clickhouse-connection-dev-V9zSgO`
- **Contains:** ClickHouse connection credentials
- **Required Permissions:** `secretsmanager:GetSecretValue`

### S3 Buckets
- **Development:** `data-storage-msp-dev`
- **Production:** `data-storage-msp-prod`
- **Required Permissions:** `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`

### IAM Roles
- **Development:** `AdministratorAccess`
- **Production:** `avesa-production-role`

## Troubleshooting Workflow

### Step 1: Run Diagnostic Script

```bash
./scripts/diagnose-aws-credentials.sh
```

### Step 2: Check Current Status

```bash
aws sts get-caller-identity
aws configure list-profiles
```

### Step 3: Fix Issues

```bash
./scripts/setup-aws-credentials.sh
```

### Step 4: Verify Fix

```bash
./scripts/start-real-clickhouse-servers.sh
```

## Problem Categories

### 1. "Unable to locate credentials"
**Symptoms:** AWS CLI commands fail with credential errors
**Solutions:**
- Run: `./scripts/setup-aws-credentials.sh`
- Or manually: `export AWS_PROFILE=AdministratorAccess-123938354448`

### 2. "The security token included in the request is invalid"
**Symptoms:** Expired SSO session or invalid credentials
**Solutions:**
- For SSO: `aws sso login --profile AdministratorAccess-123938354448`
- For regular credentials: `aws configure --profile AdministratorAccess-123938354448`

### 3. "Access Denied" for Secrets Manager
**Symptoms:** Cannot access ClickHouse connection secrets
**Solutions:**
- Verify correct profile: `AdministratorAccess-123938354448`
- Check account: Should be `123938354448`
- Contact AWS administrator for permissions

### 4. "Profile not found"
**Symptoms:** Required AWS profile doesn't exist
**Solutions:**
- Run setup script: `./scripts/setup-aws-credentials.sh`
- Manual configuration: `aws configure --profile AdministratorAccess-123938354448`

## Verification

Test your credentials:

```bash
aws sts get-caller-identity
```

Expected output:
```json
{
    "UserId": "AROARZW2NGUILM4HUFEFO:username",
    "Account": "123938354448",
    "Arn": "arn:aws:sts::123938354448:assumed-role/AWSReservedSSO_AdministratorAccess_171462d65862921c/username"
}
```

## Security Best Practices

### ✅ Recommended
- Use AWS SSO when available
- Set up MFA on AWS accounts
- Use IAM roles instead of long-term access keys
- Rotate credentials regularly
- Use least privilege principle

### ❌ Avoid
- Hardcoding credentials in code
- Sharing credentials via email/chat
- Using root account credentials
- Long-term access keys in production
- Overly broad permissions

## Integration Points

### ClickHouse API Server
- **File:** `src/clickhouse/api/config/clickhouse-dev-real.js`
- **Uses:** AWS SDK to fetch secrets from Secrets Manager
- **Requires:** `AWS_PROFILE` or credential environment variables

### Lambda Functions
- **Files:** Various Lambda functions in `src/`
- **Uses:** IAM roles for execution
- **Requires:** Proper role permissions

### Infrastructure Deployment
- **File:** `infrastructure/app.py`
- **Uses:** AWS CDK with configured credentials
- **Requires:** Administrative permissions

## Common Commands

### Credential Management
```bash
# List profiles
aws configure list-profiles

# Check current identity
aws sts get-caller-identity

# Set profile
export AWS_PROFILE=AdministratorAccess-123938354448

# SSO login
aws sso login --profile AdministratorAccess-123938354448

# Configure new profile
aws configure --profile AdministratorAccess-123938354448
```

### Testing
```bash
# Test Secrets Manager access
aws secretsmanager describe-secret \
  --secret-id arn:aws:secretsmanager:us-east-2:123938354448:secret:clickhouse-connection-dev-V9zSgO \
  --region us-east-2

# Test S3 access
aws s3 ls s3://data-storage-msp-dev/

# Test with specific profile
AWS_PROFILE=AdministratorAccess-123938354448 aws sts get-caller-identity
```

## Required Permissions

Your AWS credentials need access to:
- **Secrets Manager:** `secretsmanager:GetSecretValue`
- **S3:** `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`
- **STS:** `sts:GetCallerIdentity` (for credential verification)

## Integration with ClickHouse

The ClickHouse API servers use these credentials to:

1. **Fetch connection credentials** from AWS Secrets Manager:
   - Secret: `arn:aws:secretsmanager:us-east-2:123938354448:secret:clickhouse-connection-dev-V9zSgO`

2. **Access data** stored in S3 buckets

3. **Log and monitor** through CloudWatch

## Automated Setup Script

For convenience, you can also run:

```bash
./scripts/setup-aws-credentials.sh
```

This script will:
- Check current credential status
- Guide you through the setup process
- Verify the configuration works
- Set up environment variables

## Troubleshooting Common Errors

### Error: "Unable to locate credentials"

**Cause:** No AWS credentials configured
**Solution:** Follow Method 1 or 2 above

### Error: "The security token included in the request is invalid"

**Cause:** Expired SSO session or invalid credentials
**Solutions:**
- For SSO: `aws sso login --profile AdministratorAccess-123938354448`
- For regular credentials: `aws configure --profile AdministratorAccess-123938354448`

### Error: "Access Denied" when accessing Secrets Manager

**Cause:** Insufficient permissions
**Solutions:**
- Verify you're using the correct profile: `AdministratorAccess-123938354448`
- Contact your AWS administrator for proper permissions

### Error: "Profile not found"

**Cause:** The required AWS profile doesn't exist
**Solution:** Configure the profile using Method 3 above

## Support Resources

### Internal Documentation
- [FULLSTACK_README.md](../FULLSTACK_README.md) - Complete project overview
- [scripts/README.md](../scripts/README.md) - Scripts documentation
- [CLICKHOUSE_GUIDE.md](./CLICKHOUSE_GUIDE.md) - ClickHouse implementation guide

### External Resources
- [AWS CLI Configuration Guide](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)
- [AWS SSO Configuration](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html)
- [AWS SDK for JavaScript](https://docs.aws.amazon.com/sdk-for-javascript/)

## Contact Information

For additional support:
- Check the diagnostic output from `./scripts/diagnose-aws-credentials.sh`
- Review AWS CloudTrail logs for authentication errors
- Contact the DevOps team with specific error messages

---

**Last Updated:** December 2024
**Maintained By:** AVESA DevOps Team