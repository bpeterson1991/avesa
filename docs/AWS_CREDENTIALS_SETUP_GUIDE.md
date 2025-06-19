# AWS Credentials Setup Guide for GitHub Actions

This guide provides comprehensive instructions for setting up AWS credentials in GitHub repository secrets to enable successful production deployments through GitHub Actions.

## Overview

The AVESA production deployment workflow requires specific AWS credentials configured as GitHub repository secrets. This guide covers the complete setup process, security best practices, and troubleshooting steps.

## Required GitHub Secrets

Based on the [`deploy-production.yml`](.github/workflows/deploy-production.yml:134) workflow, the following secrets must be configured:

### Core AWS Credentials
- **`AWS_ACCESS_KEY_ID_PROD`** - AWS Access Key ID for production account
- **`AWS_SECRET_ACCESS_KEY_PROD`** - AWS Secret Access Key for production account
- **`AWS_PROD_DEPLOYMENT_ROLE_ARN`** - ARN of the deployment role to assume

### Optional Notification Secrets
- **`SLACK_WEBHOOK_URL`** - Slack webhook for deployment notifications (currently commented out)

## AWS Credential Configuration Options

### Option 1: IAM User with Cross-Account Role (Recommended)

This approach uses an IAM user in a development/management account that assumes a deployment role in the production account.

#### Step 1: Create Production Deployment Role

In your **production AWS account**:

```bash
# Set your production account ID
export PROD_ACCOUNT_ID="123456789012"  # Replace with actual production account ID
export DEV_ACCOUNT_ID="987654321098"   # Replace with development/management account ID

# Create the deployment role
aws iam create-role \
  --role-name AVESAProductionDeploymentRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "AWS": "arn:aws:iam::'$DEV_ACCOUNT_ID':root"
        },
        "Action": "sts:AssumeRole",
        "Condition": {
          "StringEquals": {
            "sts:ExternalId": "avesa-github-actions-2024"
          }
        }
      }
    ]
  }'

# Attach necessary policies for CDK deployment
aws iam attach-role-policy \
  --role-name AVESAProductionDeploymentRole \
  --policy-arn arn:aws:iam::aws:policy/PowerUserAccess

# Create custom policy for additional permissions
aws iam create-policy \
  --policy-name AVESADeploymentPolicy \
  --policy-document '{
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
          "iam:UntagRole"
        ],
        "Resource": "arn:aws:iam::'$PROD_ACCOUNT_ID':role/AVESA*"
      }
    ]
  }'

# Attach the custom policy
aws iam attach-role-policy \
  --role-name AVESAProductionDeploymentRole \
  --policy-arn arn:aws:iam::'$PROD_ACCOUNT_ID':policy/AVESADeploymentPolicy
```

#### Step 2: Create IAM User in Development Account

In your **development/management AWS account**:

```bash
# Create IAM user for GitHub Actions
aws iam create-user \
  --user-name avesa-github-actions

# Create access keys
aws iam create-access-key \
  --user-name avesa-github-actions

# Create policy to assume production role
aws iam create-policy \
  --policy-name AVESAAssumeProductionRole \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": "sts:AssumeRole",
        "Resource": "arn:aws:iam::'$PROD_ACCOUNT_ID':role/AVESAProductionDeploymentRole"
      }
    ]
  }'

# Attach policy to user
aws iam attach-user-policy \
  --user-name avesa-github-actions \
  --policy-arn arn:aws:iam::'$DEV_ACCOUNT_ID':policy/AVESAAssumeProductionRole
```

### Option 2: Direct Production Account Access

This approach uses an IAM user directly in the production account (less secure but simpler).

#### Create IAM User in Production Account

```bash
# Create IAM user for GitHub Actions
aws iam create-user \
  --user-name avesa-github-actions-prod

# Create access keys
aws iam create-access-key \
  --user-name avesa-github-actions-prod

# Attach necessary policies
aws iam attach-user-policy \
  --user-name avesa-github-actions-prod \
  --policy-arn arn:aws:iam::aws:policy/PowerUserAccess

# Create and attach custom IAM policy
aws iam create-policy \
  --policy-name AVESADirectDeploymentPolicy \
  --policy-document '{
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
          "iam:UntagRole"
        ],
        "Resource": "*"
      }
    ]
  }'

aws iam attach-user-policy \
  --user-name avesa-github-actions-prod \
  --policy-arn arn:aws:iam::'$PROD_ACCOUNT_ID':policy/AVESADirectDeploymentPolicy
```

## GitHub Secrets Configuration

### Step 1: Navigate to Repository Settings

1. Go to your GitHub repository
2. Click **Settings** tab
3. In the left sidebar, click **Secrets and variables**
4. Click **Actions**

### Step 2: Add Required Secrets

#### For Cross-Account Role Setup (Option 1):

Click **New repository secret** and add each of the following:

**Secret Name:** `AWS_ACCESS_KEY_ID_PROD`
**Secret Value:** `AKIA...` (Access Key ID from development account IAM user)

**Secret Name:** `AWS_SECRET_ACCESS_KEY_PROD`
**Secret Value:** `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` (Secret Access Key from development account)

**Secret Name:** `AWS_PROD_DEPLOYMENT_ROLE_ARN`
**Secret Value:** `arn:aws:iam::123456789012:role/AVESAProductionDeploymentRole`

#### For Direct Production Access (Option 2):

**Secret Name:** `AWS_ACCESS_KEY_ID_PROD`
**Secret Value:** `AKIA...` (Access Key ID from production account IAM user)

**Secret Name:** `AWS_SECRET_ACCESS_KEY_PROD`
**Secret Value:** `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` (Secret Access Key from production account)

**Secret Name:** `AWS_PROD_DEPLOYMENT_ROLE_ARN`
**Secret Value:** `arn:aws:iam::123456789012:role/AVESAProductionDeploymentRole` (Optional - can be empty for direct access)

### Step 3: Verify Secret Configuration

After adding secrets, you should see them listed in the repository secrets section:

```
AWS_ACCESS_KEY_ID_PROD          ••••••••••••••••••••
AWS_SECRET_ACCESS_KEY_PROD      ••••••••••••••••••••
AWS_PROD_DEPLOYMENT_ROLE_ARN    ••••••••••••••••••••
```

## Required AWS Permissions

The deployment workflow requires the following AWS permissions:

### Core Services
- **AWS CDK**: Bootstrap and deploy CloudFormation stacks
- **Lambda**: Create, update, and invoke functions
- **DynamoDB**: Create and manage tables
- **S3**: Create buckets and upload objects
- **IAM**: Create and manage service roles
- **CloudWatch**: Create dashboards and alarms
- **Secrets Manager**: Store and retrieve secrets

### Specific IAM Policies Required

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "cloudformation:*",
        "lambda:*",
        "dynamodb:*",
        "s3:*",
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
        "cloudwatch:*",
        "logs:*",
        "secretsmanager:*",
        "sts:GetCallerIdentity"
      ],
      "Resource": "*"
    }
  ]
}
```

## Security Best Practices

### 1. Least Privilege Access

- Use cross-account roles instead of direct production access when possible
- Limit IAM permissions to only what's needed for deployment
- Use resource-based restrictions where applicable

### 2. Credential Rotation

```bash
# Rotate access keys regularly (every 90 days)
aws iam create-access-key --user-name avesa-github-actions
aws iam update-access-key --user-name avesa-github-actions --access-key-id OLD_KEY_ID --status Inactive
aws iam delete-access-key --user-name avesa-github-actions --access-key-id OLD_KEY_ID
```

### 3. Monitoring and Auditing

- Enable CloudTrail logging for all API calls
- Monitor GitHub Actions logs for credential usage
- Set up alerts for unusual AWS API activity

### 4. Environment Protection

Configure GitHub environment protection rules:

1. Go to **Settings** → **Environments**
2. Click **New environment** or edit **production**
3. Add protection rules:
   - **Required reviewers**: Add team members who must approve deployments
   - **Wait timer**: Add delay before deployment starts
   - **Deployment branches**: Restrict to specific branches

## Testing AWS Credentials

### Step 1: Test Credential Access

Create a simple test workflow to verify credentials work:

```yaml
name: Test AWS Credentials
on:
  workflow_dispatch:

jobs:
  test-credentials:
    runs-on: ubuntu-latest
    steps:
    - name: Configure AWS credentials
      uses: aws-actions/configure-aws-credentials@v4
      with:
        aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID_PROD }}
        aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY_PROD }}
        aws-region: us-east-2
        role-to-assume: ${{ secrets.AWS_PROD_DEPLOYMENT_ROLE_ARN }}
        role-duration-seconds: 3600
        role-session-name: GitHubActions-CredentialTest

    - name: Test AWS access
      run: |
        aws sts get-caller-identity
        aws s3 ls
        aws lambda list-functions --max-items 5
```

### Step 2: Verify Role Assumption

If using cross-account roles, test the assumption:

```bash
# Test role assumption locally
aws sts assume-role \
  --role-arn arn:aws:iam::PROD_ACCOUNT_ID:role/AVESAProductionDeploymentRole \
  --role-session-name test-session \
  --profile your-dev-profile

# Verify assumed role identity
aws sts get-caller-identity --profile assumed-role-profile
```

## Troubleshooting Guide

### Common Issues and Solutions

#### ❌ "Error: Could not assume role"

**Symptoms:**
```
Error: Could not assume role with OIDC: Not authorized to perform sts:AssumeRole
```

**Causes & Solutions:**
1. **Incorrect Role ARN**: Verify the role ARN in `AWS_PROD_DEPLOYMENT_ROLE_ARN` secret
2. **Trust Policy Issues**: Ensure the role trust policy allows the source account
3. **Missing External ID**: Add external ID condition if required

**Fix:**
```bash
# Update role trust policy
aws iam update-assume-role-policy \
  --role-name AVESAProductionDeploymentRole \
  --policy-document file://trust-policy.json
```

#### ❌ "Access Denied" Errors

**Symptoms:**
```
User: arn:aws:iam::123456789012:user/avesa-github-actions is not authorized to perform: lambda:CreateFunction
```

**Causes & Solutions:**
1. **Insufficient Permissions**: The IAM user/role lacks required permissions
2. **Resource Restrictions**: Policies may be too restrictive

**Fix:**
```bash
# Add missing permissions
aws iam attach-user-policy \
  --user-name avesa-github-actions \
  --policy-arn arn:aws:iam::aws:policy/AWSLambda_FullAccess
```

#### ❌ "Invalid AWS Credentials"

**Symptoms:**
```
Error: The security token included in the request is invalid
```

**Causes & Solutions:**
1. **Expired Credentials**: Access keys may be expired or deactivated
2. **Incorrect Secrets**: Wrong values in GitHub secrets
3. **Account Mismatch**: Credentials from wrong AWS account

**Fix:**
1. Regenerate access keys in AWS Console
2. Update GitHub secrets with new values
3. Verify account IDs match expected values

#### ❌ "CDK Bootstrap Required"

**Symptoms:**
```
Error: This stack uses assets, so the toolkit stack must be deployed to the environment
```

**Solution:**
The deployment script handles CDK bootstrapping automatically, but you can manually bootstrap:

```bash
# Bootstrap CDK in production account
cdk bootstrap aws://PROD_ACCOUNT_ID/us-east-2 \
  --profile avesa-production
```

### Debugging Steps

#### 1. Verify Secret Values

Check that GitHub secrets contain correct values:
- Access Key ID should start with `AKIA`
- Secret Access Key should be 40 characters
- Role ARN should follow format: `arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME`

#### 2. Test Credentials Locally

```bash
# Test with AWS CLI
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
aws sts get-caller-identity

# Test role assumption
aws sts assume-role \
  --role-arn "your-role-arn" \
  --role-session-name "test-session"
```

#### 3. Check AWS CloudTrail

Review CloudTrail logs for authentication failures:
```bash
aws logs filter-log-events \
  --log-group-name CloudTrail/ManagementEvents \
  --filter-pattern "{ $.errorCode = AccessDenied || $.errorCode = UnauthorizedOperation }" \
  --start-time $(date -d '1 hour ago' +%s)000
```

#### 4. Validate IAM Policies

Use IAM Policy Simulator to test permissions:
```bash
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::ACCOUNT_ID:user/avesa-github-actions \
  --action-names lambda:CreateFunction,s3:CreateBucket,dynamodb:CreateTable
```

## Advanced Configuration

### Using OpenID Connect (OIDC) Provider

For enhanced security, consider using GitHub's OIDC provider instead of long-lived access keys:

#### Step 1: Create OIDC Provider

```bash
# Create OIDC identity provider
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
```

#### Step 2: Update Role Trust Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:YOUR_ORG/YOUR_REPO:*"
        }
      }
    }
  ]
}
```

#### Step 3: Update GitHub Workflow

```yaml
- name: Configure AWS credentials
  uses: aws-actions/configure-aws-credentials@v4
  with:
    role-to-assume: ${{ secrets.AWS_PROD_DEPLOYMENT_ROLE_ARN }}
    role-session-name: GitHubActions-ProductionDeployment
    aws-region: us-east-2
```

### Cross-Account Monitoring Setup

For monitoring production from development account:

```bash
# Create monitoring role in production account
aws iam create-role \
  --role-name AVESACrossAccountMonitoring \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "AWS": "arn:aws:iam::DEV_ACCOUNT_ID:root"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  }'

# Attach read-only monitoring permissions
aws iam attach-role-policy \
  --role-name AVESACrossAccountMonitoring \
  --policy-arn arn:aws:iam::aws:policy/CloudWatchReadOnlyAccess
```

## Maintenance and Updates

### Regular Maintenance Tasks

#### Monthly Security Review
- Rotate access keys
- Review IAM permissions
- Check CloudTrail logs for unusual activity
- Update GitHub secrets if needed

#### Quarterly Access Audit
- Review who has access to GitHub repository
- Validate AWS IAM policies are still appropriate
- Test disaster recovery procedures
- Update documentation

### Updating Credentials

When updating AWS credentials:

1. **Create new access keys** before deleting old ones
2. **Update GitHub secrets** with new values
3. **Test deployment workflow** to ensure it works
4. **Delete old access keys** only after confirming new ones work
5. **Document the change** in your security log

## Support and Escalation

### When to Escalate

Contact your DevOps or Security team if you encounter:
- Persistent authentication failures
- Suspected credential compromise
- Need for additional AWS permissions
- Questions about security best practices

### Emergency Procedures

If credentials are compromised:

1. **Immediately disable** the compromised access keys in AWS Console
2. **Rotate all related credentials** (access keys, role trust policies)
3. **Review CloudTrail logs** for unauthorized activity
4. **Update GitHub secrets** with new credentials
5. **Document the incident** and lessons learned

## Related Documentation

- [`DEPLOYMENT.md`](DEPLOYMENT.md) - Complete deployment procedures
- [`PROD_ENVIRONMENT_SETUP_GUIDE.md`](PROD_ENVIRONMENT_SETUP_GUIDE.md) - Production environment setup
- [`MANUAL_DEPLOYMENT_GUIDE.md`](MANUAL_DEPLOYMENT_GUIDE.md) - Manual deployment process
- [AWS IAM Best Practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
- [GitHub Actions Security](https://docs.github.com/en/actions/security-guides)

## Conclusion

Proper AWS credential setup is critical for secure and reliable production deployments. Follow this guide carefully, implement security best practices, and maintain regular credential rotation to ensure your deployment pipeline remains secure and functional.

For additional support or questions about AWS credential setup, refer to the troubleshooting section above or contact your DevOps team.