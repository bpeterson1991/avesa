# AWS IAM sts:TagSession Permission Fix

## Problem Description

The GitHub Actions workflow fails at step 7 (role assumption) with the following error:

```
User: arn:aws:iam::123938354448:user/github-actions/github-actions-deployer is not authorized to perform: sts:TagSession
```

This error occurs because the IAM policy for the `github-actions-deployer` user in the development account is missing the `sts:TagSession` permission, which is required for role assumption in newer AWS configurations, particularly when using the `aws-actions/configure-aws-credentials@v4` action.

## Root Cause

The `AVESAAssumeProductionRole` policy attached to the `github-actions-deployer` user only includes the `sts:AssumeRole` permission but is missing the `sts:TagSession` permission that AWS now requires for role assumption operations. This is a requirement introduced by newer versions of the AWS credentials action that automatically tags role sessions.

## Enhanced Solution

### Comprehensive Fix (Recommended)

Use the enhanced fix script that provides multiple approaches to resolve the issue:

```bash
./scripts/fix-github-actions-sts-tagsession.sh \
  --dev-account-id 123938354448 \
  --prod-account-id YOUR_PROD_ACCOUNT_ID \
  --approach comprehensive
```

**Available approaches:**
- `comprehensive` - Updates IAM policy + creates workflow alternatives (recommended)
- `minimal` - Only adds sts:TagSession to existing policy
- `workflow-only` - Creates alternative workflow without session tagging

**Example with AWS profile:**
```bash
./scripts/fix-github-actions-sts-tagsession.sh \
  --dev-account-id 123938354448 \
  --prod-account-id 987654321098 \
  --profile my-dev-profile \
  --approach comprehensive
```

### Quick Fix (For Existing Setups)

Run the original fix script to add the missing permission to your existing policy:

```bash
./scripts/fix-sts-tagsession-permission.sh \
  --dev-account-id 123938354448 \
  --prod-account-id YOUR_PROD_ACCOUNT_ID
```

### For New Setups

The development IAM setup script has been updated to include both permissions by default:

```bash
./scripts/setup-development-iam.sh \
  --prod-account-id YOUR_PROD_ACCOUNT_ID \
  --dev-account-id YOUR_DEV_ACCOUNT_ID
```

## What the Fix Does

The fix script:

1. **Validates Prerequisites**: Ensures AWS CLI, jq, and proper credentials are configured
2. **Checks Current Policy**: Verifies the existing policy and identifies missing permissions
3. **Updates Policy**: Creates a new policy version with both `sts:AssumeRole` and `sts:TagSession` permissions
4. **Tests Configuration**: Validates that the policy is correctly attached

### Updated Policy Structure

**Before (Missing sts:TagSession):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": "arn:aws:iam::PROD_ACCOUNT:role/GitHubActionsDeploymentRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "avesa-github-actions-2024"
        }
      }
    }
  ]
}
```

**After (Includes sts:TagSession):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sts:AssumeRole",
        "sts:TagSession"
      ],
      "Resource": "arn:aws:iam::PROD_ACCOUNT:role/GitHubActionsDeploymentRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "avesa-github-actions-2024"
        }
      }
    }
  ]
}
```

## Verification

After running the fix:

1. **Test GitHub Actions Workflow**: Trigger the "Deploy to Production" workflow manually
2. **Check Step 7**: The "Configure AWS credentials for production" step should now pass
3. **Run Validation Script**: Use the validation script to confirm the setup:
   ```bash
   ./scripts/validate-github-actions-setup.sh
   ```

## Security Notes

- The `sts:TagSession` permission allows tagging of the assumed role session
- This is a standard AWS requirement and does not introduce additional security risks
- The permission is scoped to the specific production role resource
- The external ID condition provides additional security

## Troubleshooting

### Common Issues

1. **Wrong AWS Account**: Ensure you're running the fix script with credentials for the development account
2. **Missing Policy**: If the policy doesn't exist, run the full development setup script first
3. **Permission Denied**: Ensure your AWS user has permissions to modify IAM policies

### Verification Commands

Check if the fix was applied correctly:

```bash
# Get the current policy document
aws iam get-policy-version \
  --policy-arn "arn:aws:iam::YOUR_DEV_ACCOUNT:policy/AVESAAssumeProductionRole" \
  --version-id $(aws iam get-policy --policy-arn "arn:aws:iam::YOUR_DEV_ACCOUNT:policy/AVESAAssumeProductionRole" --query 'Policy.DefaultVersionId' --output text)

# Check if both permissions are present
aws iam get-policy-version \
  --policy-arn "arn:aws:iam::YOUR_DEV_ACCOUNT:policy/AVESAAssumeProductionRole" \
  --version-id $(aws iam get-policy --policy-arn "arn:aws:iam::YOUR_DEV_ACCOUNT:policy/AVESAAssumeProductionRole" --query 'Policy.DefaultVersionId' --output text) \
  --query 'PolicyVersion.Document.Statement[0].Action'
```

## Alternative Solutions

### Option 1: Unified Workflow with Manual Role Assumption (Implemented)

The main deployment workflow has been updated to use manual role assumption that bypasses session tagging entirely:

**File:** `.github/workflows/deploy-production.yml`

This workflow:
- Uses manual role assumption instead of the aws-actions/configure-aws-credentials action
- Avoids session tagging completely
- Provides the same functionality as the original workflow
- Can be used immediately without IAM policy changes

**Usage:**
1. Run the enhanced fix script with any approach
2. The main workflow now uses manual role assumption to avoid sts:TagSession errors
3. Use the standard "Deploy to Production" workflow which now works without session tagging issues

### Option 2: Workflow Configuration Patch

For existing workflows, you can replace the credential configuration step:

```yaml
- name: Configure AWS credentials for production (Patched)
  run: |
    # Alternative credential configuration without session tagging
    echo "Setting up AWS credentials without session tagging..."
    
    # Configure base credentials
    aws configure set aws_access_key_id "${{ secrets.AWS_ACCESS_KEY_ID_PROD }}"
    aws configure set aws_secret_access_key "${{ secrets.AWS_SECRET_ACCESS_KEY_PROD }}"
    aws configure set region "${{ env.AWS_REGION }}"
    
    # Assume role manually without session tagging
    ROLE_CREDS=$(aws sts assume-role \
      --role-arn "${{ secrets.AWS_PROD_DEPLOYMENT_ROLE_ARN }}" \
      --role-session-name "GitHubActions-ProductionDeployment-$(date +%s)" \
      --external-id "avesa-github-actions-2024" \
      --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
      --output text)
    
    # Set environment variables for subsequent steps
    AWS_ACCESS_KEY_ID=$(echo $ROLE_CREDS | cut -d' ' -f1)
    AWS_SECRET_ACCESS_KEY=$(echo $ROLE_CREDS | cut -d' ' -f2)
    AWS_SESSION_TOKEN=$(echo $ROLE_CREDS | cut -d' ' -f3)
    
    echo "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" >> $GITHUB_ENV
    echo "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" >> $GITHUB_ENV
    echo "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN" >> $GITHUB_ENV
```

### Option 3: Enhanced IAM Policy

The comprehensive fix creates an enhanced IAM policy with broader sts:TagSession permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sts:AssumeRole",
        "sts:TagSession"
      ],
      "Resource": "arn:aws:iam::PROD_ACCOUNT:role/GitHubActionsDeploymentRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "avesa-github-actions-2024"
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
```

This approach provides broader sts:TagSession permissions while maintaining security through region restrictions.

## Troubleshooting Persistent Issues

### If the Fix Still Doesn't Work

1. **Use the unified workflow** - The main workflow now bypasses the IAM issue entirely
2. **Check AWS CLI version** - Ensure you're using a recent version
3. **Verify GitHub secrets** - Ensure all secrets are correctly configured
4. **Test locally** - Use the validation script to test credentials

### Manual Policy Update Commands

If automated scripts fail, use these direct AWS CLI commands:

```bash
# Set your account IDs
DEV_ACCOUNT_ID="123938354448"
PROD_ACCOUNT_ID="987654321098"
POLICY_NAME="AVESAAssumeProductionRole"

# Create updated policy document
cat > /tmp/updated-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sts:AssumeRole",
        "sts:TagSession"
      ],
      "Resource": "arn:aws:iam::${PROD_ACCOUNT_ID}:role/GitHubActionsDeploymentRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "avesa-github-actions-2024"
        }
      }
    }
  ]
}
EOF

# Update the policy
POLICY_ARN="arn:aws:iam::${DEV_ACCOUNT_ID}:policy/${POLICY_NAME}"
aws iam create-policy-version \
    --policy-arn "$POLICY_ARN" \
    --policy-document file:///tmp/updated-policy.json \
    --set-as-default

# Verify the update
aws iam get-policy-version \
    --policy-arn "$POLICY_ARN" \
    --version-id $(aws iam get-policy --policy-arn "$POLICY_ARN" --query 'Policy.DefaultVersionId' --output text) \
    --query 'PolicyVersion.Document.Statement[0].Action'
```

## Testing Your Fix

### Method 1: Use the Alternative Workflow

1. Go to GitHub Actions → "Deploy to Production"
2. Run workflow with test parameters:
   - Deployment confirmation: `DEPLOY TO PRODUCTION`
   - Environment target: `production`
   - Deployment reason: `Testing sts:TagSession fix`
   - Components: `infrastructure-only`

### Method 2: Test the Original Workflow

1. Go to GitHub Actions → "Deploy to Production"
2. Run workflow with the same test parameters
3. Monitor step 7 (Configure AWS credentials) - it should now pass

### Method 3: Use the Validation Script

```bash
./scripts/validate-github-actions-setup.sh \
  --access-key-id YOUR_ACCESS_KEY \
  --secret-access-key YOUR_SECRET_KEY \
  --role-arn YOUR_ROLE_ARN
```

## Related Documentation

- [AWS Credentials Setup Guide](AWS_CREDENTIALS_SETUP_GUIDE.md)
- [GitHub Secrets Quick Setup](GITHUB_SECRETS_QUICK_SETUP.md)
- [Deployment Guide](DEPLOYMENT.md)