# AWS IAM sts:TagSession Permission Fix

## Problem Description

The GitHub Actions workflow fails at step 7 (role assumption) with the following error:

```
User: arn:aws:iam::123938354448:user/github-actions/github-actions-deployer is not authorized to perform: sts:TagSession
```

This error occurs because the IAM policy for the `github-actions-deployer` user in the development account is missing the `sts:TagSession` permission, which is required for role assumption in newer AWS configurations.

## Root Cause

The `AVESAAssumeProductionRole` policy attached to the `github-actions-deployer` user only includes the `sts:AssumeRole` permission but is missing the `sts:TagSession` permission that AWS now requires for role assumption operations.

## Solution

### Quick Fix (For Existing Setups)

Run the fix script to add the missing permission to your existing policy:

```bash
./scripts/fix-sts-tagsession-permission.sh \
  --dev-account-id 123938354448 \
  --prod-account-id YOUR_PROD_ACCOUNT_ID
```

**Example with AWS profile:**
```bash
./scripts/fix-sts-tagsession-permission.sh \
  --dev-account-id 123938354448 \
  --prod-account-id 987654321098 \
  --profile my-dev-profile
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

## Related Documentation

- [AWS Credentials Setup Guide](AWS_CREDENTIALS_SETUP_GUIDE.md)
- [GitHub Secrets Quick Setup](GITHUB_SECRETS_QUICK_SETUP.md)
- [Deployment Guide](DEPLOYMENT.md)