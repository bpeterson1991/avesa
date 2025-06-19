# GitHub Secrets Quick Setup Guide

This is a condensed guide for repository administrators who need to quickly set up AWS credentials for the AVESA production deployment workflow.

## Required Secrets

Navigate to **Repository Settings** → **Secrets and variables** → **Actions** and add these secrets:

| Secret Name | Description | Example Value |
|-------------|-------------|---------------|
| `AWS_ACCESS_KEY_ID_PROD` | AWS Access Key ID | `AKIA...` |
| `AWS_SECRET_ACCESS_KEY_PROD` | AWS Secret Access Key | `wJalrXUtnFEMI/K7MDENG...` |
| `AWS_PROD_DEPLOYMENT_ROLE_ARN` | Production deployment role ARN | `arn:aws:iam::123456789012:role/AVESAProductionDeploymentRole` |

## Quick AWS Setup

### Option 1: Cross-Account Role (Recommended)

**In Production Account:**
```bash
# Create deployment role
aws iam create-role \
  --role-name AVESAProductionDeploymentRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"AWS": "arn:aws:iam::DEV_ACCOUNT_ID:root"},
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach permissions
aws iam attach-role-policy \
  --role-name AVESAProductionDeploymentRole \
  --policy-arn arn:aws:iam::aws:policy/PowerUserAccess
```

**In Development Account:**
```bash
# Create GitHub Actions user
aws iam create-user --user-name avesa-github-actions
aws iam create-access-key --user-name avesa-github-actions

# Allow role assumption
aws iam attach-user-policy \
  --user-name avesa-github-actions \
  --policy-arn arn:aws:iam::aws:policy/SecurityAudit
```

### Option 2: Direct Production Access

**In Production Account:**
```bash
# Create user with deployment permissions
aws iam create-user --user-name avesa-github-actions-prod
aws iam create-access-key --user-name avesa-github-actions-prod
aws iam attach-user-policy \
  --user-name avesa-github-actions-prod \
  --policy-arn arn:aws:iam::aws:policy/PowerUserAccess
```

## Verification

Test the setup by running the production deployment workflow:

1. Go to **Actions** → **Deploy to Production**
2. Click **Run workflow**
3. Fill in required parameters:
   - Deployment confirmation: `DEPLOY TO PRODUCTION`
   - Environment target: `production`
   - Deployment reason: `Testing AWS credentials setup`
   - Components: `infrastructure-only`

## Troubleshooting

| Error | Solution |
|-------|----------|
| "Could not assume role" | Check role ARN and trust policy |
| "Access Denied" | Verify IAM permissions |
| "Invalid credentials" | Regenerate access keys |

## Security Checklist

- [ ] Use cross-account roles when possible
- [ ] Limit permissions to minimum required
- [ ] Enable CloudTrail logging
- [ ] Set up credential rotation schedule
- [ ] Configure GitHub environment protection

## Next Steps

After successful credential setup:

1. Review the complete [AWS Credentials Setup Guide](AWS_CREDENTIALS_SETUP_GUIDE.md)
2. Test a full deployment following the [Manual Deployment Guide](MANUAL_DEPLOYMENT_GUIDE.md)
3. Set up monitoring and alerts per [Production Environment Setup Guide](PROD_ENVIRONMENT_SETUP_GUIDE.md)

## Support

For detailed instructions and troubleshooting, see:
- [AWS Credentials Setup Guide](AWS_CREDENTIALS_SETUP_GUIDE.md) - Complete setup instructions
- [Manual Deployment Guide](MANUAL_DEPLOYMENT_GUIDE.md) - How to deploy to production
- [Production Environment Setup Guide](PROD_ENVIRONMENT_SETUP_GUIDE.md) - Production configuration