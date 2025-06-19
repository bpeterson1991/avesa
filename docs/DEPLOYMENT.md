# AVESA Deployment Guide

This guide covers deploying the AVESA Multi-Tenant Data Pipeline to AWS using the unified deployment script.

## Prerequisites

Before deploying AVESA, ensure you have the following installed and configured:

1. **AWS CLI** - [Installation Guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
2. **Node.js 18+** - Required for AWS CDK
3. **Python 3.9+** - For Lambda functions and deployment scripts
4. **AWS CDK CLI** - Install globally: `npm install -g aws-cdk`

### AWS Account Setup

AVESA uses a hybrid account strategy:

- **Development/Staging**: Uses your default AWS account or specified profile
- **Production**: Uses dedicated production account with `avesa-production` profile

## Quick Start

### 1. Clone and Setup

```bash
git clone <repository-url>
cd avesa
pip install -r requirements.txt
```

### 2. Deploy to Development

```bash
./scripts/deploy.sh --environment dev
```

### 3. Deploy to Staging

```bash
./scripts/deploy.sh --environment staging
```

### 4. Deploy to Production

```bash
./scripts/deploy.sh --environment prod
```

## Unified Deployment Script

The `scripts/deploy.sh` script provides a consistent deployment experience across all environments.

### Usage

```bash
./scripts/deploy.sh [OPTIONS]

Options:
  -e, --environment ENVIRONMENT    Environment to deploy (dev, staging, prod) [REQUIRED]
  -r, --region REGION             AWS region [default: us-east-2]
  -p, --profile PROFILE           AWS profile to use [auto-detected based on environment]
  -h, --help                      Show help message

Examples:
  ./scripts/deploy.sh --environment dev
  ./scripts/deploy.sh --environment prod --region us-west-2
  ./scripts/deploy.sh -e staging -p my-aws-profile
```

### Environment-Specific Configuration

The script automatically configures environment-specific settings:

#### Development/Staging
- **AWS Profile**: Uses `default` or specified profile
- **Account Variable**: `CDK_DEFAULT_ACCOUNT`
- **Resource Naming**: Resources suffixed with `-{environment}`
- **S3 Bucket**: `data-storage-msp-{environment}`
- **DynamoDB Tables**: `TenantServices-{environment}`, `LastUpdated-{environment}`

#### Production
- **AWS Profile**: Uses `avesa-production` (can be overridden)
- **Account Variable**: `CDK_PROD_ACCOUNT`
- **Resource Naming**: No suffix (clean production names)
- **S3 Bucket**: `data-storage-msp`
- **DynamoDB Tables**: `TenantServices`, `LastUpdated`

## Deployment Workflow

The unified script follows this consistent workflow for all environments:

### 1. Prerequisites Validation
- Checks AWS CLI installation
- Verifies CDK CLI availability
- Confirms project structure
- Validates Python requirements

### 2. Environment Configuration
- Sets appropriate AWS profile
- Configures account-specific environment variables
- Verifies AWS credentials and access
- Installs Python dependencies

### 3. CDK Bootstrap
- Checks if CDK is bootstrapped in target region
- Bootstraps CDK if needed
- Uses environment-specific AWS profile

### 4. Infrastructure Deployment
- Synthesizes CDK application with environment context
- Deploys all CDK stacks
- Handles environment-specific resource naming

### 5. Mapping File Upload
- Uploads canonical mapping files to S3
- Uploads integration endpoint configurations
- Uses environment-specific bucket naming

### 6. Deployment Validation
- Verifies CloudFormation stacks
- Checks Lambda function creation
- Validates DynamoDB tables
- Confirms S3 bucket accessibility

## AWS Profile Configuration

### Development/Staging

For development and staging environments, configure your default AWS profile:

```bash
aws configure
```

Or use a specific profile:

```bash
aws configure --profile my-dev-profile
./scripts/deploy.sh --environment dev --profile my-dev-profile
```

### Production

For production deployments, configure the `avesa-production` profile:

```bash
aws configure --profile avesa-production
```

Or set up cross-account role access:

```bash
aws configure set role_arn arn:aws:iam::PROD_ACCOUNT_ID:role/DeploymentRole --profile avesa-production
aws configure set source_profile default --profile avesa-production
```

## Post-Deployment Setup

### 1. Tenant Configuration

After successful deployment, set up tenants using the service setup script:

#### Setup ConnectWise Service for a Tenant
```bash
python scripts/setup-service.py \
  --tenant-id "example-tenant" \
  --company-name "Example Company" \
  --service connectwise \
  --environment dev
```

The script will prompt you for the required ConnectWise credentials:
- **company_id**: Your ConnectWise company identifier
- **public_key**: Your ConnectWise API public key
- **private_key**: Your ConnectWise API private key
- **client_id**: Your ConnectWise API client ID
- **api_base_url**: Your ConnectWise API base URL (e.g., "https://api-na.myconnectwise.net")

Alternatively, you can provide credentials via environment variables:
```bash
export CONNECTWISE_COMPANY_ID="YourCompanyID"
export CONNECTWISE_PUBLIC_KEY="your-public-key"
export CONNECTWISE_PRIVATE_KEY="your-private-key"
export CONNECTWISE_CLIENT_ID="your-client-id"
export CONNECTWISE_API_BASE_URL="https://api-na.myconnectwise.net"

python scripts/setup-service.py \
  --tenant-id "example-tenant" \
  --company-name "Example Company" \
  --service connectwise \
  --environment dev
```

### 2. Testing the Pipeline

Test the deployed pipeline:

```bash
# Test Lambda function
aws lambda invoke \
  --function-name avesa-connectwise-ingestion-dev \
  --payload '{"tenant_id": "example-tenant"}' \
  response.json

# Monitor logs
aws logs tail /aws/lambda/avesa-connectwise-ingestion-dev --follow

# Check S3 data
aws s3 ls s3://data-storage-msp-dev/ --recursive
```

## Monitoring and Troubleshooting

### CloudWatch Resources

After deployment, monitor your pipeline using:

- **Lambda Logs**: `/aws/lambda/avesa-*` log groups
- **Custom Metrics**: `AVESA/Pipeline` namespace
- **CloudWatch Dashboards**: Available in AWS Console

### Common Commands

```bash
# View tenant configurations
aws dynamodb scan --table-name TenantServices-dev

# Get specific tenant service configuration
aws dynamodb get-item \
  --table-name TenantServices-dev \
  --key '{"tenant_id":{"S":"example-tenant"},"service":{"S":"connectwise"}}'

# List tenant secrets
aws secretsmanager list-secrets --filters Key=name,Values=tenant/

# Check deployment status
aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE
```

### Troubleshooting

#### CDK Bootstrap Required
```
Error: This stack uses assets, so the toolkit stack must be deployed
```
**Solution**: The script automatically handles CDK bootstrapping

#### Insufficient Permissions
```
Error: User is not authorized to perform: iam:CreateRole
```
**Solution**: Ensure your AWS credentials have sufficient permissions for CDK deployment

#### Profile Not Found
```
Error: AWS profile 'avesa-production' not configured
```
**Solution**: Configure the required AWS profile or specify a different one

## Security Considerations

### Secrets Management
- ConnectWise credentials stored in AWS Secrets Manager
- Tenant-specific secret isolation
- Automatic secret rotation support

### IAM Security
- Lambda functions use least-privilege IAM roles
- Environment-specific resource access
- Cross-account role separation for production

### Data Encryption
- S3 buckets use AWS managed encryption
- DynamoDB tables encrypted at rest
- Secrets Manager automatic encryption

## Cost Optimization

### Resource Sizing
- Lambda memory optimized per function
- DynamoDB on-demand pricing for variable workloads
- S3 lifecycle policies for data archival

### Monitoring Costs
- CloudWatch log retention policies
- Lambda execution monitoring
- S3 storage class optimization

## Rollback Procedures

### Emergency Rollback

To rollback a deployment:

```bash
# Destroy specific environment
cd infrastructure
cdk destroy --context environment=dev --all

# Or destroy specific stacks
cdk destroy AVESADataPipeline-dev
cdk destroy AVESAMonitoring-dev
```

### Partial Rollback

For partial rollbacks, redeploy with previous configuration:

```bash
git checkout <previous-commit>
./scripts/deploy.sh --environment dev
```

## Migration from Legacy Scripts

If migrating from the old deployment scripts:

1. **Remove old scripts** (done automatically):
   - `scripts/deploy-dev-staging.sh`
   - `scripts/deploy-prod.sh`

2. **Update CI/CD pipelines** to use:
   ```bash
   ./scripts/deploy.sh --environment $ENVIRONMENT
   ```

3. **Update documentation references** to point to unified script

## Advanced Configuration

### Custom Regions

Deploy to different AWS regions:

```bash
./scripts/deploy.sh --environment prod --region us-west-2
```

### Custom Profiles

Use specific AWS profiles:

```bash
./scripts/deploy.sh --environment dev --profile my-custom-profile
```

### Environment Variables

The script sets these environment variables automatically:

- `AWS_PROFILE`: Active AWS profile
- `CDK_DEFAULT_REGION`: Target AWS region
- `CDK_DEFAULT_ACCOUNT` or `CDK_PROD_ACCOUNT`: Account ID

## Support

For deployment issues:

1. Check the deployment logs for specific error messages
2. Verify AWS credentials and permissions
3. Ensure all prerequisites are installed
4. Review the troubleshooting section above

For additional support, refer to the project documentation or contact the development team.