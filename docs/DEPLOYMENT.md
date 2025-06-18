# Deployment Guide

This guide covers deploying the ConnectWise Data Pipeline to AWS using CDK.

## Prerequisites

1. **AWS CLI** configured with appropriate credentials
2. **Node.js 18+** for CDK
3. **Python 3.9+** for Lambda functions
4. **AWS CDK CLI** installed globally: `npm install -g aws-cdk`

## Quick Start

1. **Clone and setup the project:**
```bash
git clone <repository-url>
cd connectwise-data-pipeline
pip install -r requirements.txt
```

2. **Deploy to development environment:**
```bash
./scripts/deploy.sh --environment dev
```

3. **Setup a tenant:**
```bash
python scripts/setup-tenant.py \
  --tenant-id "example-tenant" \
  --company-name "Example Company" \
  --connectwise-url "https://api-na.myconnectwise.net" \
  --company-id "ExampleCorp" \
  --public-key "your-public-key" \
  --private-key "your-private-key" \
  --client-id "your-client-id"
```

## Detailed Deployment Steps

### 1. Environment Configuration

The pipeline supports three environments:
- **dev**: Development environment with minimal resources
- **staging**: Staging environment with monitoring enabled
- **prod**: Production environment with full monitoring and alerting

### 2. CDK Bootstrap

If this is your first time using CDK in the target region:
```bash
cdk bootstrap --region us-east-1
```

### 3. Deploy Infrastructure

Deploy all stacks:
```bash
cd infrastructure
cdk deploy --context environment=prod --all
```

Or deploy specific stacks:
```bash
cdk deploy ConnectWiseDataPipeline-prod
cdk deploy ConnectWiseMonitoring-prod
```

### 4. Configure Monitoring (Production Only)

Set up email alerts by providing an email address:
```bash
cdk deploy --context environment=prod --context alert_email=admin@company.com
```

## Environment Variables

The Lambda functions use these environment variables (automatically set by CDK):

- `BUCKET_NAME`: S3 bucket for data storage
- `TENANT_SERVICES_TABLE`: DynamoDB table for tenant configuration
- `LAST_UPDATED_TABLE`: DynamoDB table for incremental sync state
- `ENVIRONMENT`: Current environment (dev/staging/prod)

## Post-Deployment Configuration

### 1. Tenant Setup

Use the setup script to configure tenants:
```bash
python scripts/setup-tenant.py --help
```

### 2. Verify Deployment

Check that resources were created:
```bash
# List Lambda functions
aws lambda list-functions --query 'Functions[?contains(FunctionName, `connectwise`)]'

# Check DynamoDB tables
aws dynamodb list-tables --query 'TableNames[?contains(@, `TenantServices`) || contains(@, `LastUpdated`)]'

# Verify S3 bucket
aws s3 ls | grep data-storage-msp
```

### 3. Test the Pipeline

Test raw ingestion for a specific tenant:
```bash
aws lambda invoke \
  --function-name connectwise-raw-ingestion-dev \
  --payload '{"tenant_id": "example-tenant"}' \
  response.json
```

Monitor logs:
```bash
aws logs tail /aws/lambda/connectwise-raw-ingestion-dev --follow
```

## Troubleshooting

### Common Issues

1. **CDK Bootstrap Required**
   ```
   Error: This stack uses assets, so the toolkit stack must be deployed
   ```
   Solution: Run `cdk bootstrap` in the target region

2. **Insufficient Permissions**
   ```
   Error: User is not authorized to perform: iam:CreateRole
   ```
   Solution: Ensure your AWS credentials have sufficient permissions for CDK deployment

3. **Lambda Package Too Large**
   ```
   Error: Unzipped size must be smaller than 262144000 bytes
   ```
   Solution: Optimize dependencies in requirements.txt files

### Monitoring and Debugging

1. **CloudWatch Dashboards**: Available in AWS Console under CloudWatch > Dashboards
2. **Lambda Logs**: `/aws/lambda/connectwise-*` log groups
3. **Custom Metrics**: `ConnectWise/Pipeline` namespace in CloudWatch

### Rollback

To rollback a deployment:
```bash
cdk destroy ConnectWiseDataPipeline-prod
cdk destroy ConnectWiseMonitoring-prod
```

## Security Considerations

1. **Secrets Management**: ConnectWise credentials are stored in AWS Secrets Manager
2. **IAM Roles**: Lambda functions use least-privilege IAM roles
3. **VPC**: Consider deploying Lambda functions in a VPC for additional security
4. **Encryption**: S3 buckets and DynamoDB tables use AWS managed encryption

## Cost Optimization

1. **S3 Lifecycle Policies**: Automatically transition old data to cheaper storage classes
2. **DynamoDB On-Demand**: Pay only for what you use
3. **Lambda Memory**: Tune memory settings based on actual usage
4. **CloudWatch Logs**: Set appropriate retention periods

## Scaling Considerations

1. **Lambda Concurrency**: Monitor and adjust reserved concurrency if needed
2. **DynamoDB Capacity**: Switch to provisioned capacity for predictable workloads
3. **S3 Performance**: Use appropriate prefixes for high-throughput scenarios
4. **API Rate Limits**: Implement backoff strategies for ConnectWise API calls