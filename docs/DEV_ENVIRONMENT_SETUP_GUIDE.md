# AVESA Dev Environment Setup Guide

This guide provides step-by-step instructions for setting up and testing the AVESA data pipeline in the development environment.

## Overview

The AVESA data pipeline consists of:
- **ConnectWise Ingestion Lambda**: Pulls data from ConnectWise API
- **Canonical Transform Lambda**: Single function that transforms raw data to canonical format using `CANONICAL_TABLE` environment variable
- **DynamoDB Tables**: Store tenant configurations and tracking data
- **S3 Buckets**: Store raw and canonical data in Parquet format
- **Secrets Manager**: Store API credentials securely

## Prerequisites

- AWS CLI configured with appropriate permissions
- Python 3.9+
- Access to AWS account YOUR_DEV_ACCOUNT_ID (dev/staging account)
- Region: us-east-2

## Quick Start

### 1. Deploy Dev Environment Infrastructure

```bash
# Deploy complete dev environment infrastructure using CDK
./scripts/deploy.sh --environment dev --region us-east-2
```

This script will:
- ✅ Create `TenantServices-dev` and `LastUpdated-dev` DynamoDB tables
- ✅ Create `data-storage-msp-dev` S3 bucket
- ✅ Deploy Lambda functions with proper dependencies
- ✅ Upload mapping configurations to S3
- ✅ Set up EventBridge scheduling rules
- ✅ Configure IAM roles and policies

### 2. Create a Test Tenant with Service

```bash
# Add ConnectWise service to a tenant (creates tenant automatically if it doesn't exist)
python scripts/setup-service.py \
  --tenant-id "{tenant-id}" \
  --company-name "Test Company" \
  --service connectwise \
  --environment dev \
  --region us-east-2
```

This will:
- ✅ Create a test tenant in DynamoDB (if it doesn't exist)
- ✅ Store test ConnectWise credentials in Secrets Manager
- ✅ Configure the tenant for ConnectWise data ingestion

**Note:** Tenants are created automatically when adding their first service. The DynamoDB schema uses a composite key (`tenant_id` + `service`), so tenant creation is handled seamlessly during service setup.

### 3. Test the Pipeline

```bash
# Run comprehensive tests
python scripts/test-lambda-functions.py --environment dev --region us-east-2 --verbose

# Test specific components:
python scripts/test-lambda-functions.py --environment dev --region us-east-2 --skip-canonical
python scripts/test-lambda-functions.py --environment dev --region us-east-2 --skip-ingestion
```

## Detailed Setup Steps

### Step 1: Environment Infrastructure

The dev environment requires specific AWS resources that may be missing:

#### DynamoDB Tables
- `TenantServices-dev`: Stores tenant and service configurations
- `LastUpdated-dev`: Tracks last sync timestamps for incremental updates

#### S3 Bucket
- `data-storage-msp-dev`: Stores raw and canonical data files

#### Test Data
- Sample tenant: `{tenant-id}`
- Sample ConnectWise credentials (non-functional for testing)

### Step 2: Lambda Function Packaging

The AVESA Lambda functions use a lightweight packaging approach:

#### AWS Pandas Layer Optimization
- ✅ 99.9% package size reduction achieved using AWS Pandas layers
- ✅ Heavy dependencies (pandas, numpy, pyarrow) provided by AWS layers
- ✅ Lightweight application-specific packages only
- ✅ Optimized deployment and cold start performance

### Step 3: Testing Process

The testing script validates:

#### Environment Setup
- DynamoDB table existence and status
- S3 bucket accessibility
- Mapping file availability

#### Lambda Function Functionality
- Function existence and configuration
- Successful invocation without errors
- Proper error handling and logging

#### End-to-End Pipeline
- Data ingestion from ConnectWise API (with test credentials)
- Canonical transformation processing
- S3 data storage and retrieval

## Manual Testing Commands

### Test ConnectWise Ingestion

```bash
# Invoke ConnectWise ingestion function
aws lambda invoke \
  --function-name avesa-connectwise-ingestion-dev \
  --payload '{"tenant_id": "{tenant-id}"}' \
  --cli-binary-format raw-in-base64-out \
  response.json --region us-east-2

# Check the response
cat response.json

# Monitor logs
aws logs tail /aws/lambda/avesa-connectwise-ingestion-dev --follow --region us-east-2
```

### Test Canonical Transform Functions

```bash
# Test canonical transformation (single function with CANONICAL_TABLE environment variable)
aws lambda invoke \
  --function-name avesa-canonical-transform-dev \
  --payload '{"tenant_id": "{tenant-id}", "table_name": "tickets"}' \
  --cli-binary-format raw-in-base64-out \
  response.json --region us-east-2

# Test other canonical tables
aws lambda invoke --function-name avesa-canonical-transform-dev --payload '{"tenant_id": "{tenant-id}", "table_name": "time_entries"}' --cli-binary-format raw-in-base64-out response.json --region us-east-2
aws lambda invoke --function-name avesa-canonical-transform-dev --payload '{"tenant_id": "{tenant-id}", "table_name": "companies"}' --cli-binary-format raw-in-base64-out response.json --region us-east-2
aws lambda invoke --function-name avesa-canonical-transform-dev --payload '{"tenant_id": "{tenant-id}", "table_name": "contacts"}' --cli-binary-format raw-in-base64-out response.json --region us-east-2
```

### Check Data Storage

```bash
# List S3 data
aws s3 ls s3://data-storage-msp-dev/ --recursive --region us-east-2

# Check DynamoDB data
aws dynamodb scan --table-name TenantServices-dev --region us-east-2
aws dynamodb scan --table-name LastUpdated-dev --region us-east-2

# Check secrets
aws secretsmanager list-secrets --filters Key=name,Values={tenant-id}/ --region us-east-2
```

## Adding Real ConnectWise Credentials

To test with real ConnectWise data:

```bash
# Add real ConnectWise service to test tenant
python scripts/setup-service.py \
  --tenant-id {tenant-id} \
  --company-name "Test Company" \
  --service connectwise \
  --environment dev \
  --region us-east-2

# The script will prompt for ConnectWise credentials interactively,
# or you can provide them via environment variables:
# export CONNECTWISE_API_BASE_URL='https://api-na.myconnectwise.net'
# export CONNECTWISE_COMPANY_ID='YourCompanyID'
# export CONNECTWISE_PUBLIC_KEY='your-public-key'
# export CONNECTWISE_PRIVATE_KEY='your-private-key'
# export CONNECTWISE_CLIENT_ID='your-client-id'
```

## Troubleshooting

### Common Issues

#### 1. Pydantic Import Errors
```
Error: No module named 'pydantic_core._pydantic_core'
```
**Solution**: Redeploy Lambda functions using the deployment script
```bash
./scripts/deploy.sh --environment dev --region us-east-2
```

#### 2. Missing DynamoDB Tables
```
Error: Requested resource not found
```
**Solution**: Deploy the dev environment infrastructure
```bash
./scripts/deploy.sh --environment dev --region us-east-2
```

#### 3. S3 Access Denied
```
Error: Access Denied
```
**Solution**: Check IAM permissions and bucket existence
```bash
aws s3 ls s3://data-storage-msp-dev/ --region us-east-2
```

#### 4. Lambda Function Not Found
```
Error: Function not found
```
**Solution**: Deploy the infrastructure first
```bash
cd infrastructure
cdk deploy --context environment=dev --all --region us-east-2
```

### Debugging Steps

1. **Check CloudWatch Logs**
   ```bash
   aws logs tail /aws/lambda/[FUNCTION_NAME] --follow --region us-east-2
   ```

2. **Validate Environment Variables**
   ```bash
   aws lambda get-function-configuration --function-name [FUNCTION_NAME] --region us-east-2
   ```

3. **Test IAM Permissions**
   ```bash
   aws sts get-caller-identity
   aws iam get-role --role-name [LAMBDA_ROLE_NAME]
   ```

4. **Check Resource Status**
   ```bash
   aws dynamodb describe-table --table-name TenantServices-dev --region us-east-2
   aws s3api head-bucket --bucket data-storage-msp-dev --region us-east-2
   ```

## Next Steps

After successful dev environment setup:

1. **Create Additional Tenants with Services**
   ```bash
   # Creates tenant automatically when adding the first service
   python scripts/setup-service.py --tenant-id "{tenant-id}" --company-name "Your Company" --service connectwise --environment dev
   ```

2. **Add Additional Services to Existing Tenants**
   ```bash
   python scripts/setup-service.py --tenant-id "{tenant-id}" --company-name "Your Company" --service servicenow --environment dev
   ```

3. **Monitor Pipeline Performance**
   - Check CloudWatch dashboards
   - Review S3 data growth
   - Monitor DynamoDB usage

4. **Set Up Automated Testing**
   - Schedule regular pipeline tests
   - Set up data quality monitoring
   - Configure alerting for failures

## Architecture Notes

### Data Flow
```
ConnectWise API → ConnectWise Lambda → S3 (Raw Parquet)
                                           ↓
S3 (Raw Parquet) → Canonical Transform Lambdas → S3 (Canonical Parquet)
```

### Storage Structure
```
s3://data-storage-msp-dev/
├── {tenant-id}/
│   ├── raw/
│   │   └── connectwise/
│   │       ├── service/tickets/
│   │       ├── time/entries/
│   │       ├── company/companies/
│   │       └── company/contacts/
│   └── canonical/
│       ├── tickets/
│       ├── time_entries/
│       ├── companies/
│       └── contacts/
└── mappings/
    ├── canonical/                # Canonical transformation mappings
    │   ├── tickets.json          # Canonical mapping for tickets
    │   ├── time_entries.json     # Canonical mapping for time entries
    │   ├── companies.json        # Canonical mapping for companies
    │   └── contacts.json         # Canonical mapping for contacts
    ├── backfill_config.json      # Backfill configuration
    └── integrations/
        └── connectwise_endpoints.json
```

### Security Considerations

- All API credentials stored in AWS Secrets Manager
- S3 buckets have public access blocked
- Lambda functions use least-privilege IAM roles
- DynamoDB tables use encryption at rest
- VPC endpoints recommended for production

## Support

For issues or questions:
1. Check CloudWatch logs for detailed error messages
2. Run the test script with `--verbose` flag
3. Review this documentation for common solutions
4. Check AWS service status and limits