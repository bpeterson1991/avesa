# AVESA Dev Environment Setup Guide

This guide provides step-by-step instructions for setting up and testing the AVESA data pipeline in the development environment.

## Overview

The AVESA data pipeline consists of:
- **ConnectWise Ingestion Lambda**: Pulls data from ConnectWise API
- **Canonical Transform Lambdas**: Transform raw data to canonical format (separate function per table)
- **DynamoDB Tables**: Store tenant configurations and tracking data
- **S3 Buckets**: Store raw and canonical data in Parquet format
- **Secrets Manager**: Store API credentials securely

## Prerequisites

- AWS CLI configured with appropriate permissions
- Python 3.9+
- Access to AWS account 123938354448 (dev/staging account)
- Region: us-east-2

## Quick Start

### 1. Set Up Dev Environment Infrastructure

```bash
# Create missing DynamoDB tables and S3 bucket for dev environment
python scripts/setup-dev-environment.py --region us-east-2

# If you want to see what would be created without actually creating it:
python scripts/setup-dev-environment.py --region us-east-2 --dry-run
```

This script will:
- ✅ Create `TenantServices-dev` and `LastUpdated-dev` DynamoDB tables
- ✅ Create `data-storage-msp-dev` S3 bucket
- ✅ Upload mapping configurations to S3
- ✅ Create a test tenant with sample credentials
- ✅ Validate the setup

### 2. Fix Lambda Function Dependencies

```bash
# Redeploy Lambda functions with fixed dependencies
python scripts/deploy-lambda-functions.py --environment dev --region us-east-2

# Deploy only specific functions if needed:
python scripts/deploy-lambda-functions.py --environment dev --region us-east-2 --function connectwise
python scripts/deploy-lambda-functions.py --environment dev --region us-east-2 --function canonical
```

This script will:
- ✅ Fix the `pydantic_core` import issues
- ✅ Package shared modules correctly
- ✅ Update environment variables
- ✅ Deploy updated function code

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
- Sample tenant: `test-tenant`
- Sample ConnectWise credentials (non-functional for testing)

### Step 2: Lambda Function Issues

The main issues identified:

#### Dependency Problems
- ❌ `pydantic_core._pydantic_core` import errors
- ❌ Missing shared module imports
- ❌ Incorrect Python path configuration

#### Solutions Applied
- ✅ Added `pydantic-core>=2.0.0` to requirements.txt
- ✅ Fixed import paths in Lambda functions
- ✅ Proper packaging of shared modules
- ✅ Updated deployment process

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
  --payload '{"tenant_id": "test-tenant"}' \
  --cli-binary-format raw-in-base64-out \
  response.json --region us-east-2

# Check the response
cat response.json

# Monitor logs
aws logs tail /aws/lambda/avesa-connectwise-ingestion-dev --follow --region us-east-2
```

### Test Canonical Transform Functions

```bash
# Test tickets transformation
aws lambda invoke \
  --function-name avesa-canonical-transform-tickets-dev \
  --payload '{"tenant_id": "test-tenant"}' \
  --cli-binary-format raw-in-base64-out \
  response.json --region us-east-2

# Test other canonical functions
aws lambda invoke --function-name avesa-canonical-transform-time-entries-dev --payload '{"tenant_id": "test-tenant"}' --cli-binary-format raw-in-base64-out response.json --region us-east-2
aws lambda invoke --function-name avesa-canonical-transform-companies-dev --payload '{"tenant_id": "test-tenant"}' --cli-binary-format raw-in-base64-out response.json --region us-east-2
aws lambda invoke --function-name avesa-canonical-transform-contacts-dev --payload '{"tenant_id": "test-tenant"}' --cli-binary-format raw-in-base64-out response.json --region us-east-2
```

### Check Data Storage

```bash
# List S3 data
aws s3 ls s3://data-storage-msp-dev/ --recursive --region us-east-2

# Check DynamoDB data
aws dynamodb scan --table-name TenantServices-dev --region us-east-2
aws dynamodb scan --table-name LastUpdated-dev --region us-east-2

# Check secrets
aws secretsmanager list-secrets --filters Key=name,Values=tenant/ --region us-east-2
```

## Adding Real ConnectWise Credentials

To test with real ConnectWise data:

```bash
# Add real ConnectWise service to test tenant
python scripts/setup-service.py \
  --tenant-id test-tenant \
  --service connectwise \
  --connectwise-url 'https://api-na.myconnectwise.net' \
  --company-id 'YourCompanyID' \
  --public-key 'your-public-key' \
  --private-key 'your-private-key' \
  --client-id 'your-client-id' \
  --environment dev \
  --region us-east-2
```

## Troubleshooting

### Common Issues

#### 1. Pydantic Import Errors
```
Error: No module named 'pydantic_core._pydantic_core'
```
**Solution**: Redeploy Lambda functions with fixed dependencies
```bash
python scripts/deploy-lambda-functions.py --environment dev --region us-east-2
```

#### 2. Missing DynamoDB Tables
```
Error: Requested resource not found
```
**Solution**: Create dev environment tables
```bash
python scripts/setup-dev-environment.py --region us-east-2
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

1. **Create Additional Tenants**
   ```bash
   python scripts/setup-tenant.py --tenant-id "your-tenant" --company-name "Your Company" --environment dev
   ```

2. **Configure Additional Services**
   ```bash
   python scripts/setup-service.py --tenant-id "your-tenant" --service connectwise [options]
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
├── tenant-id/
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
    ├── canonical_mappings.json
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