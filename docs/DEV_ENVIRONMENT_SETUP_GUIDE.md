# AVESA Dev Environment Setup Guide

This guide provides step-by-step instructions for setting up and testing the AVESA optimized data pipeline in the development environment.

**Last Updated:** December 19, 2025
**Status:** Updated for optimized architecture

## Overview

The AVESA optimized data pipeline consists of:
- **Pipeline Orchestrator**: Main entry point using Step Functions for workflow coordination
- **Tenant Processor**: Handles tenant-level processing with parallel table execution
- **Table Processor**: Manages table-level processing with intelligent chunking
- **Chunk Processor**: Processes individual data chunks with timeout handling
- **Enhanced DynamoDB Tables**: Store tenant configurations, job tracking, and chunk progress
- **S3 Buckets**: Store raw and canonical data in optimized Parquet format
- **Secrets Manager**: Store API credentials securely
- **CloudWatch Monitoring**: Comprehensive metrics and dashboards for performance tracking

> **Architecture Change:** The pipeline now uses a multi-level parallel processing architecture instead of sequential Lambda execution.

## Prerequisites

- AWS CLI configured with appropriate permissions
- Python 3.9+
- Access to AWS account YOUR_DEV_ACCOUNT_ID (dev/staging account)
- Region: us-east-2

## Quick Start

### 1. Deploy Dev Environment Infrastructure

```bash
# Deploy complete dev environment infrastructure using optimized CDK
./scripts/deploy.sh --environment dev --region us-east-2
```

This script will:
- ✅ Create `TenantServices-dev` and `LastUpdated-dev` DynamoDB tables
- ✅ Create `ProcessingJobs-dev` and `ChunkProgress-dev` tables for job tracking
- ✅ Create `data-storage-msp-dev` S3 bucket
- ✅ Deploy optimized Lambda functions with lightweight packaging
- ✅ Deploy Step Functions state machines for workflow orchestration
- ✅ Upload mapping configurations to S3
- ✅ Set up EventBridge scheduling rules
- ✅ Configure IAM roles and policies
- ✅ Create CloudWatch dashboards and monitoring

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
- ✅ Configure the tenant for optimized data ingestion
- ✅ Enable the tenant for the new parallel processing pipeline

**Note:** Tenants are created automatically when adding their first service. The DynamoDB schema uses a composite key (`tenant_id` + `service`), and the optimized pipeline automatically discovers and processes configured tenants.

### 3. Test the Optimized Pipeline

```bash
# Run comprehensive tests for optimized pipeline
python scripts/test-lambda-functions.py --environment dev --region us-east-2 --verbose

# Test end-to-end pipeline with optimized architecture
python scripts/test-end-to-end-pipeline.py --environment dev --region us-east-2

# Test specific components:
python scripts/test-lambda-functions.py --environment dev --region us-east-2 --skip-canonical
python scripts/test-lambda-functions.py --environment dev --region us-east-2 --skip-ingestion

# Test Step Functions workflow
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-2:ACCOUNT:stateMachine:PipelineOrchestrator-dev" \
  --input '{"tenant_id": "{tenant-id}", "table_name": "service/tickets"}'
```

## Detailed Setup Steps

### Step 1: Environment Infrastructure

The dev environment requires specific AWS resources that may be missing:

#### DynamoDB Tables
- `TenantServices-dev`: Stores tenant and service configurations
- `LastUpdated-dev`: Tracks last sync timestamps for incremental updates
- `ProcessingJobs-dev`: Tracks execution state and job metadata for the optimized pipeline
- `ChunkProgress-dev`: Tracks chunk-level progress and resumability

#### S3 Bucket
- `data-storage-msp-dev`: Stores raw and canonical data files

#### Test Data
- Sample tenant: `{tenant-id}`
- Sample ConnectWise credentials (non-functional for testing)

### Step 2: Optimized Lambda Function Packaging

The AVESA optimized Lambda functions use an advanced lightweight packaging approach:

#### AWS Pandas Layer Optimization
- ✅ 99.9% package size reduction achieved using AWS Pandas layers
- ✅ Heavy dependencies (pandas, numpy, pyarrow) provided by AWS layers
- ✅ Lightweight application-specific packages only
- ✅ Optimized deployment and cold start performance
- ✅ Specialized functions for orchestration, tenant processing, table processing, and chunk processing

#### Optimized Architecture Components
- **Pipeline Orchestrator**: [`src/optimized/orchestrator/lambda_function.py`](../src/optimized/orchestrator/lambda_function.py)
- **Tenant Processor**: [`src/optimized/processors/tenant_processor.py`](../src/optimized/processors/tenant_processor.py)
- **Table Processor**: [`src/optimized/processors/table_processor.py`](../src/optimized/processors/table_processor.py)
- **Chunk Processor**: [`src/optimized/processors/chunk_processor.py`](../src/optimized/processors/chunk_processor.py)

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

#### End-to-End Optimized Pipeline
- Multi-level parallel processing coordination via Step Functions
- Intelligent chunking and concurrent data processing
- Enhanced monitoring and progress tracking
- Data ingestion from ConnectWise API (with test credentials)
- Canonical transformation processing
- S3 data storage and retrieval with optimized performance

## Manual Testing Commands

### Test Optimized Pipeline Orchestrator

```bash
# Start optimized pipeline execution via Step Functions
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-2:ACCOUNT:stateMachine:PipelineOrchestrator-dev" \
  --input '{"tenant_id": "{tenant-id}", "table_name": "service/tickets"}' \
  --region us-east-2

# Monitor Step Functions execution
aws stepfunctions describe-execution \
  --execution-arn "EXECUTION_ARN" \
  --region us-east-2

# Test individual Lambda components
aws lambda invoke \
  --function-name avesa-pipeline-orchestrator-dev \
  --payload '{"tenant_id": "{tenant-id}"}' \
  --cli-binary-format raw-in-base64-out \
  response.json --region us-east-2

# Monitor optimized pipeline logs
aws logs tail /aws/lambda/avesa-pipeline-orchestrator-dev --follow --region us-east-2
aws logs tail /aws/lambda/avesa-tenant-processor-dev --follow --region us-east-2
```

### Test Canonical Transform Functions

```bash
# Test canonical transformation (integrated into optimized pipeline)
aws lambda invoke \
  --function-name avesa-canonical-transform-dev \
  --payload '{"tenant_id": "{tenant-id}", "table_name": "tickets"}' \
  --cli-binary-format raw-in-base64-out \
  response.json --region us-east-2

# Test other canonical tables
aws lambda invoke --function-name avesa-canonical-transform-dev --payload '{"tenant_id": "{tenant-id}", "table_name": "time_entries"}' --cli-binary-format raw-in-base64-out response.json --region us-east-2
aws lambda invoke --function-name avesa-canonical-transform-dev --payload '{"tenant_id": "{tenant-id}", "table_name": "companies"}' --cli-binary-format raw-in-base64-out response.json --region us-east-2
aws lambda invoke --function-name avesa-canonical-transform-dev --payload '{"tenant_id": "{tenant-id}", "table_name": "contacts"}' --cli-binary-format raw-in-base64-out response.json --region us-east-2

# Monitor optimized processing jobs
aws dynamodb scan --table-name ProcessingJobs-dev --region us-east-2
aws dynamodb scan --table-name ChunkProgress-dev --region us-east-2
```

### Check Data Storage

```bash
# List S3 data
aws s3 ls s3://data-storage-msp-dev/ --recursive --region us-east-2

# Check DynamoDB data
aws dynamodb scan --table-name TenantServices-dev --region us-east-2
aws dynamodb scan --table-name LastUpdated-dev --region us-east-2

# Check optimized pipeline tracking tables
aws dynamodb scan --table-name ProcessingJobs-dev --region us-east-2
aws dynamodb scan --table-name ChunkProgress-dev --region us-east-2

# Check secrets
aws secretsmanager list-secrets --filters Key=name,Values={tenant-id}/ --region us-east-2

# Monitor CloudWatch dashboards
aws cloudwatch get-dashboard --dashboard-name "AVESA-Pipeline-Overview-dev" --region us-east-2
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
**Solution**: Redeploy Lambda functions using the optimized deployment script
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
**Solution**: Deploy the optimized infrastructure first
```bash
cd infrastructure
cdk deploy --app "python app.py" --context environment=dev --all --region us-east-2
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
EventBridge → Pipeline Orchestrator (Step Functions) → Tenant Processor (Step Functions)
                                                              ↓
                                                    Table Processor (Step Functions)
                                                              ↓
                                                    Chunk Processor (Lambda) → S3 (Raw Parquet)
                                                              ↓
                                                    Canonical Transform → S3 (Canonical Parquet)
```

**Optimized Architecture Benefits:**
- Multi-level parallelization (tenant → table → chunk)
- Intelligent chunking with timeout handling
- Resumable processing with state persistence
- Comprehensive monitoring and progress tracking

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