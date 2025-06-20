# Canonical Transform Pipeline Testing Guide

This guide provides comprehensive instructions for testing the canonical transform part of the AVESA data pipeline on the dev environment.

## Overview

The canonical transform pipeline converts raw JSON data from various integration services into standardized parquet files with SCD Type 2 historical tracking. This guide covers:

1. **Prerequisites and Setup**
2. **Testing Canonical Transform Lambda Functions**
3. **Verifying S3 Parquet File Generation**
4. **Validating Canonical Data Format**
5. **Data Quality Validation**
6. **Troubleshooting Common Issues**

## Prerequisites

### 1. AWS Credentials Configuration

Before running any tests, ensure AWS credentials are properly configured:

```bash
# Option 1: Configure AWS CLI
aws configure
# Enter your Access Key ID, Secret Access Key, and region (us-east-2)

# Option 2: Use environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-2

# Option 3: Use AWS profiles
aws configure --profile avesa-dev
export AWS_PROFILE=avesa-dev
```

### 2. Required Python Dependencies

```bash
pip install boto3 pandas pyarrow
```

### 3. Infrastructure Deployment

Ensure the canonical transform Lambda functions are deployed:

```bash
cd infrastructure
cdk deploy --context environment=dev
```

## Canonical Transform Architecture

### Lambda Functions

The canonical transform pipeline uses separate Lambda functions for each table type:

- `avesa-canonical-transform-companies-dev`
- `avesa-canonical-transform-contacts-dev`
- `avesa-canonical-transform-tickets-dev`
- `avesa-canonical-transform-time-entries-dev`

### S3 Structure

**Raw Data Path:**
```
s3://avesa-data-dev/{tenant_id}/raw/connectwise/{table}/YYYY/MM/DD/
```

**Canonical Data Path:**
```
s3://avesa-data-dev/{tenant_id}/canonical/{table}/YYYY/MM/DD/
```

### Canonical Mappings

Transformation rules are defined in:
- `mappings/canonical/companies.json`
- `mappings/canonical/contacts.json`
- `mappings/canonical/tickets.json`
- `mappings/canonical/time_entries.json`

## Testing Methods

### Method 1: Direct Lambda Function Testing

Test individual canonical transform functions:

```bash
cd tests
python3 test-canonical-lambda-direct.py --table companies
python3 test-canonical-lambda-direct.py --table contacts
python3 test-canonical-lambda-direct.py --table tickets
python3 test-canonical-lambda-direct.py --table time_entries
```

### Method 2: Comprehensive Pipeline Testing

Test the complete canonical transform pipeline:

```bash
cd tests
python3 test-canonical-transform-pipeline.py
```

### Method 3: Using Existing Test Framework

Use the existing Lambda function test framework:

```bash
cd tests
python3 test-lambda-functions.py --skip-ingestion
```

## Test Scenarios

### 1. Basic Function Execution Test

**Objective:** Verify that canonical transform Lambda functions can be invoked successfully.

**Test Steps:**
1. Check if Lambda functions exist and are accessible
2. Invoke each function with test payload
3. Verify successful execution and response format

**Expected Results:**
- HTTP 200 status code
- Valid JSON response with processing summary
- No function errors or timeouts

### 2. Raw Data Processing Test

**Objective:** Verify that functions can process actual raw data files.

**Test Steps:**
1. Check for available raw data in S3
2. Trigger canonical transform for tables with raw data
3. Monitor execution and verify completion

**Expected Results:**
- Functions process available raw data files
- Records are successfully transformed
- No processing errors

### 3. Parquet File Generation Test

**Objective:** Verify that canonical parquet files are generated correctly.

**Test Steps:**
1. Trigger canonical transform
2. Check S3 for generated parquet files
3. Verify file structure and naming conventions
4. Validate parquet file format

**Expected Results:**
- Parquet files created in correct S3 path structure
- Files follow naming convention: `{timestamp}.parquet`
- Files are valid parquet format and readable

### 4. Schema Validation Test

**Objective:** Verify that canonical data follows expected schema.

**Test Steps:**
1. Read generated parquet files
2. Check for required canonical fields
3. Verify SCD Type 2 fields are present
4. Validate data types and formats

**Expected Canonical Fields:**
- `source_system`: Source integration system
- `canonical_table`: Target canonical table name
- `ingestion_timestamp`: Processing timestamp
- `effective_start_date`: SCD Type 2 start date
- `effective_end_date`: SCD Type 2 end date
- `is_current`: Current record flag
- `record_hash`: Change detection hash

### 5. Data Quality Validation Test

**Objective:** Verify data quality and transformation accuracy.

**Test Steps:**
1. Sample records from canonical parquet files
2. Verify field mappings are applied correctly
3. Check for data completeness and consistency
4. Validate timestamp formats and data types

**Quality Criteria:**
- No null values in key fields
- Valid timestamp formats
- Proper field mapping from source to canonical
- Consistent data types

## Manual Testing Procedures

### 1. Check Raw Data Availability

```bash
# List raw data files for a tenant
aws s3 ls s3://avesa-data-dev/sitetechnology/raw/connectwise/ --recursive

# Check recent files (last 24 hours)
aws s3 ls s3://avesa-data-dev/sitetechnology/raw/connectwise/companies/ --recursive | grep $(date +%Y/%m/%d)
```

### 2. Trigger Canonical Transform Manually

```bash
# Invoke canonical transform function
aws lambda invoke \
  --function-name avesa-canonical-transform-companies-dev \
  --payload '{"tenant_id":"sitetechnology","test_mode":true}' \
  response.json

# Check response
cat response.json
```

### 3. Verify Parquet File Generation

```bash
# List canonical parquet files
aws s3 ls s3://avesa-data-dev/sitetechnology/canonical/ --recursive

# Download and inspect a parquet file
aws s3 cp s3://avesa-data-dev/sitetechnology/canonical/companies/2024/12/19/20241219_143022.parquet ./sample.parquet

# Use Python to inspect parquet content
python3 -c "
import pandas as pd
df = pd.read_parquet('sample.parquet')
print(f'Records: {len(df)}')
print(f'Columns: {list(df.columns)}')
print(df.head())
"
```

### 4. Check Lambda Function Logs

```bash
# View recent logs
aws logs tail /aws/lambda/avesa-canonical-transform-companies-dev --follow

# Search for errors
aws logs filter-log-events \
  --log-group-name /aws/lambda/avesa-canonical-transform-companies-dev \
  --filter-pattern "ERROR"
```

## Expected Test Results

### Successful Canonical Transform

**Lambda Function Response:**
```json
{
  "statusCode": 200,
  "body": {
    "message": "Canonical transformation completed for companies",
    "canonical_table": "companies",
    "successful_jobs": 1,
    "failed_jobs": 0,
    "total_records": 150,
    "results": [
      {
        "tenant_id": "sitetechnology",
        "canonical_table": "companies",
        "status": "success",
        "record_count": 150,
        "execution_time": 12.5,
        "s3_key": "sitetechnology/canonical/companies/2024/12/19/20241219_143022.parquet"
      }
    ]
  }
}
```

**S3 File Structure:**
```
s3://avesa-data-dev/
└── sitetechnology/
    └── canonical/
        ├── companies/
        │   └── 2024/12/19/
        │       └── 20241219_143022.parquet
        ├── contacts/
        │   └── 2024/12/19/
        │       └── 20241219_143527.parquet
        ├── tickets/
        │   └── 2024/12/19/
        │       └── 20241219_144032.parquet
        └── time_entries/
            └── 2024/12/19/
                └── 20241219_144537.parquet
```

**Parquet File Schema:**
```
Column                  Type
----------------------  --------
id                      int64
company_name            object
company_identifier      object
status                  object
address_line1           object
city                    object
state                   object
zip                     object
country                 object
phone_number            object
website                 object
last_updated            object
source_system           object
canonical_table         object
ingestion_timestamp     object
effective_start_date    object
effective_end_date      object
is_current              bool
record_hash             object
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Lambda Function Not Found

**Error:** `Function not found or not accessible`

**Solutions:**
- Verify function deployment: `aws lambda get-function --function-name avesa-canonical-transform-companies-dev`
- Check AWS credentials and permissions
- Ensure correct region (us-east-2)
- Redeploy infrastructure if needed

#### 2. No Raw Data to Transform

**Error:** `No raw data files found for transformation`

**Solutions:**
- Run ingestion pipeline first to generate raw data
- Check raw data availability: `aws s3 ls s3://avesa-data-dev/sitetechnology/raw/connectwise/`
- Verify tenant configuration in DynamoDB
- Check ingestion pipeline logs

#### 3. Permission Denied Errors

**Error:** `Access Denied` when accessing S3 or DynamoDB

**Solutions:**
- Verify Lambda execution role has required permissions
- Check S3 bucket policies
- Ensure DynamoDB table permissions are correct
- Review IAM policies in infrastructure stack

#### 4. Parquet File Generation Failures

**Error:** `Failed to write canonical data to S3`

**Solutions:**
- Check S3 bucket exists and is accessible
- Verify pandas and pyarrow dependencies in Lambda layer
- Check Lambda memory and timeout settings
- Review transformation logic for data type issues

#### 5. Schema Validation Failures

**Error:** Invalid or missing canonical fields

**Solutions:**
- Review canonical mapping files in `mappings/canonical/`
- Check source data structure matches mapping expectations
- Verify field transformation logic in Lambda function
- Update mappings if source schema has changed

### Debugging Commands

```bash
# Check Lambda function configuration
aws lambda get-function-configuration --function-name avesa-canonical-transform-companies-dev

# List recent Lambda invocations
aws logs describe-log-streams --log-group-name /aws/lambda/avesa-canonical-transform-companies-dev --order-by LastEventTime --descending

# Check DynamoDB tenant configuration
aws dynamodb get-item --table-name TenantServices-dev --key '{"tenant_id":{"S":"sitetechnology"},"service":{"S":"connectwise"}}'

# Verify S3 bucket access
aws s3 ls s3://avesa-data-dev/sitetechnology/
```

## Performance Expectations

### Processing Times

- **Small datasets (< 1000 records):** 5-15 seconds
- **Medium datasets (1000-10000 records):** 15-60 seconds  
- **Large datasets (> 10000 records):** 1-5 minutes

### Resource Usage

- **Memory:** 512 MB (configurable)
- **Timeout:** 300 seconds (5 minutes)
- **Concurrent executions:** Up to 10 per function

### File Sizes

- **Raw JSON files:** 1-50 MB typical
- **Canonical parquet files:** 20-80% smaller than JSON
- **Compression ratio:** 2-5x reduction in file size

## Monitoring and Alerting

### CloudWatch Metrics

Monitor these key metrics:
- Lambda invocation count and duration
- Error rate and timeout rate
- S3 PUT operations and data transfer
- DynamoDB read/write operations

### Log Analysis

Key log patterns to monitor:
- `Canonical transformation completed` - Success indicator
- `Failed to process canonical transformation` - Error indicator
- `No raw data files found` - Data availability issue
- `Failed to write canonical data to S3` - Storage issue

## Next Steps

After successful canonical transform testing:

1. **Set up automated testing** in CI/CD pipeline
2. **Configure monitoring and alerting** for production
3. **Implement data quality checks** and validation rules
4. **Test with multiple tenants** and data volumes
5. **Optimize performance** based on actual usage patterns

## Support and Documentation

- **Infrastructure Code:** `infrastructure/stacks/data_pipeline_stack.py`
- **Lambda Function Code:** `src/canonical_transform/lambda_function.py`
- **Canonical Mappings:** `mappings/canonical/`
- **Test Scripts:** `tests/test-canonical-*.py`
- **Deployment Guide:** `docs/DEPLOYMENT.md`