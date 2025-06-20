# S3 Data Verification Guide

## Overview

This guide documents the expected S3 file structure and verification procedures for the AVESA data pipeline. After a successful pipeline execution, data should be stored in S3 with a specific structure and format.

## Expected S3 File Structure

### Directory Structure
```
s3://avesa-data-{environment}/
├── {tenant_id}/
│   └── connectwise/
│       ├── companies/
│       │   └── YYYY/MM/DD/
│       │       └── {job_id}/
│       │           ├── companies_chunk_001.json
│       │           ├── companies_chunk_002.json
│       │           └── ...
│       ├── contacts/
│       │   └── YYYY/MM/DD/
│       │       └── {job_id}/
│       │           ├── contacts_chunk_001.json
│       │           └── ...
│       ├── tickets/
│       │   └── YYYY/MM/DD/
│       │       └── {job_id}/
│       │           ├── tickets_chunk_001.json
│       │           └── ...
│       └── entries/
│           └── YYYY/MM/DD/
│               └── {job_id}/
│                   ├── entries_chunk_001.json
│                   └── ...
```

### File Naming Convention
- **Job ID Format**: `job-YYYYMMDD-HHMMSS-{8char_uuid}`
- **File Format**: `{table_name}_chunk_{number}.json`
- **Date Partitioning**: Files are organized by execution date (YYYY/MM/DD)

## Expected Tables and Data

### 1. Companies Table
- **Endpoint**: `company/companies`
- **Expected Records**: 100-500 per tenant
- **Chunk Size**: ~5,000 records per file
- **Key Fields**: `id`, `name`, `status`, `type`

### 2. Contacts Table
- **Endpoint**: `company/contacts`
- **Expected Records**: 200-1,000 per tenant
- **Chunk Size**: ~8,000 records per file
- **Key Fields**: `id`, `firstName`, `lastName`, `company`

### 3. Tickets Table
- **Endpoint**: `service/tickets`
- **Expected Records**: 500-2,000 per tenant
- **Chunk Size**: ~5,000 records per file
- **Key Fields**: `id`, `summary`, `status`, `priority`

### 4. Time Entries Table
- **Endpoint**: `time/entries`
- **Expected Records**: 1,000-5,000 per tenant
- **Chunk Size**: ~10,000 records per file
- **Key Fields**: `id`, `timeStart`, `timeEnd`, `member`

## Verification Procedures

### 1. Automated Verification (via test script)
```bash
# Run end-to-end test with S3 verification
python tests/test-end-to-end-pipeline.py --environment dev

# Check recent pipeline executions and verify S3 data
python tests/test-end-to-end-pipeline.py --environment dev --check-recent

# Test specific environment
python tests/test-end-to-end-pipeline.py --environment staging
```

### 2. Manual S3 Verification
```bash
# List recent job executions
aws s3 ls s3://avesa-data-dev/sitetechnology/connectwise/ --recursive

# Check specific job data
aws s3 ls s3://avesa-data-dev/sitetechnology/connectwise/companies/2024/06/20/job-20240620-143022-abc12345/

# Download and inspect a file
aws s3 cp s3://avesa-data-dev/sitetechnology/connectwise/companies/2024/06/20/job-20240620-143022-abc12345/companies_chunk_001.json ./
```

### 3. Data Quality Checks

#### File Size Validation
- **Minimum file size**: 1KB (indicates data was written)
- **Maximum file size**: 50MB (prevents oversized chunks)
- **Empty files**: Should not exist for successful executions

#### JSON Structure Validation
```json
[
  {
    "id": "12345",
    "field1": "value1",
    "field2": "value2",
    "_metadata": {
      "extracted_at": "2024-06-20T14:30:22Z",
      "tenant_id": "sitetechnology",
      "job_id": "job-20240620-143022-abc12345"
    }
  }
]
```

#### Record Count Validation
- Each file should contain at least 1 record
- Total records across all chunks should match API response counts
- No duplicate records across chunks

## Troubleshooting

### Common Issues

#### 1. No Files Found in S3
**Symptoms**: S3 verification fails, no files in expected paths
**Possible Causes**:
- Pipeline execution failed before data writing
- S3 permissions issues
- Incorrect bucket name or path structure

**Resolution**:
1. Check pipeline execution logs
2. Verify S3 bucket permissions
3. Confirm bucket name matches environment

#### 2. Empty or Malformed Files
**Symptoms**: Files exist but contain no data or invalid JSON
**Possible Causes**:
- API returned no data
- JSON serialization errors
- Chunk processing failures

**Resolution**:
1. Check API response logs
2. Verify tenant credentials
3. Review chunk processor logs

#### 3. Missing Tables
**Symptoms**: Some tables have data, others don't
**Possible Causes**:
- Table-specific processing failures
- Disabled tables in tenant configuration
- API endpoint issues

**Resolution**:
1. Check tenant service configuration
2. Verify table enablement status
3. Test individual table endpoints

## Monitoring and Alerts

### CloudWatch Metrics
- `AVESA/DataPipeline/S3FilesWritten`
- `AVESA/DataPipeline/S3BytesWritten`
- `AVESA/DataPipeline/RecordsProcessed`

### Expected Metrics After Successful Run
- Files written: 4-16 files (1-4 per table)
- Bytes written: 1MB-100MB depending on data volume
- Records processed: 1,000-10,000 depending on tenant size

## Environment-Specific Configurations

### Development Environment
- **Bucket**: `avesa-data-dev`
- **Test Tenant**: `sitetechnology`
- **Expected Tables**: All 4 tables enabled

### Staging Environment
- **Bucket**: `avesa-data-staging`
- **Test Tenant**: `sitetechnology`
- **Expected Tables**: All 4 tables enabled
- **Additional Validation**: Performance metrics

### Production Environment
- **Bucket**: `avesa-data-prod`
- **Multiple Tenants**: Various production tenants
- **Monitoring**: Enhanced alerting and monitoring