# AVESA Multi-Tenant Data Pipeline

A multi-tenant data ingestion and transformation pipeline supporting 30+ integration services, each with 10-20 endpoints. Built using AWS serverless technologies with canonical data modeling and SCD Type 2 historical tracking.

## Architecture Overview

This pipeline consists of two main components:

1. **Integration-Specific Lambda Functions**: Each integration service has its own dedicated lambda function that handles authentication, API calls, and data extraction for that specific service
2. **Canonical SCD Type 2 Transformation Lambda**: Transforms raw data from all integration services using per-canonical-table mapping files with historical tracking

## Integration Services

The pipeline is designed to support multiple integration services, each with its own lambda function:
- **ConnectWise** (PSA/RMM platform) - `avesa-connectwise-ingestion`
- **ServiceNow** (ITSM platform) - `avesa-servicenow-ingestion`
- **Salesforce** (CRM platform) - `avesa-salesforce-ingestion`
- **Microsoft 365** (Productivity suite) - `avesa-microsoft365-ingestion`
- **And 25+ more services...**

Each integration service lambda function handles:
- Service-specific authentication (OAuth, API keys, etc.)
- Service-specific API structures and rate limiting
- 10-20 different endpoints/tables per service
- Service-specific error handling and retry logic

## Project Structure

```
avesa/
├── infrastructure/          # CDK infrastructure code
│   ├── app.py              # CDK app entry point
│   ├── stacks/             # CDK stack definitions
│   └── constructs/         # Reusable CDK constructs
├── src/                    # Lambda function source code
│   ├── integrations/       # Integration-specific lambda functions
│   │   ├── connectwise/    # ConnectWise-specific ingestion lambda
│   │   ├── servicenow/     # ServiceNow-specific ingestion lambda (future)
│   │   └── salesforce/     # Salesforce-specific ingestion lambda (future)
│   ├── canonical_transform/ # SCD2 transformation lambda
│   └── shared/             # Shared utilities and libraries
├── mappings/               # JSON mapping files (one per canonical table)
│   ├── tickets.json        # Mapping for canonical tickets table
│   ├── time_entries.json   # Mapping for canonical time entries table
│   ├── companies.json      # Mapping for canonical companies table
│   └── contacts.json       # Mapping for canonical contacts table
├── tests/                  # Unit and integration tests
├── scripts/                # Deployment and utility scripts
├── docs/                   # Documentation
└── config/                 # Environment-specific configurations
```

## Data Flow

```
ConnectWise API → ConnectWise Lambda → S3 (Raw Parquet)
ServiceNow API → ServiceNow Lambda → S3 (Raw Parquet)     → Canonical Transform Lambda → S3 (Canonical Parquet) → Data Warehouse
Salesforce API → Salesforce Lambda → S3 (Raw Parquet)
```

## Storage Structure

### Raw Data
```
s3://{bucket}/{tenant_id}/raw/connectwise/{table_name}/{timestamp}.parquet
```

### Canonical Data
```
s3://{bucket}/{tenant_id}/canonical/{canonical_table}/{timestamp}.parquet
```

## Prerequisites

- AWS CLI configured
- Python 3.9+
- Node.js 18+ (for CDK)
- AWS CDK CLI installed (`npm install -g aws-cdk`)

## Quick Start

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Deploy infrastructure:
```bash
cd infrastructure
cdk deploy --all
```

3. Configure tenant settings in DynamoDB `TenantServices` table

## Environment Variables

- `BUCKET_NAME`: S3 bucket for data storage
- `TENANT_SERVICES_TABLE`: DynamoDB table for tenant configuration
- `LAST_UPDATED_TABLE`: DynamoDB table for incremental sync state

## Monitoring

The pipeline includes CloudWatch dashboards, alarms, and SNS notifications for operational monitoring.