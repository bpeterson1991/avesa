# AVESA Multi-Tenant Data Pipeline

A multi-tenant data ingestion and transformation pipeline supporting 30+ integration services, each with 10-20 endpoints. Built using AWS serverless technologies with canonical data modeling and SCD Type 2 historical tracking.

## Architecture Overview

This pipeline consists of two main components:

1. **Raw Data Ingestion Lambda**: Multi-service lambda functions that pull data from various integration service APIs (ConnectWise, ServiceNow, etc.), flatten JSON, and store as Parquet in S3
2. **Canonical SCD Type 2 Transformation Lambda**: Transforms raw data using per-canonical-table mapping files with historical tracking

## Integration Services

The pipeline is designed to support multiple integration services:
- **ConnectWise** (PSA/RMM platform)
- **ServiceNow** (ITSM platform)
- **Salesforce** (CRM platform)
- **Microsoft 365** (Productivity suite)
- **And 25+ more services...**

Each integration service has its own lambda function that handles 10-20 different endpoints/tables.

## Project Structure

```
avesa/
├── infrastructure/          # CDK infrastructure code
│   ├── app.py              # CDK app entry point
│   ├── stacks/             # CDK stack definitions
│   └── constructs/         # Reusable CDK constructs
├── src/                    # Lambda function source code
│   ├── raw_ingestion/      # Raw data ingestion lambda (per service)
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
Integration APIs → Service-Specific Ingestion Lambdas → S3 (Raw Parquet) → Canonical Transform Lambda → S3 (Canonical Parquet) → Data Warehouse
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