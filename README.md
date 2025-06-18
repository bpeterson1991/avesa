# ConnectWise Data Pipeline

A multi-tenant data ingestion and transformation pipeline for ConnectWise data using AWS serverless technologies.

## Architecture Overview

This pipeline consists of two main components:

1. **Raw Data Ingestion Lambda**: Pulls data from ConnectWise REST API, flattens JSON, and stores as Parquet in S3
2. **Canonical SCD Type 2 Transformation Lambda**: Transforms raw data using mapping files with historical tracking

## Project Structure

```
connectwise-data-pipeline/
├── infrastructure/          # CDK infrastructure code
│   ├── app.py              # CDK app entry point
│   ├── stacks/             # CDK stack definitions
│   └── constructs/         # Reusable CDK constructs
├── src/                    # Lambda function source code
│   ├── raw_ingestion/      # Raw data ingestion lambda
│   ├── canonical_transform/ # SCD2 transformation lambda
│   └── shared/             # Shared utilities and libraries
├── mappings/               # JSON mapping files for transformations
├── tests/                  # Unit and integration tests
├── scripts/                # Deployment and utility scripts
├── docs/                   # Documentation
└── config/                 # Environment-specific configurations
```

## Data Flow

```
ConnectWise API → Raw Ingestion Lambda → S3 (Raw Parquet) → Canonical Transform Lambda → S3 (Canonical Parquet) → Redshift Serverless
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