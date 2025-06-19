# AVESA Multi-Tenant Data Pipeline

A multi-tenant data ingestion and transformation pipeline supporting 30+ integration services, each with 10-20 endpoints. Built using AWS serverless technologies with canonical data modeling and SCD Type 2 historical tracking.

**Architecture Version:** 2.0.0 - Hybrid Account Strategy with Separate Canonical Transform Functions

## Architecture Overview

This pipeline uses a hybrid AWS account strategy with separate canonical transform functions:

### Hybrid Account Architecture
- **Production Account (563583517998):** Dedicated account for production workloads with enhanced security
- **Current Account (123938354448):** Development and staging environments only (production resources cleaned up June 2025)

### Pipeline Components
1. **Integration-Specific Lambda Functions**: Each integration service has its own dedicated lambda function that handles authentication, API calls, and data extraction for that specific service
2. **Separate Canonical Transform Functions**: Individual Lambda functions per canonical table for optimized processing:
   - `avesa-canonical-transform-tickets-{env}`
   - `avesa-canonical-transform-time-entries-{env}`
   - `avesa-canonical-transform-companies-{env}`
   - `avesa-canonical-transform-contacts-{env}`

### Scheduling Architecture
- **Hourly Ingestion:** ConnectWise ingestion runs every hour (00:00)
- **Staggered Canonical Transforms:**
  - Tickets: 15 minutes past hour (00:15)
  - Time Entries: 20 minutes past hour (00:20)
  - Companies: 25 minutes past hour (00:25)
  - Contacts: 30 minutes past hour (00:30)

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
│   ├── app.py              # CDK app entry point (hybrid account support)
│   ├── stacks/             # CDK stack definitions
│   │   ├── data_pipeline_stack.py      # Main pipeline stack
│   │   ├── monitoring_stack.py         # Monitoring and alerting
│   │   ├── backfill_stack.py          # Backfill infrastructure
│   │   └── cross_account_monitoring.py # Cross-account monitoring
│   └── constructs/         # Reusable CDK constructs
├── src/                    # Lambda function source code
│   ├── integrations/       # Integration-specific lambda functions
│   │   ├── connectwise/    # ConnectWise-specific ingestion lambda
│   │   ├── servicenow/     # ServiceNow-specific ingestion lambda (future)
│   │   └── salesforce/     # Salesforce-specific ingestion lambda (future)
│   ├── canonical_transform/ # Separate SCD2 transformation lambdas
│   │   ├── tickets/        # Tickets canonical transform
│   │   ├── time_entries/   # Time entries canonical transform
│   │   ├── companies/      # Companies canonical transform
│   │   └── contacts/       # Contacts canonical transform
│   ├── backfill/           # Backfill processing functions
│   └── shared/             # Shared utilities and libraries
├── mappings/               # JSON mapping and configuration files
│   ├── integrations/       # Integration service endpoint configurations
│   │   ├── connectwise_endpoints.json    # ConnectWise endpoints and settings
│   │   ├── servicenow_endpoints.json     # ServiceNow endpoints and settings
│   │   └── salesforce_endpoints.json     # Salesforce endpoints and settings
│   ├── canonical_mappings.json # Unified canonical schema definition
│   ├── backfill_config.json    # Backfill configuration per service
│   ├── tickets.json        # Canonical tickets table mapping
│   ├── time_entries.json   # Canonical time entries table mapping
│   ├── companies.json      # Canonical companies table mapping
│   └── contacts.json       # Canonical contacts table mapping
├── scripts/                # Deployment and utility scripts
│   ├── deploy-prod.sh           # Deploy to production account
│   ├── deploy-dev-staging.sh    # Deploy to dev/staging environments
│   ├── deploy-lambda-functions.py # Update Lambda functions
│   ├── test-lambda-functions.py # Test pipeline functionality
│   ├── setup-tenant.py         # Add new tenants
│   ├── setup-service.py        # Add services to tenants
│   ├── trigger-backfill.py     # Run backfill operations
│   └── setup-dev-environment.py # Set up dev environment
├── docs/                   # Documentation
│   ├── HYBRID_ACCOUNT_SETUP_GUIDE.md
│   ├── BACKFILL_STRATEGY.md
│   ├── PRODUCTION_MIGRATION_PHASE5_PHASE6_REPORT.md
│   ├── PRODUCTION_CLEANUP_REPORT.md
│   └── MIGRATION_CHECKLIST.md
├── tests/                  # Unit and integration tests
├── ARCHITECTURE_TRANSFORMATION_SUMMARY.md # This transformation summary
└── README_HYBRID_IMPLEMENTATION.md        # Hybrid implementation details
```

## Data Flow

```
ConnectWise API → ConnectWise Lambda → S3 (Raw Parquet)
ServiceNow API → ServiceNow Lambda → S3 (Raw Parquet)     → Separate Canonical Transform Lambdas → S3 (Canonical Parquet) → Data Warehouse
Salesforce API → Salesforce Lambda → S3 (Raw Parquet)
```

### Canonical Transform Flow (Hourly + Staggered)
```
00:00 - ConnectWise Ingestion
00:15 - Tickets Canonical Transform
00:20 - Time Entries Canonical Transform
00:25 - Companies Canonical Transform
00:30 - Contacts Canonical Transform
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

## Environment Architecture

### Production Account (563583517998)
- **Purpose:** Production workloads with enhanced security
- **Resources:** Clean naming without environment suffixes
  - DynamoDB: `TenantServices`, `LastUpdated`
  - S3: `data-storage-msp-prod`
  - Lambda: `avesa-{service}-prod`

### Current Account (123938354448)
- **Purpose:** Development and staging environments only
- **Status:** ✅ Production resources cleaned up (June 2025)
- **Resources:** Environment-suffixed naming
  - DynamoDB: `TenantServices-{env}`, `LastUpdated-{env}` (dev/staging only)
  - S3: `data-storage-msp-{env}` (dev/staging only)
  - Lambda: `avesa-{service}-{env}` (dev/staging only)

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
./scripts/deploy-dev-staging.sh --environment dev
```

3. Create a tenant:
```bash
python scripts/setup-tenant.py \
  --tenant-id "example-tenant" \
  --company-name "Example Company" \
  --environment dev
```

4. Add services to the tenant:
```bash
# Add ConnectWise service
python scripts/setup-service.py \
  --tenant-id "example-tenant" \
  --service connectwise \
  --connectwise-url "https://api-na.myconnectwise.net" \
  --company-id "YourCompanyID" \
  --public-key "your-public-key" \
  --private-key "your-private-key" \
  --client-id "your-client-id" \
  --environment dev

# Add ServiceNow service (optional)
python scripts/setup-service.py \
  --tenant-id "example-tenant" \
  --service servicenow \
  --servicenow-instance "https://yourinstance.service-now.com" \
  --servicenow-username "your-username" \
  --servicenow-password "your-password" \
  --environment dev
```

5. Test the pipeline:
```bash
aws lambda invoke \
  --function-name avesa-connectwise-ingestion-dev \
  --payload '{"tenant_id": "example-tenant"}' \
  response.json
```

## Environment Variables

### Production Account (563583517998)
- `BUCKET_NAME`: `data-storage-msp-prod`
- `TENANT_SERVICES_TABLE`: `TenantServices`
- `LAST_UPDATED_TABLE`: `LastUpdated`
- `CDK_PROD_ACCOUNT`: `563583517998`

### Development/Staging (Current Account)
- `BUCKET_NAME`: `data-storage-msp-{env}`
- `TENANT_SERVICES_TABLE`: `TenantServices-{env}`
- `LAST_UPDATED_TABLE`: `LastUpdated-{env}`
- `CDK_DEFAULT_ACCOUNT`: Current account ID

## Key Features

### 🏗️ Hybrid Account Architecture
- **Production Isolation**: Dedicated AWS account for production workloads
- **Development Simplicity**: Dev/staging remain in current account
- **Security Enhancement**: Complete separation of production data
- **Compliance Ready**: Foundation for SOC 2, ISO 27001 certifications

### 🔄 Separate Canonical Transform Functions
- **Independent Scaling**: Each canonical table scales independently
- **Error Isolation**: Failures in one table don't affect others
- **Optimized Processing**: Table-specific optimizations and configurations
- **Parallel Execution**: Multiple transforms can run simultaneously

### ⏰ Hourly Scheduling with Staggered Transforms
- **Simplified Scheduling**: Hourly ingestion instead of 15/30 minute intervals
- **Resource Optimization**: Staggered transforms prevent resource contention
- **Better Monitoring**: Clear separation of processing stages
- **Rate Limit Compliance**: Proper spacing to avoid API rate limits

### 📊 Enhanced Backfill Strategy
- **Automatic Detection**: Detects new services needing backfill
- **Chunked Processing**: 30-day chunks for efficient processing
- **Progress Tracking**: DynamoDB-based job tracking and resumption
- **Service Integration**: Seamless integration with canonical transforms

## Monitoring

### Production Monitoring
- **CloudWatch Dashboard**: `AVESA-DataPipeline-PROD`
- **Cross-Account Monitoring**: Monitor production from current account
- **SNS Alerts**: `arn:aws:sns:us-east-2:563583517998:avesa-alerts-prod`
- **Function-Level Metrics**: Individual metrics per canonical transform

### Development Monitoring
- **Environment-Specific Dashboards**: Separate monitoring per environment
- **Integrated Alerting**: Consolidated alerts across all environments
- **Log Insights**: Advanced querying and analysis capabilities