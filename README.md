# AVESA Multi-Tenant Data Pipeline

A multi-tenant data ingestion and transformation pipeline supporting 30+ integration services, each with 10-20 endpoints. Built using AWS serverless technologies with canonical data modeling and SCD Type 2 historical tracking.

**Architecture Version:** 2.0.0 - Hybrid Account Strategy with Separate Canonical Transform Functions

## Architecture Overview

This pipeline uses a hybrid AWS account strategy with separate canonical transform functions:

### Hybrid Account Architecture
- **Production Account (YOUR_PRODUCTION_ACCOUNT_ID):** Dedicated account for production workloads with enhanced security
- **Development Account (YOUR_DEV_ACCOUNT_ID):** Development and staging environments only (production resources cleaned up June 2025)

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
│   ├── canonical/              # Canonical transformation mappings
│   │   ├── tickets.json        # Canonical tickets table mapping
│   │   ├── time_entries.json   # Canonical time entries table mapping
│   │   ├── companies.json      # Canonical companies table mapping
│   │   └── contacts.json       # Canonical contacts table mapping
│   └── backfill_config.json    # Backfill configuration per service
├── scripts/                # Deployment and utility scripts
│   ├── deploy.sh                # Unified deployment script for all environments
│   ├── deploy-lambda-functions.py # Update Lambda functions
│   ├── test-lambda-functions.py # Test pipeline functionality
│   ├── setup-service.py        # Add services to tenants (creates tenant if needed)
│   └── trigger-backfill.py     # Run backfill operations
├── docs/                   # Documentation
│   ├── AWS_CREDENTIALS_SETUP_GUIDE.md  # AWS credentials for GitHub Actions
│   ├── BACKFILL_STRATEGY.md
│   ├── DEPLOYMENT.md
│   ├── DEPLOYMENT_VERIFICATION.md
│   ├── DEV_ENVIRONMENT_SETUP_GUIDE.md
│   ├── GITHUB_SECRETS_QUICK_SETUP.md   # Quick GitHub secrets setup
│   ├── MANUAL_DEPLOYMENT_GUIDE.md
│   └── PROD_ENVIRONMENT_SETUP_GUIDE.md
└── tests/                  # Unit and integration tests
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

### Production Account (YOUR_PRODUCTION_ACCOUNT_ID)
- **Purpose:** Production workloads with enhanced security
- **Resources:** Clean naming without environment suffixes
  - DynamoDB: `TenantServices`, `LastUpdated`
  - S3: `data-storage-msp-prod`
  - Lambda: `avesa-{service}-prod`

### Development Account (YOUR_DEV_ACCOUNT_ID)
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

### GitHub Actions Deployment Setup

For production deployments via GitHub Actions, you'll need to configure AWS credentials as repository secrets:

- **Quick Setup**: See [`GITHUB_SECRETS_QUICK_SETUP.md`](docs/GITHUB_SECRETS_QUICK_SETUP.md) for rapid configuration
- **Complete Guide**: See [`AWS_CREDENTIALS_SETUP_GUIDE.md`](docs/AWS_CREDENTIALS_SETUP_GUIDE.md) for detailed setup and security best practices
- **Manual Deployment**: See [`MANUAL_DEPLOYMENT_GUIDE.md`](docs/MANUAL_DEPLOYMENT_GUIDE.md) for production deployment procedures

**Required GitHub Secrets:**
- `AWS_ACCESS_KEY_ID_PROD` - AWS Access Key ID for production deployment
- `AWS_SECRET_ACCESS_KEY_PROD` - AWS Secret Access Key for production deployment
- `AWS_PROD_DEPLOYMENT_ROLE_ARN` - Production deployment role ARN (for cross-account setup)

## Quick Start

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Deploy infrastructure:
```bash
./scripts/deploy.sh --environment dev
```

3. Add services to tenants (creates tenant automatically):
```bash
# Add ConnectWise service (creates tenant if it doesn't exist)
python scripts/setup-service.py \
  --tenant-id "example-tenant" \
  --company-name "Example Company" \
  --service connectwise \
  --environment dev

# Add ServiceNow service (optional)
python scripts/setup-service.py \
  --tenant-id "example-tenant" \
  --company-name "Example Company" \
  --service servicenow \
  --environment dev
```

The script will prompt for service-specific credentials interactively, or you can provide them via environment variables:
```bash
# Using environment variables for ConnectWise
export CONNECTWISE_API_URL="https://api-na.myconnectwise.net"
export CONNECTWISE_COMPANY_ID="YourCompanyID"
export CONNECTWISE_PUBLIC_KEY="your-public-key"
export CONNECTWISE_PRIVATE_KEY="your-private-key"
export CONNECTWISE_CLIENT_ID="your-client-id"

python scripts/setup-service.py \
  --tenant-id "example-tenant" \
  --company-name "Example Company" \
  --service connectwise \
  --environment dev
```

4. Test the pipeline:
```bash
aws lambda invoke \
  --function-name avesa-connectwise-ingestion-dev \
  --payload '{"tenant_id": "example-tenant"}' \
  response.json
```

## Environment Variables

### Production Account (YOUR_PRODUCTION_ACCOUNT_ID)
- `BUCKET_NAME`: `data-storage-msp-prod`
- `TENANT_SERVICES_TABLE`: `TenantServices`
- `LAST_UPDATED_TABLE`: `LastUpdated`
- `CDK_PROD_ACCOUNT`: `YOUR_PRODUCTION_ACCOUNT_ID`

### Development/Staging (YOUR_DEV_ACCOUNT_ID)
- `BUCKET_NAME`: `data-storage-msp-{env}`
- `TENANT_SERVICES_TABLE`: `TenantServices-{env}`
- `LAST_UPDATED_TABLE`: `LastUpdated-{env}`
- `CDK_DEFAULT_ACCOUNT`: `YOUR_DEV_ACCOUNT_ID`

## Key Features

### 🏗️ Hybrid Account Architecture
- **Production Isolation**: Dedicated AWS account for production workloads
- **Development Simplicity**: Dev/staging remain in development account
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
- **Cross-Account Monitoring**: Monitor production from development account
- **SNS Alerts**: `arn:aws:sns:us-east-2:YOUR_PRODUCTION_ACCOUNT_ID:avesa-alerts-prod`
- **Function-Level Metrics**: Individual metrics per canonical transform

### Development Monitoring
- **Environment-Specific Dashboards**: Separate monitoring per environment
- **Integrated Alerting**: Consolidated alerts across all environments
- **Log Insights**: Advanced querying and analysis capabilities