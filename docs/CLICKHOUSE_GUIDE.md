# ClickHouse Comprehensive Guide

This guide provides complete instructions for implementing and deploying ClickHouse integration with the AVESA data pipeline, including both implementation details and cloud deployment procedures.

## Table of Contents

1. [Overview](#overview)
2. [Infrastructure Deployment](#infrastructure-deployment)
3. [ClickHouse Cloud Setup](#clickhouse-cloud-setup)
4. [Lambda Function Implementations](#lambda-function-implementations)
5. [Node.js Backend Integration](#nodejs-backend-integration)
6. [CDK Infrastructure Stack](#cdk-infrastructure-stack)
7. [Testing and Validation](#testing-and-validation)
8. [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)

## Overview

The ClickHouse integration provides real-time analytics capabilities for the AVESA multi-tenant data pipeline. It includes:

- **SCD Type 2 Processing**: Historical data tracking with change detection
- **Tenant Isolation**: Secure multi-tenant data separation
- **Real-time Analytics**: High-performance query capabilities
- **Automated Data Movement**: Lambda-based ETL pipeline
- **Cloud Deployment**: AWS infrastructure with ClickHouse Cloud

## Infrastructure Deployment

### 🎉 Infrastructure Deployment Status: COMPLETE ✅

The AWS infrastructure for ClickHouse integration has been successfully deployed:

#### Deployed Resources:
- **VPC**: `vpc-032adcbdc10427bea` with private subnets for secure connectivity
- **Lambda Functions**: 6 functions for schema initialization, data loading, and SCD processing
- **Step Functions**: Data pipeline orchestration state machine
- **Secrets Manager**: Secret for ClickHouse connection credentials
- **Security Groups**: Configured for ClickHouse Cloud connectivity
- **CloudWatch**: Monitoring, dashboards, and alerting
- **SNS**: Alert notifications

## ClickHouse Cloud Setup

### Step 1: Subscribe to ClickHouse Cloud via AWS Marketplace

#### 1.1 Subscribe to ClickHouse Cloud

1. **Navigate to AWS Marketplace**:
   - Go to [AWS Marketplace - ClickHouse Cloud](https://aws.amazon.com/marketplace/pp/prodview-jettukeanwrfc)
   - Or search for "ClickHouse Cloud" in AWS Marketplace

2. **Subscribe to ClickHouse Cloud**:
   - Click "Continue to Subscribe"
   - Review pricing and terms
   - Click "Subscribe"
   - Wait for subscription confirmation

3. **Launch ClickHouse Cloud Service**:
   - After subscription, click "Continue to Configuration"
   - Select your region: `us-east-2` (Ohio)
   - Click "Continue to Launch"
   - Choose "Launch through EC2" or "Launch from Website"

#### 1.2 ClickHouse Cloud Service Configuration

1. **Create ClickHouse Cloud Organization**:
   - Sign up at [ClickHouse Cloud Console](https://clickhouse.cloud/)
   - Use your AWS account email for consistency
   - Verify your email address

2. **Create a New Service**:
   - Service Name: `avesa-analytics-dev`
   - Cloud Provider: `AWS`
   - Region: `us-east-2` (Ohio)
   - Service Type: `Production` (for better performance)
   - Tier: Start with `Development` for testing, upgrade to `Production` for production workloads

3. **Configure Service Settings**:
   - **Compute**: Start with 1-2 nodes
   - **Storage**: 100GB minimum for development
   - **Backup**: Enable automatic backups
   - **IP Access List**: Configure for your VPC CIDR or specific IPs

#### 1.3 VPC PrivateLink Setup (Recommended)

1. **Enable PrivateLink in ClickHouse Cloud**:
   - In ClickHouse Cloud console, go to your service settings
   - Navigate to "Private Endpoints"
   - Click "Create Private Endpoint"
   - Select AWS region: `us-east-2`

2. **Configure VPC Endpoint in AWS**:
   - Note the service name provided by ClickHouse Cloud
   - In AWS Console, go to VPC → Endpoints
   - Create VPC Endpoint:
     - Service Category: Other endpoint services
     - Service Name: [provided by ClickHouse Cloud]
     - VPC: `vpc-032adcbdc10427bea`
     - Subnets: Select private subnets
     - Security Groups: Use ClickHouse client security group

#### 1.4 Security Configuration

1. **IP Access List** (if not using PrivateLink):
   - Get NAT Gateway public IPs from your VPC
   - Add them to ClickHouse Cloud IP access list
   - Format: `x.x.x.x/32` for specific IPs

2. **Database Users**:
   - Create a service user: `avesa_service`
   - Generate a strong password
   - Grant necessary permissions for data operations

### Step 2: Credentials and Secrets Management

#### 2.1 Update AWS Secrets Manager

Once your ClickHouse Cloud service is running, update the secret with actual connection details:

```bash
# Get the secret ARN (already created)
SECRET_ARN="arn:aws:secretsmanager:us-east-2:123938354448:secret:clickhouse-connection-dev-V9zSgO"

# Update the secret with actual ClickHouse Cloud connection details
aws secretsmanager update-secret \
  --secret-id $SECRET_ARN \
  --secret-string '{
    "host": "your-service.clickhouse.cloud",
    "port": "8443",
    "username": "avesa_service",
    "password": "your-generated-password",
    "database": "default",
    "secure": "true",
    "protocol": "https"
  }' \
  --region us-east-2
```

#### 2.2 Connection Details Format

Your ClickHouse Cloud connection details should follow this format:

```json
{
  "host": "abc123def.us-east-2.aws.clickhouse.cloud",
  "port": "8443",
  "username": "avesa_service", 
  "password": "your-secure-password",
  "database": "default",
  "secure": "true",
  "protocol": "https"
}
```

### Step 3: Database Schema Initialization

#### 3.1 Test Schema Initialization Lambda

Once credentials are configured, test the schema initialization:

```bash
# Invoke the schema initialization Lambda
aws lambda invoke \
  --function-name clickhouse-schema-init-dev \
  --region us-east-2 \
  --payload '{}' \
  response.json

# Check the response
cat response.json
```

#### 3.2 Manual Schema Creation (if needed)

If Lambda initialization fails, you can manually create the schema using ClickHouse client:

```sql
-- Connect to your ClickHouse Cloud service
-- Create shared tables with tenant partitioning

CREATE TABLE shared_companies (
    tenant_id String,
    company_id String,
    name String,
    domain String,
    industry String,
    size_category String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    is_active Boolean DEFAULT true,
    effective_date DateTime64(3) DEFAULT now64(3),
    expiry_date DateTime64(3) DEFAULT toDateTime64('2999-12-31 23:59:59', 3),
    is_current Boolean DEFAULT true,
    record_hash String
) ENGINE = MergeTree()
PARTITION BY tenant_id
ORDER BY (tenant_id, company_id, effective_date)
SETTINGS index_granularity = 8192;

-- Repeat for contacts, tickets, and time_entries tables
-- (See src/clickhouse/schemas/shared_tables.sql for complete schema)
```

## Lambda Function Implementations

### 1. Bulk Loader Lambda

The bulk loader handles initial data loading and full refresh scenarios:

**Key Features:**
- Loads canonical data from S3
- Transforms data for ClickHouse schema
- Implements SCD Type 2 fields
- Bulk inserts with batching for performance
- Tenant isolation and security

**Location**: [`src/clickhouse/data_loader/lambda_function.py`](../src/clickhouse/data_loader/lambda_function.py)

### 2. SCD Type 2 Processor Lambda

Handles incremental updates with change detection and historical tracking:

**Key Features:**
- Loads new canonical data (last 2 hours)
- Compares with existing current records
- Detects changes using record hashing
- Implements SCD Type 2 operations (insert/update/close)
- Maintains historical data integrity

**Location**: [`src/clickhouse/scd_processor/lambda_function.py`](../src/clickhouse/scd_processor/lambda_function.py)

### 3. Schema Initialization Lambda

Creates and manages ClickHouse database schema:

**Key Features:**
- Creates shared tables with tenant partitioning
- Implements SCD Type 2 schema design
- Handles schema migrations and updates
- Validates table structures

**Location**: [`src/clickhouse/schema_init/lambda_function.py`](../src/clickhouse/schema_init/lambda_function.py)

## Node.js Backend Integration

### ClickHouse Client

The Node.js backend provides API access to ClickHouse analytics:

```javascript
const { ClickHouse } = require('clickhouse');

class ClickHouseClient {
    constructor() {
        this.client = new ClickHouse({
            url: process.env.CLICKHOUSE_URL,
            port: process.env.CLICKHOUSE_PORT || 8443,
            debug: process.env.NODE_ENV === 'development',
            basicAuth: {
                username: process.env.CLICKHOUSE_USER,
                password: process.env.CLICKHOUSE_PASSWORD,
            },
            isUseGzip: true,
            format: "json",
            config: {
                session_timeout: 60,
                output_format_json_quote_64bit_integers: 0,
                enable_http_compression: 1,
                database: process.env.CLICKHOUSE_DATABASE
            }
        });
    }

    async queryWithTenantIsolation(query, tenantId, params = {}) {
        // Ensure tenant isolation in all queries
        const tenantSafeQuery = this.addTenantFilter(query, tenantId);
        
        try {
            const result = await this.client.query(tenantSafeQuery, params).toPromise();
            return result;
        } catch (error) {
            console.error('ClickHouse query error:', error);
            throw error;
        }
    }

    addTenantFilter(query, tenantId) {
        // Add tenant_id filter to WHERE clause
        if (query.toLowerCase().includes('where')) {
            return query.replace(/where/i, `WHERE tenant_id = '${tenantId}' AND`);
        } else {
            return query.replace(/from\s+(\w+)/i, `FROM $1 WHERE tenant_id = '${tenantId}'`);
        }
    }

    async getCompanyAnalytics(tenantId, startDate, endDate) {
        const query = `
            SELECT 
                count() as total_companies,
                countIf(status = 'Active') as active_companies,
                avg(number_of_employees) as avg_employees,
                sum(annual_revenue) as total_revenue
            FROM companies_current
            WHERE effective_start_date >= '${startDate}'
            AND effective_start_date <= '${endDate}'
        `;

        return await this.queryWithTenantIsolation(query, tenantId);
    }

    async getTicketTrends(tenantId, period = 30) {
        const query = `
            SELECT 
                toDate(created_date) as date,
                count() as ticket_count,
                countIf(status = 'Closed') as closed_count,
                avg(actual_hours) as avg_hours
            FROM tickets_current
            WHERE created_date >= now() - INTERVAL ${period} DAY
            GROUP BY toDate(created_date)
            ORDER BY date
        `;

        return await this.queryWithTenantIsolation(query, tenantId);
    }
}

module.exports = ClickHouseClient;
```

### Analytics API Routes

```javascript
const express = require('express');
const ClickHouseClient = require('./clickhouse-client');

const router = express.Router();
const clickhouse = new ClickHouseClient();

// Get company analytics
router.get('/companies/analytics/:tenantId', async (req, res) => {
    try {
        const { tenantId } = req.params;
        const { startDate, endDate } = req.query;

        const result = await clickhouse.getCompanyAnalytics(tenantId, startDate, endDate);
        res.json(result);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Get ticket trends
router.get('/tickets/trends/:tenantId', async (req, res) => {
    try {
        const { tenantId } = req.params;
        const { period = '30' } = req.query;

        const result = await clickhouse.getTicketTrends(tenantId, parseInt(period));
        res.json(result);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

module.exports = router;
```

## CDK Infrastructure Stack

The ClickHouse infrastructure is deployed using AWS CDK:

**Location**: [`infrastructure/stacks/clickhouse_stack.py`](../infrastructure/stacks/clickhouse_stack.py)

**Key Components:**
- Lambda functions with proper IAM roles
- Step Functions state machine for orchestration
- Secrets Manager for credentials
- VPC and security groups
- CloudWatch monitoring and alerting
- SNS notifications

## Testing and Validation

### Step 1: Connectivity Testing

Test Lambda connectivity to ClickHouse:

```bash
# Test each Lambda function
for func in schema-init loader-companies loader-contacts loader-tickets loader-time-entries scd-processor; do
  echo "Testing clickhouse-$func-dev..."
  aws lambda invoke \
    --function-name "clickhouse-$func-dev" \
    --region us-east-2 \
    --payload '{"test": true}' \
    "response-$func.json"
  
  echo "Response:"
  cat "response-$func.json"
  echo -e "\n---\n"
done
```

### Step 2: Step Functions Pipeline Testing

```bash
# Start the ClickHouse data pipeline
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-2:123938354448:stateMachine:clickhouse-data-pipeline-dev" \
  --name "test-execution-$(date +%s)" \
  --input '{"test_mode": true}' \
  --region us-east-2
```

### Step 3: Data Loading Testing

```bash
# Test loading data for a specific tenant
aws lambda invoke \
  --function-name clickhouse-loader-companies-dev \
  --region us-east-2 \
  --payload '{
    "tenant_id": "sitetechnology",
    "table_name": "companies",
    "s3_prefix": "sitetechnology/canonical/companies/"
  }' \
  response-load-test.json

cat response-load-test.json
```

### Step 4: Integration with Existing Pipeline

The Lambda functions are configured to access your existing S3 bucket:
- **Bucket**: `data-storage-msp-dev`
- **Access**: Read permissions for canonical data

## Monitoring and Troubleshooting

### CloudWatch Resources

- **Dashboard**: `ClickHouse-Pipeline-dev`
- **Log Groups**: `/aws/lambda/clickhouse-*-dev`
- **Alarms**: Configured for Lambda errors and Step Functions failures
- **SNS Topic**: `arn:aws:sns:us-east-2:123938354448:clickhouse-alerts-dev`

### Common Issues and Solutions

#### 1. Connection Timeout
- **Cause**: VPC PrivateLink configuration issues
- **Solution**: 
  - Verify VPC PrivateLink configuration
  - Check security group rules
  - Ensure ClickHouse Cloud service is running

#### 2. Authentication Errors
- **Cause**: Invalid credentials or permissions
- **Solution**:
  - Verify credentials in Secrets Manager
  - Check ClickHouse Cloud user permissions
  - Ensure IP access list includes your VPC NAT Gateway IPs

#### 3. Schema Creation Failures
- **Cause**: Database permissions or service issues
- **Solution**:
  - Check ClickHouse Cloud service status
  - Verify database permissions
  - Review Lambda function logs

#### 4. Data Loading Errors
- **Cause**: S3 access issues or data format problems
- **Solution**:
  - Verify S3 bucket permissions
  - Check canonical data format
  - Review transformation logic

### Debugging Commands

```bash
# Check Lambda logs
aws logs tail /aws/lambda/clickhouse-schema-init-dev --follow --region us-east-2

# Verify Secrets Manager access
aws secretsmanager describe-secret \
  --secret-id arn:aws:secretsmanager:us-east-2:123938354448:secret:clickhouse-connection-dev-V9zSgO \
  --region us-east-2

# Test S3 access
aws s3 ls s3://data-storage-msp-dev/sitetechnology/canonical/ --region us-east-2

# Check Step Functions execution
aws stepfunctions describe-execution \
  --execution-arn "EXECUTION_ARN" \
  --region us-east-2
```

## Next Steps

1. **Complete ClickHouse Cloud setup** following the cloud setup section
2. **Update Secrets Manager** with actual connection credentials
3. **Test schema initialization** using the provided commands
4. **Validate connectivity** with the testing procedures
5. **Run end-to-end pipeline test** to verify complete integration

The infrastructure is ready and waiting for your ClickHouse Cloud service configuration!

## Security Considerations

- **Tenant Isolation**: All queries include tenant_id filters
- **Network Security**: VPC PrivateLink for secure connectivity
- **Credential Management**: AWS Secrets Manager for secure storage
- **Access Control**: IAM roles with least privilege principles
- **Data Encryption**: In-transit and at-rest encryption

## Performance Optimization

- **Partitioning**: Tables partitioned by tenant_id for query performance
- **Indexing**: Optimized ORDER BY clauses for common query patterns
- **Batching**: Bulk operations for efficient data loading
- **Compression**: ClickHouse native compression for storage efficiency
- **Caching**: Query result caching for frequently accessed data

---

**Last Updated:** December 2024  
**Maintained By:** AVESA DevOps Team