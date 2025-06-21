# AVESA Data Movement Strategy: S3 Canonical → ClickHouse with SCD Type 2

## Executive Summary

This document outlines the comprehensive data movement strategy from canonical S3 data to ClickHouse Cloud using SCD Type 2 format for the multi-tenant SaaS application. The design leverages your existing infrastructure while adding a high-performance analytical layer.

## Architecture Overview

```mermaid
graph TB
    subgraph "Data Sources"
        A[ConnectWise API]
        B[ServiceNow API]
        C[Salesforce API]
    end
    
    subgraph "Current Pipeline"
        D[Raw Data S3<br/>{tenant_id}/raw/]
        E[Canonical Transform<br/>Lambda Functions]
        F[Canonical Data S3<br/>{tenant_id}/canonical/]
    end
    
    subgraph "New Data Movement Layer"
        G[Data Movement Orchestrator<br/>Step Functions]
        H[Bulk Loader Lambda]
        I[SCD Type 2 Processor<br/>Lambda]
        J[Data Validator<br/>Lambda]
    end
    
    subgraph "Target Databases"
        K[ClickHouse Cloud<br/>Analytical Data]
        L[DynamoDB<br/>Transactional Data]
    end
    
    subgraph "Application Layer"
        M[Node.js Backend]
        N[SaaS Frontend]
    end
    
    A --> D
    B --> D
    C --> D
    D --> E
    E --> F
    F --> G
    G --> H
    G --> I
    G --> J
    H --> K
    I --> K
    J --> K
    K --> M
    L --> M
    M --> N
```

## 1. Database Schema Design

### 1.1 ClickHouse Multi-Tenant SCD Type 2 Schema

```sql
-- Companies Table with SCD Type 2
CREATE TABLE companies_scd (
    tenant_id String,
    company_id String,
    company_name String,
    company_identifier Nullable(String),
    company_type Nullable(String),
    status String,
    address_line1 Nullable(String),
    address_line2 Nullable(String),
    city Nullable(String),
    state Nullable(String),
    zip Nullable(String),
    country Nullable(String),
    phone_number Nullable(String),
    website Nullable(String),
    annual_revenue Nullable(Decimal64(2)),
    number_of_employees Nullable(UInt32),
    
    -- SCD Type 2 Fields
    effective_start_date DateTime64(3),
    effective_end_date Nullable(DateTime64(3)),
    is_current Bool,
    record_hash String,
    
    -- Metadata
    source_system String,
    source_table String,
    ingestion_timestamp DateTime64(3),
    last_updated DateTime64(3),
    
    -- Partitioning and Indexing
    partition_date Date MATERIALIZED toDate(effective_start_date)
) ENGINE = MergeTree()
PARTITION BY (tenant_id, partition_date)
ORDER BY (tenant_id, company_id, effective_start_date)
SETTINGS index_granularity = 8192;

-- Contacts Table with SCD Type 2
CREATE TABLE contacts_scd (
    tenant_id String,
    contact_id String,
    company_id String,
    company_name Nullable(String),
    first_name Nullable(String),
    last_name Nullable(String),
    title Nullable(String),
    department Nullable(String),
    default_email_address Nullable(String),
    default_phone_number Nullable(String),
    address_line1 Nullable(String),
    city Nullable(String),
    state Nullable(String),
    zip Nullable(String),
    country Nullable(String),
    
    -- SCD Type 2 Fields
    effective_start_date DateTime64(3),
    effective_end_date Nullable(DateTime64(3)),
    is_current Bool,
    record_hash String,
    
    -- Metadata
    source_system String,
    source_table String,
    ingestion_timestamp DateTime64(3),
    last_updated DateTime64(3),
    
    partition_date Date MATERIALIZED toDate(effective_start_date)
) ENGINE = MergeTree()
PARTITION BY (tenant_id, partition_date)
ORDER BY (tenant_id, contact_id, effective_start_date)
SETTINGS index_granularity = 8192;

-- Tickets Table with SCD Type 2
CREATE TABLE tickets_scd (
    tenant_id String,
    ticket_id String,
    ticket_number Nullable(String),
    summary Nullable(String),
    description Nullable(String),
    status String,
    priority Nullable(String),
    severity Nullable(String),
    company_id Nullable(String),
    company_name Nullable(String),
    contact_id Nullable(String),
    contact_name Nullable(String),
    owner_id Nullable(String),
    owner_name Nullable(String),
    created_date Nullable(DateTime64(3)),
    closed_date Nullable(DateTime64(3)),
    budget_hours Nullable(Decimal64(2)),
    actual_hours Nullable(Decimal64(2)),
    
    -- SCD Type 2 Fields
    effective_start_date DateTime64(3),
    effective_end_date Nullable(DateTime64(3)),
    is_current Bool,
    record_hash String,
    
    -- Metadata
    source_system String,
    source_table String,
    ingestion_timestamp DateTime64(3),
    last_updated DateTime64(3),
    
    partition_date Date MATERIALIZED toDate(effective_start_date)
) ENGINE = MergeTree()
PARTITION BY (tenant_id, partition_date)
ORDER BY (tenant_id, ticket_id, effective_start_date)
SETTINGS index_granularity = 8192;

-- Time Entries Table with SCD Type 2
CREATE TABLE time_entries_scd (
    tenant_id String,
    entry_id String,
    company_id Nullable(String),
    company_name Nullable(String),
    member_id Nullable(String),
    member_name Nullable(String),
    work_type_id Nullable(String),
    work_type_name Nullable(String),
    time_start Nullable(DateTime64(3)),
    time_end Nullable(DateTime64(3)),
    actual_hours Nullable(Decimal64(2)),
    billable_option Nullable(String),
    notes Nullable(String),
    date_entered Nullable(DateTime64(3)),
    
    -- SCD Type 2 Fields
    effective_start_date DateTime64(3),
    effective_end_date Nullable(DateTime64(3)),
    is_current Bool,
    record_hash String,
    
    -- Metadata
    source_system String,
    source_table String,
    ingestion_timestamp DateTime64(3),
    last_updated DateTime64(3),
    
    partition_date Date MATERIALIZED toDate(effective_start_date)
) ENGINE = MergeTree()
PARTITION BY (tenant_id, partition_date)
ORDER BY (tenant_id, entry_id, effective_start_date)
SETTINGS index_granularity = 8192;
```

### 1.2 Current Record Views for Application Queries

```sql
-- Current Companies View
CREATE VIEW companies_current AS
SELECT *
FROM companies_scd
WHERE is_current = 1;

-- Current Contacts View
CREATE VIEW contacts_current AS
SELECT *
FROM contacts_scd
WHERE is_current = 1;

-- Current Tickets View
CREATE VIEW tickets_current AS
SELECT *
FROM tickets_scd
WHERE is_current = 1;

-- Current Time Entries View
CREATE VIEW time_entries_current AS
SELECT *
FROM time_entries_scd
WHERE is_current = 1;
```

## 2. Data Movement Pipeline Architecture

### 2.1 Step Functions Orchestrator

```json
{
  "Comment": "ClickHouse Data Movement Orchestrator",
  "StartAt": "InitializeDataMovement",
  "States": {
    "InitializeDataMovement": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:avesa-clickhouse-initializer-${Environment}",
      "Parameters": {
        "tenant_id.$": "$.tenant_id",
        "table_name.$": "$.table_name",
        "force_full_sync.$": "$.force_full_sync"
      },
      "ResultPath": "$.movement_config",
      "Next": "DetermineMovementStrategy"
    },
    
    "DetermineMovementStrategy": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.movement_config.strategy",
          "StringEquals": "bulk_load",
          "Next": "BulkDataMovement"
        },
        {
          "Variable": "$.movement_config.strategy",
          "StringEquals": "incremental",
          "Next": "IncrementalDataMovement"
        }
      ],
      "Default": "InvalidStrategy"
    },
    
    "BulkDataMovement": {
      "Type": "Map",
      "ItemsPath": "$.movement_config.tables",
      "MaxConcurrency": 4,
      "Parameters": {
        "table_config.$": "$$.Map.Item.Value",
        "tenant_config.$": "$.movement_config.tenant_config",
        "job_id.$": "$.movement_config.job_id"
      },
      "Iterator": {
        "StartAt": "ProcessTableBulk",
        "States": {
          "ProcessTableBulk": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:avesa-clickhouse-bulk-loader-${Environment}",
            "TimeoutSeconds": 900,
            "Retry": [
              {
                "ErrorEquals": ["States.TaskFailed"],
                "IntervalSeconds": 30,
                "MaxAttempts": 3,
                "BackoffRate": 2.0
              }
            ],
            "End": true
          }
        }
      },
      "Next": "ValidateDataMovement"
    },
    
    "IncrementalDataMovement": {
      "Type": "Map",
      "ItemsPath": "$.movement_config.tables",
      "MaxConcurrency": 6,
      "Parameters": {
        "table_config.$": "$$.Map.Item.Value",
        "tenant_config.$": "$.movement_config.tenant_config",
        "job_id.$": "$.movement_config.job_id"
      },
      "Iterator": {
        "StartAt": "ProcessTableIncremental",
        "States": {
          "ProcessTableIncremental": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:avesa-clickhouse-scd-processor-${Environment}",
            "TimeoutSeconds": 600,
            "Retry": [
              {
                "ErrorEquals": ["States.TaskFailed"],
                "IntervalSeconds": 15,
                "MaxAttempts": 3,
                "BackoffRate": 2.0
              }
            ],
            "End": true
          }
        }
      },
      "Next": "ValidateDataMovement"
    },
    
    "ValidateDataMovement": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:avesa-clickhouse-validator-${Environment}",
      "Parameters": {
        "job_id.$": "$.movement_config.job_id",
        "validation_config.$": "$.movement_config.validation_config"
      },
      "End": true
    }
  }
}
```

## 3. Performance Optimization Strategies

### 3.1 Bulk Loading Algorithm

```python
class OptimizedBulkLoader:
    def __init__(self):
        self.batch_size = 50000  # Records per batch
        self.max_memory_mb = 512  # Memory limit per batch
    
    def process_large_dataset(self, data: List[Dict], table_name: str) -> int:
        """Process large datasets in optimized batches."""
        total_processed = 0
        
        # Calculate optimal batch size based on record size
        if data:
            sample_size = len(json.dumps(data[0]).encode('utf-8'))
            optimal_batch_size = min(
                self.batch_size,
                (self.max_memory_mb * 1024 * 1024) // (sample_size * 2)  # 2x safety factor
            )
        else:
            optimal_batch_size = self.batch_size
        
        # Process in batches
        for i in range(0, len(data), optimal_batch_size):
            batch = data[i:i + optimal_batch_size]
            self.process_batch(batch, table_name)
            total_processed += len(batch)
            
            # Log progress
            print(f"Processed {total_processed}/{len(data)} records")
        
        return total_processed
```

### 3.2 Incremental Update Strategy

```python
class IncrementalUpdateStrategy:
    def __init__(self):
        self.change_detection_window = 2  # hours
        self.max_concurrent_updates = 10
    
    def detect_changes(self, new_data: List[Dict], existing_data: Dict) -> Dict:
        """Detect changes using hash comparison."""
        changes = {
            'new_records': [],
            'changed_records': [],
            'unchanged_records': []
        }
        
        for record in new_data:
            record_id = self.get_record_id(record)
            new_hash = self.calculate_hash(record)
            
            if record_id in existing_data:
                existing_hash = existing_data[record_id].get('record_hash')
                if new_hash != existing_hash:
                    changes['changed_records'].append(record)
                else:
                    changes['unchanged_records'].append(record)
            else:
                changes['new_records'].append(record)
        
        return changes
```

## 4. Data Validation and Error Handling

### 4.1 Data Quality Validation

```python
class DataQualityValidator:
    def __init__(self):
        self.validation_rules = {
            'companies': {
                'required_fields': ['tenant_id', 'company_id', 'company_name'],
                'data_types': {
                    'annual_revenue': 'numeric',
                    'number_of_employees': 'integer'
                }
            },
            'contacts': {
                'required_fields': ['tenant_id', 'contact_id', 'company_id'],
                'data_types': {
                    'default_email_address': 'email'
                }
            }
        }
    
    def validate_batch(self, data: List[Dict], table_name: str) -> Dict:
        """Validate a batch of records."""
        validation_result = {
            'valid_records': [],
            'invalid_records': [],
            'validation_errors': []
        }
        
        rules = self.validation_rules.get(table_name, {})
        
        for record in data:
            errors = self.validate_record(record, rules)
            if errors:
                validation_result['invalid_records'].append({
                    'record': record,
                    'errors': errors
                })
            else:
                validation_result['valid_records'].append(record)
        
        return validation_result
```

### 4.2 Error Recovery Mechanisms

```python
class ErrorRecoveryManager:
    def __init__(self):
        self.max_retries = 3
        self.retry_delay_seconds = [30, 60, 120]
        self.dead_letter_queue = "avesa-clickhouse-dlq"
    
    def handle_processing_error(self, error: Exception, context: Dict) -> Dict:
        """Handle processing errors with retry logic."""
        retry_count = context.get('retry_count', 0)
        
        if retry_count < self.max_retries:
            # Schedule retry
            return {
                'action': 'retry',
                'delay_seconds': self.retry_delay_seconds[retry_count],
                'retry_count': retry_count + 1
            }
        else:
            # Send to dead letter queue
            return {
                'action': 'dead_letter',
                'queue': self.dead_letter_queue,
                'error': str(error)
            }
```

## 5. Monitoring and Observability

### 5.1 CloudWatch Metrics

```python
class ClickHouseMetrics:
    def __init__(self):
        self.cloudwatch = boto3.client('cloudwatch')
        self.namespace = 'AVESA/ClickHouse'
    
    def publish_processing_metrics(self, metrics: Dict):
        """Publish processing metrics to CloudWatch."""
        metric_data = [
            {
                'MetricName': 'RecordsProcessed',
                'Value': metrics['records_processed'],
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'TenantId', 'Value': metrics['tenant_id']},
                    {'Name': 'TableName', 'Value': metrics['table_name']}
                ]
            },
            {
                'MetricName': 'ProcessingDuration',
                'Value': metrics['duration_seconds'],
                'Unit': 'Seconds',
                'Dimensions': [
                    {'Name': 'TenantId', 'Value': metrics['tenant_id']},
                    {'Name': 'TableName', 'Value': metrics['table_name']}
                ]
            },
            {
                'MetricName': 'DataFreshness',
                'Value': metrics['data_age_minutes'],
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'TenantId', 'Value': metrics['tenant_id']}
                ]
            }
        ]
        
        self.cloudwatch.put_metric_data(
            Namespace=self.namespace,
            MetricData=metric_data
        )
```

### 5.2 Alerting Configuration

```yaml
alerts:
  data_movement_failure:
    metric: ProcessingErrors
    threshold: 1
    period: 300
    evaluation_periods: 1
    alarm_actions:
      - "arn:aws:sns:us-east-2:123456789012:avesa-alerts"
  
  data_freshness_violation:
    metric: DataFreshness
    threshold: 45  # minutes
    period: 900
    evaluation_periods: 2
    alarm_actions:
      - "arn:aws:sns:us-east-2:123456789012:avesa-alerts"
  
  high_processing_duration:
    metric: ProcessingDuration
    threshold: 600  # seconds
    period: 300
    evaluation_periods: 1
    alarm_actions:
      - "arn:aws:sns:us-east-2:123456789012:avesa-alerts"
```

## 6. Node.js Backend Integration

### 6.1 ClickHouse Client Configuration

```javascript
// clickhouse-client.js
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
}

module.exports = ClickHouseClient;
```

### 6.2 API Endpoints for Analytics

```javascript
// analytics-routes.js
const express = require('express');
const ClickHouseClient = require('./clickhouse-client');

const router = express.Router();
const clickhouse = new ClickHouseClient();

// Get company analytics
router.get('/companies/analytics/:tenantId', async (req, res) => {
    try {
        const { tenantId } = req.params;
        const { startDate, endDate } = req.query;

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

        const result = await clickhouse.queryWithTenantIsolation(query, tenantId);
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

        const result = await clickhouse.queryWithTenantIsolation(query, tenantId);
        res.json(result);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

module.exports = router;
```

## 7. Deployment Strategy

### 7.1 ClickHouse Cloud Setup

```bash
# 1. Subscribe to ClickHouse Cloud via AWS Marketplace
# 2. Create ClickHouse Cloud service
# 3. Configure VPC peering or PrivateLink for secure access
# 4. Set up database and users

# ClickHouse Cloud Configuration
CREATE DATABASE avesa_analytics;

CREATE USER avesa_app_user IDENTIFIED BY 'secure_password';
GRANT SELECT ON avesa_analytics.* TO avesa_app_user;

CREATE USER avesa_etl_user IDENTIFIED BY 'secure_etl_password';
GRANT ALL ON avesa_analytics.* TO avesa_etl_user;
```

### 7.2 CDK Infrastructure Updates

```python
# infrastructure/stacks/clickhouse_stack.py
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_secretsmanager as secrets,
    Duration
)

class ClickHouseStack(Stack):
    def __init__(self, scope, construct_id, **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        
        # ClickHouse connection secrets
        self.clickhouse_secret = secrets.Secret(
            self, "ClickHouseCredentials",
            description="ClickHouse Cloud connection credentials",
            secret_object_value={
                "host": SecretValue.unsafe_plain_text("your-clickhouse-host"),
                "username": SecretValue.unsafe_plain_text("avesa_etl_user"),
                "password": SecretValue.unsafe_plain_text("secure_etl_password"),
                "database": SecretValue.unsafe_plain_text("avesa_analytics")
            }
        )
        
        # Data movement Lambda functions
        self.bulk_loader = self.create_bulk_loader_lambda()
        self.scd_processor = self.create_scd_processor_lambda()
        self.validator = self.create_validator_lambda()
        
        # Step Functions state machine
        self.data_movement_sm = self.create_data_movement_state_machine()
    
    def create_bulk_loader_lambda(self):
        return _lambda.Function(
            self, "ClickHouseBulkLoader",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="bulk_loader.lambda_handler",
            code=_lambda.Code.from_asset("../src/clickhouse"),
            timeout=Duration.minutes(15),
            memory_size=1024,
            environment={
                "CLICKHOUSE_SECRET_ARN": self.clickhouse_secret.secret_arn
            }
        )
```

## 8. Performance Targets and SLAs

### 8.1 Performance Targets

- **Data Freshness**: 15-30 minutes from canonical S3 to ClickHouse
- **Bulk Load Performance**: 100,000+ records per minute per table
- **Query Performance**: Sub-second response for current record queries
- **Concurrent Tenants**: Support 100+ tenants simultaneously
- **Data Consistency**: 99.9% accuracy in SCD Type 2 tracking

### 8.2 Scalability Metrics

- **Storage Efficiency**: 70%+ compression ratio in ClickHouse
- **Query Concurrency**: 1000+ concurrent analytical queries
- **Data Volume**: Support up to 10TB per tenant
- **Processing Throughput**: 10M+ records per hour across all tenants

## 9. Cost Optimization

### 9.1 ClickHouse Cloud Pricing Model

```yaml
cost_optimization:
  clickhouse_cloud:
    pricing_model: "pay_per_use"
    storage_cost: "$0.10 per GB/month"
    compute_cost: "$0.30 per compute hour"
    
  optimization_strategies:
    - partition_pruning: "Reduce scan costs by 80%"
    - compression: "Achieve 70% storage reduction"
    - materialized_views: "Pre-aggregate common queries"
    - query_caching: "Reduce compute costs by 50%"
```

### 9.2 Lambda Cost Optimization

```python
# Optimized Lambda configuration
lambda_config = {
    "bulk_loader": {
        "memory": 1024,  # MB
        "timeout": 900,  # seconds
        "estimated_monthly_cost": "$50-100"
    },
    "scd_processor": {
        "memory": 512,   # MB
        "timeout": 600,  # seconds
        "estimated_monthly_cost": "$30-60"
    }
}
```

## 10. Migration Plan

### 10.1 Phase 1: Infrastructure Setup (Week 1-2)
1. Deploy ClickHouse Cloud instance
2. Create database schemas and tables
3. Set up VPC connectivity and security
4. Deploy Lambda functions and Step Functions

### 10.2 Phase 2: Initial Data Load (Week 3)
1. Perform bulk load for historical data
2. Validate data integrity and completeness
3. Set up monitoring and alerting
4. Performance testing and optimization

### 10.3 Phase 3: Incremental Processing (Week 4)
1. Enable incremental SCD Type 2 processing
2. Integrate with existing canonical pipeline
3. Test 15-30 minute data freshness targets
4. Validate multi-tenant isolation

### 10.4 Phase 4: Application Integration (Week 5-6)
1. Update Node.js backend with ClickHouse client
2. Create analytical API endpoints
3. Implement tenant-aware query patterns
4. Performance testing and optimization

### 10.5 Phase 5: Production Rollout (Week 7-8)
1. Gradual tenant migration to new system
2. Monitor performance and data quality
3. Optimize based on production workloads
4. Documentation and team training

This comprehensive data movement strategy provides a robust, scalable, and cost-effective solution for moving canonical S3 data to ClickHouse Cloud with full SCD Type 2 historical tracking and multi-tenant support.