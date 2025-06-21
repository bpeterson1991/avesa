# ClickHouse Multi-Tenant SaaS Implementation

This directory contains the foundational infrastructure and implementation for ClickHouse Cloud multi-tenant SaaS application with shared tables approach.

## Architecture Overview

The implementation provides:

- **Shared Tables with Tenant Isolation**: Multi-tenant architecture using `tenant_id` partitioning
- **SCD Type 2 Implementation**: Historical data tracking with clean business names
- **Scalable Data Pipeline**: Lambda functions for S3 → ClickHouse data movement
- **Node.js API**: Tenant-aware REST API with automatic isolation
- **Migration Framework**: Tools for upgrading tenants to dedicated schemas
- **Monitoring & Security**: CloudWatch metrics, audit logging, SOC 2 compliance

## Directory Structure

```
src/clickhouse/
├── api/                    # Node.js REST API
│   ├── config/            # ClickHouse client configuration
│   ├── middleware/        # Authentication and tenant isolation
│   ├── routes/           # API endpoints
│   ├── utils/            # Logging and utilities
│   ├── package.json      # Dependencies
│   └── server.js         # Main server file
├── data_loader/          # Lambda for S3 → ClickHouse data loading
├── schema_init/          # Lambda for schema initialization
├── scd_processor/        # Lambda for SCD Type 2 processing
├── migration/            # Tenant migration utilities
├── monitoring/           # CloudWatch configuration
└── README.md            # This file

schemas/
└── clickhouse_shared_tables.sql  # Database schema definitions

infrastructure/stacks/
└── clickhouse_stack.py   # CDK infrastructure stack
```

## Key Features

### 1. Shared Tables with Tenant Isolation

- **Partitioning Strategy**: Tables partitioned by `(tenant_id, toYYYYMM(effective_date))`
- **Row-Level Security**: Application-layer tenant isolation
- **Performance Optimization**: Bloom filter and set indexes for fast queries

### 2. SCD Type 2 Implementation

- **Clean Business Names**: No `_scd` suffixes in table names
- **Automatic Versioning**: `effective_date`, `expiration_date`, `is_current` fields
- **Data Quality**: Hash-based change detection and integrity validation

### 3. Data Pipeline

- **Schema Initialization**: [`schema_init/lambda_function.py`](schema_init/lambda_function.py)
- **Data Loading**: [`data_loader/lambda_function.py`](data_loader/lambda_function.py)
- **SCD Processing**: [`scd_processor/lambda_function.py`](scd_processor/lambda_function.py)
- **Step Functions Orchestration**: Automated pipeline execution

### 4. Node.js API

- **Tenant-Aware Queries**: Automatic `tenant_id` filtering
- **Connection Pooling**: Efficient ClickHouse client management
- **Authentication**: JWT-based with role/permission support
- **Rate Limiting**: Protection against abuse

### 5. Migration Framework

- **Dedicated Schema Upgrade**: [`migration/tenant_migration.js`](migration/tenant_migration.js)
- **Data Validation**: Integrity checks during migration
- **Rollback Support**: Safe migration with rollback capabilities

## Database Schema

### Core Tables

1. **companies**: Company/organization data
2. **contacts**: Contact/user information
3. **tickets**: Service tickets and incidents
4. **time_entries**: Time tracking data

### Schema Features

- **Tenant Partitioning**: All tables include `tenant_id` for isolation
- **SCD Type 2**: Historical tracking with `effective_date`/`expiration_date`
- **Audit Fields**: `source_system`, `last_updated`, `created_date`
- **Data Quality**: `data_hash` for change detection

## Deployment

### Prerequisites

1. **ClickHouse Cloud Account**: Set up ClickHouse Cloud instance
2. **AWS Secrets Manager**: Store ClickHouse credentials
3. **VPC Configuration**: Private subnets for Lambda functions

### Infrastructure Deployment

```bash
# Deploy ClickHouse infrastructure
cd infrastructure
cdk deploy AVESAClickHouse-dev --context environment=dev
```

### Environment Variables

```bash
# ClickHouse Configuration
CLICKHOUSE_SECRET_NAME=clickhouse-connection-dev
AWS_REGION=us-east-2

# API Configuration
JWT_SECRET=your-jwt-secret
NODE_ENV=development
PORT=3000
LOG_LEVEL=info
```

## API Usage

### Authentication

```bash
# Get JWT token (implement your auth service)
TOKEN="your-jwt-token"

# Make authenticated request
curl -H "Authorization: Bearer $TOKEN" \
     -H "X-Tenant-ID: your-tenant-id" \
     http://localhost:3000/api/analytics/dashboard
```

### Example Endpoints

```bash
# Dashboard summary
GET /api/analytics/dashboard?period=30d

# Ticket status distribution
GET /api/analytics/tickets/status?period=7d

# Top companies by ticket volume
GET /api/analytics/companies/top?metric=tickets&limit=10

# Health check
GET /health/detailed
```

## Data Pipeline Execution

### Manual Execution

```bash
# Initialize schema
aws lambda invoke \
  --function-name clickhouse-schema-init-dev \
  --payload '{}' \
  response.json

# Load data for specific table
aws lambda invoke \
  --function-name clickhouse-loader-companies-dev \
  --payload '{"tenant_id": "your-tenant"}' \
  response.json

# Process SCD Type 2
aws lambda invoke \
  --function-name clickhouse-scd-processor-dev \
  --payload '{}' \
  response.json
```

### Step Functions Execution

```bash
# Execute full pipeline
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-2:account:stateMachine:clickhouse-data-pipeline-dev \
  --input '{}'
```

## Monitoring

### CloudWatch Dashboards

- **ClickHouse Overview**: Lambda metrics, Step Functions execution
- **API Performance**: Response times, error rates
- **Data Quality**: SCD integrity, tenant isolation violations

### Key Metrics

- `AWS/Lambda` - Function invocations, errors, duration
- `AWS/StepFunctions` - Execution success/failure rates
- `ClickHouse/Security` - Tenant isolation violations
- `ClickHouse/DataQuality` - Data integrity issues

### Alerts

- Lambda function errors
- Step Functions failures
- API high latency
- Tenant isolation violations

## Security

### Tenant Isolation

- **Application-Level**: Automatic `tenant_id` filtering in queries
- **Validation**: Tenant access verification on each request
- **Audit Logging**: All tenant access logged for compliance

### Network Security

- **VPC PrivateLink**: Secure ClickHouse connectivity
- **Security Groups**: Restricted network access
- **IAM Roles**: Least privilege access

### Data Protection

- **Encryption**: Data encrypted in transit and at rest
- **Access Control**: JWT-based authentication
- **Rate Limiting**: Protection against abuse

## Migration to Dedicated Schemas

### When to Migrate

- Enterprise clients requiring dedicated isolation
- Performance requirements exceed shared table limits
- Compliance requirements mandate dedicated resources

### Migration Process

```javascript
const TenantMigration = require('./migration/tenant_migration');

// Initialize migration
const migration = new TenantMigration(connectionConfig);

// Create dedicated database
const databaseName = await migration.createDedicatedDatabase('tenant-id');

// Migrate data
const results = await migration.migrateTenantData('tenant-id', databaseName);

// Validate migration
const validation = await migration.validateMigration('tenant-id', databaseName);

// Cleanup shared data (after validation)
if (validation.valid) {
  await migration.cleanupSharedData('tenant-id');
}
```

## Performance Optimization

### Query Optimization

- Use appropriate `ORDER BY` clauses for MergeTree engines
- Leverage partition pruning with date ranges
- Utilize bloom filter indexes for string searches

### Scaling Considerations

- Monitor partition sizes and optimize as needed
- Consider dedicated schemas for high-volume tenants
- Implement query result caching for frequently accessed data

## Troubleshooting

### Common Issues

1. **Connection Timeouts**: Check VPC configuration and security groups
2. **Tenant Isolation Violations**: Verify query filtering logic
3. **SCD Integrity Issues**: Run SCD processor to fix data inconsistencies
4. **High Memory Usage**: Optimize query complexity and result sizes

### Debug Commands

```bash
# Check ClickHouse connectivity
aws lambda invoke \
  --function-name clickhouse-schema-init-dev \
  --payload '{}' \
  --log-type Tail \
  response.json

# View API logs
docker logs clickhouse-api

# Check Step Functions execution
aws stepfunctions describe-execution \
  --execution-arn arn:aws:states:region:account:execution:name
```

## Development

### Local Development

```bash
# Start API server
cd api
npm install
npm run dev

# Run tests
npm test
```

### Adding New Tables

1. Update schema in [`schemas/clickhouse_shared_tables.sql`](../schemas/clickhouse_shared_tables.sql)
2. Add mapping in [`mappings/canonical/`](../../mappings/canonical/)
3. Update data loader Lambda functions
4. Add API endpoints as needed

## Contributing

1. Follow existing code patterns and naming conventions
2. Add comprehensive logging for debugging
3. Include error handling and validation
4. Update documentation for new features
5. Test tenant isolation thoroughly

## Support

For issues and questions:

1. Check CloudWatch logs for error details
2. Verify ClickHouse connectivity and credentials
3. Validate tenant configuration in DynamoDB
4. Review API authentication and authorization

---

This implementation provides a production-ready foundation for ClickHouse multi-tenant SaaS with clear upgrade paths for enterprise clients requiring dedicated isolation.