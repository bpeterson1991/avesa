# AVESA Production Environment Setup Guide

This guide provides comprehensive instructions for setting up, configuring, and managing the AVESA data pipeline in the production environment. This guide complements the [`DEV_ENVIRONMENT_SETUP_GUIDE.md`](DEV_ENVIRONMENT_SETUP_GUIDE.md) and follows production security best practices.

## Overview

The AVESA production environment uses a **dedicated AWS account** with enhanced security, monitoring, and compliance features:

- **Production AWS Account**: Isolated from development/staging environments
- **Enhanced Security**: Multi-factor authentication, least-privilege access, and audit logging
- **Cross-Account Monitoring**: Centralized monitoring from development account
- **Automated Backups**: Point-in-time recovery and disaster recovery capabilities
- **Compliance Ready**: Audit trails, encryption at rest and in transit

## Prerequisites

### AWS Account Requirements

- **Production AWS Account**: Dedicated account separate from development (configured via CDK_PROD_ACCOUNT)
- **Cross-Account Role Access**: Configured between development and production accounts
- **AWS CLI**: Version 2.x with production profile configured
- **Multi-Factor Authentication**: Required for all production access
- **Region**: us-east-2 (primary), with disaster recovery in us-west-2

### Required Tools

- **AWS CLI 2.x** - [Installation Guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
- **Node.js 18+** - Required for AWS CDK
- **Python 3.9+** - For Lambda functions and deployment scripts
- **AWS CDK CLI** - Install globally: `npm install -g aws-cdk`

### Security Prerequisites

- **MFA Device**: Hardware or software MFA token configured
- **Production Access Role**: `arn:aws:iam::YOUR_PRODUCTION_ACCOUNT_ID:role/AVESAProductionAccess`
- **Deployment Role**: `arn:aws:iam::YOUR_PRODUCTION_ACCOUNT_ID:role/AVESADeploymentRole`
- **Monitoring Role**: Cross-account role for monitoring access

## AWS Account Setup

### 1. Production Account Configuration

#### Create Production AWS Profile

```bash
# Configure the production profile with cross-account role access
aws configure --profile avesa-production

# Set up cross-account role assumption
aws configure set role_arn arn:aws:iam::YOUR_PRODUCTION_ACCOUNT_ID:role/AVESAProductionAccess --profile avesa-production
aws configure set source_profile avesa-production --profile avesa-production
aws configure set mfa_serial arn:aws:iam::YOUR_PRODUCTION_ACCOUNT_ID:mfa/YOUR_MFA_DEVICE --profile avesa-production
```

#### Alternative: Direct Production Account Access

```bash
# If you have direct access to the production account
aws configure --profile avesa-production
# AWS Access Key ID: [Production Account Access Key]
# AWS Secret Access Key: [Production Account Secret Key]
# Default region name: us-east-2
# Default output format: json
```

### 2. Cross-Account Role Setup

#### In Production Account (YOUR_PRODUCTION_ACCOUNT_ID)

Create the deployment role that allows access from the development account:

```bash
# Switch to production account context
export AWS_PROFILE=avesa-production

# Create the deployment role (this should be done via CloudFormation/CDK)
aws iam create-role \
  --role-name AVESADeploymentRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "AWS": "arn:aws:iam::YOUR_DEV_ACCOUNT_ID:root"
        },
        "Action": "sts:AssumeRole",
        "Condition": {
          "StringEquals": {
            "sts:ExternalId": "avesa-deployment-2024"
          }
        }
      }
    ]
  }'

# Attach necessary policies for deployment
aws iam attach-role-policy \
  --role-name AVESADeploymentRole \
  --policy-arn arn:aws:iam::aws:policy/PowerUserAccess
```

### 3. Environment Variables Configuration

Set the production account ID for CDK deployment:

```bash
# Set production account ID (replace with your actual production account ID)
export CDK_PROD_ACCOUNT=YOUR_PRODUCTION_ACCOUNT_ID

# Verify access to production account
aws sts get-caller-identity --profile avesa-production
```

## Production Deployment

### Quick Start

Deploy the complete production environment:

```bash
# Deploy to production
./scripts/deploy.sh --environment prod --region us-east-2

# Verify deployment
aws cloudformation list-stacks \
  --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE \
  --profile avesa-production \
  --region us-east-2 \
  --query 'StackSummaries[?contains(StackName, `AVESA`)].StackName'
```

### Detailed Deployment Steps

#### 1. Pre-Deployment Validation

```bash
# Validate prerequisites
./scripts/deploy.sh --environment prod --help

# Check production account access
aws sts get-caller-identity --profile avesa-production

# Verify CDK bootstrap status
aws cloudformation describe-stacks \
  --stack-name CDKToolkit \
  --profile avesa-production \
  --region us-east-2
```

#### 2. Infrastructure Deployment

The deployment script automatically handles:

- ✅ **CDK Bootstrap**: Production account CDK initialization
- ✅ **Data Pipeline Stack**: Lambda functions, DynamoDB tables, S3 buckets
- ✅ **Monitoring Stack**: CloudWatch dashboards, alarms, SNS topics
- ✅ **Cross-Account Monitoring**: Monitoring access from development account
- ✅ **Backfill Stack**: Historical data processing capabilities

#### 3. Post-Deployment Configuration

```bash
# Upload mapping configurations
aws s3 sync mappings/ s3://data-storage-msp-prod/mappings/ \
  --profile avesa-production \
  --region us-east-2

# Verify Lambda functions
aws lambda list-functions \
  --profile avesa-production \
  --region us-east-2 \
  --query 'Functions[?contains(FunctionName, `avesa`)].FunctionName'
```

## Security Configuration

### 1. IAM Roles and Policies

#### Lambda Execution Roles

Production Lambda functions use least-privilege IAM roles:

```bash
# View Lambda execution role
aws iam get-role \
  --role-name AVESALambdaExecutionRole-prod \
  --profile avesa-production

# Review attached policies
aws iam list-attached-role-policies \
  --role-name AVESALambdaExecutionRole-prod \
  --profile avesa-production
```

#### Cross-Account Access

```bash
# Verify cross-account monitoring role
aws iam get-role \
  --role-name AVESACrossAccountMonitoring-prod \
  --profile avesa-production
```

### 2. Secrets Management

#### Production Credentials Storage

```bash
# Store production ConnectWise credentials
python scripts/setup-service.py \
  --tenant-id "{tenant-id}" \
  --company-name "Production Company" \
  --service connectwise \
  --environment prod \
  --region us-east-2

# Verify secrets are encrypted
aws secretsmanager describe-secret \
  --secret-id "{tenant-id}/prod" \
  --profile avesa-production \
  --region us-east-2
```

#### Secret Backup and Versioning

```bash
# Verify secret versioning is enabled
aws secretsmanager describe-secret \
  --secret-id "{tenant-id}/prod" \
  --profile avesa-production \
  --region us-east-2 \
  --query 'VersionIdsToStages'

# Create manual backup of current secret version
aws secretsmanager get-secret-value \
  --secret-id "{tenant-id}/prod" \
  --profile avesa-production \
  --region us-east-2 > secret-backup-$(date +%Y%m%d).json
```

### 3. Encryption Configuration

#### S3 Bucket Encryption

```bash
# Verify S3 bucket encryption
aws s3api get-bucket-encryption \
  --bucket data-storage-msp-prod \
  --profile avesa-production \
  --region us-east-2

# Check bucket policy
aws s3api get-bucket-policy \
  --bucket data-storage-msp-prod \
  --profile avesa-production \
  --region us-east-2
```

#### DynamoDB Encryption

```bash
# Verify DynamoDB encryption at rest
aws dynamodb describe-table \
  --table-name TenantServices \
  --profile avesa-production \
  --region us-east-2 \
  --query 'Table.SSEDescription'
```

## Monitoring and Alerting

### 1. CloudWatch Dashboards

Access production monitoring dashboards:

```bash
# List available dashboards
aws cloudwatch list-dashboards \
  --profile avesa-production \
  --region us-east-2 \
  --query 'DashboardEntries[?contains(DashboardName, `AVESA`)].DashboardName'
```

**Key Dashboards:**
- `AVESA-DataPipeline-PROD`: Main production monitoring dashboard
- `ConnectWise-Pipeline-prod`: Service-specific metrics

### 2. Alerting Configuration

#### SNS Topic Setup

```bash
# Get alert topic ARN
aws sns list-topics \
  --profile avesa-production \
  --region us-east-2 \
  --query 'Topics[?contains(TopicArn, `avesa-alerts`)].TopicArn'

# Subscribe to production alerts
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-2:YOUR_PRODUCTION_ACCOUNT_ID:avesa-alerts-prod \
  --protocol email \
  --notification-endpoint production-alerts@yourcompany.com \
  --profile avesa-production \
  --region us-east-2
```

#### Critical Alarms

Production environment includes these critical alarms:

- **Lambda Error Rate**: > 5 errors in 10 minutes
- **Lambda Duration**: > 4 minutes average duration
- **DynamoDB Throttling**: Any read/write throttles
- **Data Freshness**: Data not updated in > 1 hour
- **S3 Access Errors**: Failed S3 operations

### 3. Cross-Account Monitoring

Monitor production from development account:

```bash
# Assume cross-account monitoring role
aws sts assume-role \
  --role-arn arn:aws:iam::YOUR_PRODUCTION_ACCOUNT_ID:role/AVESACrossAccountMonitoring-prod \
  --role-session-name monitoring-session \
  --profile avesa-production

# View production metrics from development account
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Invocations \
  --dimensions Name=FunctionName,Value=avesa-connectwise-ingestion-prod \
  --start-time 2024-01-01T00:00:00Z \
  --end-time 2024-01-01T23:59:59Z \
  --period 3600 \
  --statistics Sum \
  --region us-east-2
```

## Backup and Disaster Recovery

### 1. Automated Backups

#### DynamoDB Point-in-Time Recovery

```bash
# Enable point-in-time recovery
aws dynamodb update-continuous-backups \
  --table-name TenantServices \
  --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true \
  --profile avesa-production \
  --region us-east-2

aws dynamodb update-continuous-backups \
  --table-name LastUpdated \
  --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true \
  --profile avesa-production \
  --region us-east-2
```

#### S3 Cross-Region Replication

```bash
# Verify S3 replication configuration
aws s3api get-bucket-replication \
  --bucket data-storage-msp-prod \
  --profile avesa-production \
  --region us-east-2
```

### 2. Backup Verification

```bash
# List DynamoDB backups
aws dynamodb list-backups \
  --table-name TenantServices \
  --profile avesa-production \
  --region us-east-2

# Check S3 versioning
aws s3api get-bucket-versioning \
  --bucket data-storage-msp-prod \
  --profile avesa-production \
  --region us-east-2
```

### 3. Disaster Recovery Procedures

#### Multi-Region Deployment

```bash
# Deploy to disaster recovery region
./scripts/deploy.sh --environment prod --region us-west-2

# Verify DR deployment
aws cloudformation list-stacks \
  --stack-status-filter CREATE_COMPLETE \
  --profile avesa-production \
  --region us-west-2 \
  --query 'StackSummaries[?contains(StackName, `AVESA`)].StackName'
```

#### Recovery Testing

```bash
# Test Lambda function in DR region
aws lambda invoke \
  --function-name avesa-connectwise-ingestion-prod \
  --payload '{"tenant_id": "{tenant-id}"}' \
  --profile avesa-production \
  --region us-west-2 \
  response.json

# Verify data replication
aws s3 ls s3://data-storage-msp-prod-dr/ \
  --recursive \
  --profile avesa-production \
  --region us-west-2
```

## Production Operations

### 1. Tenant Management

#### Production Tenant Setup

```bash
# Create production tenant
python scripts/setup-service.py \
  --tenant-id "{tenant-id}" \
  --company-name "Client Production Environment" \
  --service connectwise \
  --environment prod \
  --region us-east-2

# Verify tenant configuration
aws dynamodb get-item \
  --table-name TenantServices \
  --key '{"tenant_id":{"S":"{tenant-id}"},"service":{"S":"connectwise"}}' \
  --profile avesa-production \
  --region us-east-2
```

#### Tenant Data Migration

For tenant data migration between environments, use the service setup script to recreate tenant configurations in production:

```bash
# Recreate tenant service configuration in production
python scripts/setup-service.py \
  --tenant-id "{tenant-id}" \
  --company-name "Client Production Environment" \
  --service connectwise \
  --environment prod \
  --region us-east-2

# Verify tenant configuration
aws dynamodb get-item \
  --table-name TenantServices \
  --key '{"tenant_id":{"S":"{tenant-id}"},"service":{"S":"connectwise"}}' \
  --profile avesa-production \
  --region us-east-2
```

### 2. Performance Monitoring

#### Lambda Performance

```bash
# Monitor Lambda performance
aws logs tail /aws/lambda/avesa-connectwise-ingestion-prod \
  --follow \
  --profile avesa-production \
  --region us-east-2

# Get performance metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Duration \
  --dimensions Name=FunctionName,Value=avesa-connectwise-ingestion-prod \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Average,Maximum \
  --profile avesa-production \
  --region us-east-2
```

#### Data Pipeline Health

```bash
# Check pipeline health
python scripts/test-lambda-functions.py \
  --environment prod \
  --region us-east-2 \
  --profile avesa-production \
  --health-check

# Verify data freshness
aws dynamodb scan \
  --table-name LastUpdated \
  --profile avesa-production \
  --region us-east-2 \
  --projection-expression "tenant_id, service, last_updated"
```

### 3. Scaling Configuration

#### Auto Scaling Setup

```bash
# Configure DynamoDB auto scaling
aws application-autoscaling register-scalable-target \
  --service-namespace dynamodb \
  --resource-id table/TenantServices \
  --scalable-dimension dynamodb:table:ReadCapacityUnits \
  --min-capacity 5 \
  --max-capacity 100 \
  --profile avesa-production \
  --region us-east-2

# Set scaling policy
aws application-autoscaling put-scaling-policy \
  --service-namespace dynamodb \
  --resource-id table/TenantServices \
  --scalable-dimension dynamodb:table:ReadCapacityUnits \
  --policy-name TenantServicesReadScalingPolicy \
  --policy-type TargetTrackingScaling \
  --target-tracking-scaling-policy-configuration '{
    "TargetValue": 70.0,
    "PredefinedMetricSpecification": {
      "PredefinedMetricType": "DynamoDBReadCapacityUtilization"
    }
  }' \
  --profile avesa-production \
  --region us-east-2
```

## Troubleshooting

### 1. Common Production Issues

#### Lambda Function Errors

```bash
# Check Lambda function logs
aws logs filter-log-events \
  --log-group-name /aws/lambda/avesa-connectwise-ingestion-prod \
  --filter-pattern "ERROR" \
  --start-time $(date -d '1 hour ago' +%s)000 \
  --profile avesa-production \
  --region us-east-2

# Get function configuration
aws lambda get-function-configuration \
  --function-name avesa-connectwise-ingestion-prod \
  --profile avesa-production \
  --region us-east-2
```

#### DynamoDB Issues

```bash
# Check DynamoDB table status
aws dynamodb describe-table \
  --table-name TenantServices \
  --profile avesa-production \
  --region us-east-2 \
  --query 'Table.TableStatus'

# Monitor throttling
aws cloudwatch get-metric-statistics \
  --namespace AWS/DynamoDB \
  --metric-name ReadThrottles \
  --dimensions Name=TableName,Value=TenantServices \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum \
  --profile avesa-production \
  --region us-east-2
```

#### S3 Access Issues

```bash
# Test S3 access
aws s3 ls s3://data-storage-msp-prod/ \
  --profile avesa-production \
  --region us-east-2

# Check bucket policy
aws s3api get-bucket-policy \
  --bucket data-storage-msp-prod \
  --profile avesa-production \
  --region us-east-2
```

### 2. Performance Troubleshooting

#### Slow Lambda Execution

```bash
# Analyze Lambda performance
aws logs insights start-query \
  --log-group-name /aws/lambda/avesa-connectwise-ingestion-prod \
  --start-time $(date -d '24 hours ago' +%s) \
  --end-time $(date +%s) \
  --query-string 'fields @timestamp, @duration, @requestId | filter @type = "REPORT" | sort @duration desc | limit 20' \
  --profile avesa-production \
  --region us-east-2
```

#### Memory and Timeout Issues

```bash
# Check Lambda memory usage
aws logs insights start-query \
  --log-group-name /aws/lambda/avesa-connectwise-ingestion-prod \
  --start-time $(date -d '24 hours ago' +%s) \
  --end-time $(date +%s) \
  --query-string 'fields @timestamp, @maxMemoryUsed, @memorySize | filter @type = "REPORT" | sort @maxMemoryUsed desc' \
  --profile avesa-production \
  --region us-east-2
```

### 3. Security Incident Response

#### Access Audit

```bash
# Review CloudTrail logs for suspicious activity
aws logs filter-log-events \
  --log-group-name CloudTrail/AVESAProduction \
  --filter-pattern "{ $.eventName = AssumeRole || $.eventName = GetSessionToken }" \
  --start-time $(date -d '24 hours ago' +%s)000 \
  --profile avesa-production \
  --region us-east-2

# Check recent API calls
aws logs insights start-query \
  --log-group-name CloudTrail/AVESAProduction \
  --start-time $(date -d '1 hour ago' +%s) \
  --end-time $(date +%s) \
  --query-string 'fields @timestamp, sourceIPAddress, userIdentity.type, eventName | filter eventName like /Delete/ or eventName like /Put/ | sort @timestamp desc' \
  --profile avesa-production \
  --region us-east-2
```

#### Emergency Access Revocation

```bash
# Disable compromised IAM user (if applicable)
aws iam put-user-policy \
  --user-name CompromisedUser \
  --policy-name DenyAllPolicy \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Deny",
        "Action": "*",
        "Resource": "*"
      }
    ]
  }' \
  --profile avesa-production \
  --region us-east-2

# Update secret with new credentials (manual rotation)
aws secretsmanager update-secret \
  --secret-id "{tenant-id}/prod" \
  --secret-string '{"connectwise": {"company_id": "NEW_VALUE", "public_key": "NEW_VALUE", "private_key": "NEW_VALUE", "client_id": "NEW_VALUE", "api_base_url": "https://api-na.myconnectwise.net"}}' \
  --profile avesa-production \
  --region us-east-2
```

## Compliance and Auditing

### 1. Audit Logging

#### CloudTrail Configuration

```bash
# Verify CloudTrail is enabled
aws cloudtrail describe-trails \
  --profile avesa-production \
  --region us-east-2 \
  --query 'trailList[?contains(Name, `AVESA`)].{Name:Name,IsLogging:IsLogging,S3BucketName:S3BucketName}'

# Check trail status
aws cloudtrail get-trail-status \
  --name AVESAProductionTrail \
  --profile avesa-production \
  --region us-east-2
```

#### Access Logging

```bash
# Review S3 access logs
aws s3 ls s3://avesa-access-logs-prod/ \
  --recursive \
  --profile avesa-production \
  --region us-east-2

# Analyze access patterns
aws logs insights start-query \
  --log-group-name /aws/s3/data-storage-msp-prod \
  --start-time $(date -d '24 hours ago' +%s) \
  --end-time $(date +%s) \
  --query-string 'fields @timestamp, remoteip, operation, key | stats count() by operation' \
  --profile avesa-production \
  --region us-east-2
```

### 2. Compliance Reporting

#### Data Retention Compliance

```bash
# Check S3 lifecycle policies
aws s3api get-bucket-lifecycle-configuration \
  --bucket data-storage-msp-prod \
  --profile avesa-production \
  --region us-east-2

# Verify data encryption compliance
aws s3api get-bucket-encryption \
  --bucket data-storage-msp-prod \
  --profile avesa-production \
  --region us-east-2
```

#### Access Control Audit

```bash
# Generate IAM access report
aws iam generate-service-last-accessed-details \
  --arn arn:aws:iam::YOUR_PRODUCTION_ACCOUNT_ID:role/AVESALambdaExecutionRole-prod \
  --profile avesa-production

# Review role permissions
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::YOUR_PRODUCTION_ACCOUNT_ID:role/AVESALambdaExecutionRole-prod \
  --action-names s3:GetObject,dynamodb:PutItem,secretsmanager:GetSecretValue \
  --profile avesa-production
```

## Maintenance Procedures

### 1. Regular Maintenance Tasks

#### Weekly Health Checks

```bash
# Run comprehensive health check
python scripts/production-health-check.py \
  --environment prod \
  --region us-east-2 \
  --profile avesa-production \
  --report-format json

# Check resource utilization
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name ConcurrentExecutions \
  --start-time $(date -u -d '7 days ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 86400 \
  --statistics Maximum \
  --profile avesa-production \
  --region us-east-2
```

#### Monthly Security Review

```bash
# Review IAM access patterns
aws iam get-account-authorization-details \
  --filter Role \
  --profile avesa-production \
  --query 'Roles[?contains(RoleName, `AVESA`)].{RoleName:RoleName,LastUsed:RoleLastUsed.LastUsedDate}'

# Check for unused resources
aws lambda list-functions \
  --profile avesa-production \
  --region us-east-2 \
  --query 'Functions[?contains(FunctionName, `avesa`)].{FunctionName:FunctionName,LastModified:LastModified}'
```

### 2. Update Procedures

#### Lambda Function Updates

```bash
# Deploy updated Lambda functions
./scripts/deploy.sh --environment prod --region us-east-2

# Verify deployment
aws lambda get-function \
  --function-name avesa-connectwise-ingestion-prod \
  --profile avesa-production \
  --region us-east-2 \
  --query 'Configuration.{Version:Version,LastModified:LastModified,CodeSha256:CodeSha256}'
```

#### Configuration Updates

```bash
# Update environment variables
aws lambda update-function-configuration \
  --function-name avesa-connectwise-ingestion-prod \
  --environment Variables='{ENVIRONMENT=prod,LOG_LEVEL=INFO}' \
  --profile avesa-production \
  --region us-east-2

# Update timeout settings
aws lambda update-function-configuration \
  --function-name avesa-connectwise-ingestion-prod \
  --timeout 900 \
  --profile avesa-production \
  --region us-east-2
```

## Cost Optimization

### 1. Resource Optimization

#### Lambda Cost Analysis

```bash
# Get Lambda cost metrics
aws ce get-cost-and-usage \
  --time-period Start=2024-01-01,End=2024-01-31 \
  --granularity MONTHLY \
  --metrics BlendedCost \
  --group-by Type=DIMENSION,Key=SERVICE \
  --filter '{
    "Dimensions": {
      "Key": "SERVICE",
      "Values": ["AWS Lambda"]
    }
  }' \
  --profile avesa-production
```

#### DynamoDB Cost Optimization

```bash
# Review DynamoDB usage patterns
aws dynamodb describe-table \
  --table-name TenantServices \
  --profile avesa-production \
  --region us-east-2 \
  --query 'Table.{BillingMode:BillingModeSummary.BillingMode,ReadCapacity:ProvisionedThroughput.ReadCapacityUnits,WriteCapacity:ProvisionedThroughput.WriteCapacityUnits}'

# Check for unused indexes
aws dynamodb describe-table \
  --table-name TenantServices \
  --profile avesa-production \
  --region us-east-2 \
  --query 'Table.GlobalSecondaryIndexes[].{IndexName:IndexName,Status:IndexStatus,ReadCapacity:ProvisionedThroughput.ReadCapacityUnits}'
```

### 2. Storage Optimization

#### S3 Storage Classes

```bash
# Review S3 storage class distribution
aws s3api list-objects-v2 \
  --bucket data-storage-msp-prod \
  --query 'Contents[].{Key:Key,StorageClass:StorageClass,Size:Size}' \
  --profile avesa-production \
  --region us-east-2

# Implement lifecycle policies
aws s3api put-bucket-lifecycle-configuration \
  --bucket data-storage-msp-prod \
  --lifecycle-configuration file://s3-lifecycle-policy.json \
  --profile avesa-production \
  --region us-east-2
```

## Integration with Development Environment

### 1. Cross-Account Access

Reference the [`DEPLOYMENT.md`](DEPLOYMENT.md) for detailed deployment procedures and the [`DEV_ENVIRONMENT_SETUP_GUIDE.md`](DEV_ENVIRONMENT_SETUP_GUIDE.md) for development environment setup.

#### Development to Production Promotion

```bash
# Test in development first
./scripts/deploy.sh --environment dev --region us-east-2

# Promote to staging
./scripts/deploy.sh --environment staging --region us-east-2

# Deploy to production
./scripts/deploy.sh --environment prod --region us-east-2
```

### 2. Configuration Consistency

#### Environment Parity

```bash
# Compare configurations between environments
python scripts/compare-environments.py \
  --source dev \
  --target prod \
  --check-lambda-config \
  --check-dynamodb-config \
  --check-s3-config
```

## Support and Escalation

### 1. Production Support Contacts

- **Primary On-Call**: production-oncall@yourcompany.com
- **Secondary Escalation**: engineering-leads@yourcompany.com
- **Security Incidents**: security-team@yourcompany.com

### 2. Emergency Procedures

#### Critical System Failure

1. **Immediate Response**: Check [`DEPLOYMENT.md`](DEPLOYMENT.md) rollback procedures
2. **Escalation**: Contact production on-call team
3. **Communication**: Update status page and stakeholders
4. **Recovery**: Execute disaster recovery procedures

#### Security Incident

1. **Isolation**: Revoke compromised access immediately
2. **Assessment**: Review CloudTrail logs for impact
3. **Containment**: Implement emergency access controls
4. **Recovery**: Rotate credentials and update security policies

### 3. Documentation Updates

When making changes to production:

1. Update this guide with new procedures
2. Update [`DEPLOYMENT.md`](DEPLOYMENT.md) with deployment changes
3. Document lessons learned in incident reports
4. Review and update security procedures quarterly

## Next Steps

After successful production environment setup:

1. **Configure Monitoring Alerts**: Set up email/Slack notifications for critical alarms
2. **Implement Backup Verification**: Schedule regular backup restoration tests
3. **Security Hardening**: Review and implement additional security controls
4. **Performance Optimization**: Monitor and optimize resource utilization
5. **Compliance Audit**: Conduct regular compliance reviews and audits

For additional support and detailed deployment procedures, refer to:
- [`DEPLOYMENT.md`](DEPLOYMENT.md) - Comprehensive deployment guide
- [`DEV_ENVIRONMENT_SETUP_GUIDE.md`](DEV_ENVIRONMENT_SETUP_GUIDE.md) - Development environment setup
- AWS documentation for service-specific configuration details