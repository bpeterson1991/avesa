# AVESA Production Account Migration Checklist

> **✅ MIGRATION COMPLETED** - This checklist is maintained for historical reference. The architectural transformation and production migration to the dedicated AWS account have been successfully completed.

## Completed Migration Phases

### ✅ Phase 1: Account Setup (COMPLETED)
- [x] **Create production AWS account**
  - [x] Use dedicated email address
  - [x] Set strong password
  - [x] Note down 12-digit Account ID
- [x] **Configure account security**
  - [x] Enable MFA for root account
  - [x] Create administrative IAM user
  - [x] Enable MFA for admin user
  - [x] Create access keys for admin user
- [x] **Setup AWS profile**
  - [x] Configure `avesa-production` profile
  - [x] Verify AWS profile: `aws sts get-caller-identity --profile avesa-production`
- [x] **Set environment variables**
  - [x] `export CDK_PROD_ACCOUNT=<your-account-id>`
  - [x] `export CDK_DEFAULT_REGION=us-east-2`
  - [x] Add to shell profile (.bashrc/.zshrc)

### ✅ Phase 2: Infrastructure Deployment (COMPLETED)
- [x] **Bootstrap CDK**
  - [x] `cd infrastructure`
  - [x] `cdk bootstrap --profile avesa-production --context environment=prod`
- [x] **Deploy infrastructure**
  - [x] `./scripts/deploy-prod.sh`
  - [x] Verify all stacks deployed successfully
- [x] **Validate deployment**
  - [x] Check CloudFormation stacks
  - [x] Check Lambda functions exist
  - [x] Check DynamoDB tables exist
  - [x] Check S3 bucket exists

### ✅ Phase 3: Data Migration (COMPLETED)
- [x] **Dry run migration**
  - [x] Tested migration process
  - [x] Reviewed what will be migrated
- [x] **Execute migration**
  - [x] Migrated production data successfully
  - [x] Monitored for errors
- [x] **Validate migration**
  - [x] Validated hybrid setup
  - [x] Checked data integrity

### ✅ Phase 4: Testing & Validation (COMPLETED)
- [x] **Test Lambda functions**
  - [x] Invoke test: `aws lambda invoke --function-name avesa-connectwise-ingestion-prod --payload '{}' response.json --profile avesa-production`
  - [x] Check response for errors
- [x] **Monitor logs**
  - [x] `aws logs tail /aws/lambda/avesa-connectwise-ingestion-prod --follow --profile avesa-production`
  - [x] Verify no errors in logs
- [x] **Test data pipeline**
  - [x] Run end-to-end test with sample data
  - [x] Verify canonical transformation works
- [x] **Set up monitoring**
  - [x] Subscribe to SNS alerts
  - [x] Access CloudWatch dashboard
  - [x] Test alarm notifications

### ✅ Phase 5: Security & Compliance (COMPLETED)
- [x] **Enable security services**
  - [x] Enable CloudTrail
  - [x] Enable GuardDuty
  - [x] Set up billing alarms
- [x] **Review access controls**
  - [x] Verify IAM roles and policies
  - [x] Check cross-account access
  - [x] Review resource permissions

### ✅ Phase 6: Go-Live Preparation (COMPLETED)
- [x] **Update application configuration**
  - [x] Point to production account resources
  - [x] Update environment variables
  - [x] Test configuration changes
- [x] **Plan cutover**
  - [x] Schedule maintenance window
  - [x] Prepare rollback plan
  - [x] Notify stakeholders
- [x] **Final validation**
  - [x] Run full system test
  - [x] Verify monitoring works
  - [x] Check backup procedures

## Current Operational Commands

> **Note:** Migration-specific scripts have been removed as the migration is complete. Use these commands for ongoing operations:

```bash
# Deploy infrastructure updates
./scripts/deploy-prod.sh

# Test Lambda functions
python3 scripts/test-lambda-functions.py --environment prod --region us-east-2

# Deploy Lambda function updates
python3 scripts/deploy-lambda-functions.py --environment prod --region us-east-2

# Set up new tenants
python3 scripts/setup-tenant-only.py --tenant-id "new-tenant" --company-name "Company Name" --environment prod

# Add services to tenants
python3 scripts/setup-service.py --tenant-id "tenant-id" --service connectwise --environment prod

# Trigger backfill operations
python3 scripts/trigger-backfill.py --tenant-id "tenant-id" --environment prod

# Monitor logs
aws logs tail /aws/lambda/avesa-connectwise-ingestion-prod \
  --follow \
  --profile avesa-production
```

## Ongoing Operations

### Lambda Function Management
```bash
# Check function exists
aws lambda list-functions --profile avesa-production

# Check function logs
aws logs describe-log-groups --profile avesa-production

# Test specific function
aws lambda invoke \
  --function-name avesa-connectwise-ingestion-prod \
  --payload '{"test": true}' \
  response.json \
  --profile avesa-production
```

### Infrastructure Management
```bash
# Check CloudFormation stacks
aws cloudformation list-stacks --profile avesa-production

# Check DynamoDB tables
aws dynamodb list-tables --profile avesa-production

# Check S3 bucket
aws s3 ls s3://data-storage-msp-prod --profile avesa-production
```

## Important Account IDs & Resources

- **Current Account (Dev/Staging):** `123938354448`
- **Production Account:** `563583517998`
- **Region:** `us-east-2`

### Key Resources in Production:
- **S3 Bucket:** `data-storage-msp-prod`
- **DynamoDB Tables:** `TenantServices`, `LastUpdated`
- **Lambda Functions:** `avesa-*-prod`
- **CloudWatch Dashboard:** `AVESA-DataPipeline-PROD`
- **SNS Topic:** `avesa-alerts-prod`

## Post-Migration Tasks (COMPLETED)

- [x] Update CI/CD pipelines
- [x] Update documentation
- [x] Train team on new account structure
- [x] Set up regular backup procedures
- [x] Schedule compliance reviews
- [x] Plan cost optimization reviews

## Current Operations

### Available Scripts for Ongoing Operations:
- [`scripts/deploy-prod.sh`](../scripts/deploy-prod.sh) - Deploy infrastructure updates
- [`scripts/test-lambda-functions.py`](../scripts/test-lambda-functions.py) - Test Lambda functions
- [`scripts/deploy-lambda-functions.py`](../scripts/deploy-lambda-functions.py) - Deploy function updates
- [`scripts/setup-tenant-only.py`](../scripts/setup-tenant-only.py) - Set up new tenants
- [`scripts/setup-service.py`](../scripts/setup-service.py) - Add services to tenants
- [`scripts/trigger-backfill.py`](../scripts/trigger-backfill.py) - Trigger backfill operations
- [`scripts/setup-dev-environment.py`](../scripts/setup-dev-environment.py) - Set up dev environment

### Emergency Contacts:
- **AWS Support:** [Your support plan contact]
- **Team Lead:** [Contact information]
- **DevOps Engineer:** [Contact information]

---

**Note:** This migration checklist is maintained for historical reference. The architectural transformation to a hybrid AWS account strategy has been successfully completed. All production workloads are now running in the dedicated production account.