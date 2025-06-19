# AVESA Production Account Migration Checklist

## Pre-Migration Checklist

### ✅ Phase 1: Account Setup
- [ ] **Create production AWS account**
  - [ ] Use dedicated email address
  - [ ] Set strong password
  - [ ] Note down 12-digit Account ID
- [ ] **Configure account security**
  - [ ] Enable MFA for root account
  - [ ] Create administrative IAM user
  - [ ] Enable MFA for admin user
  - [ ] Create access keys for admin user
- [ ] **Run setup script**
  - [ ] `./scripts/setup-production-account.sh`
  - [ ] Verify AWS profile: `aws sts get-caller-identity --profile avesa-production`
- [ ] **Set environment variables**
  - [ ] `export CDK_PROD_ACCOUNT=<your-account-id>`
  - [ ] `export CDK_DEFAULT_REGION=us-east-1`
  - [ ] Add to shell profile (.bashrc/.zshrc)

### ✅ Phase 2: Infrastructure Deployment
- [ ] **Bootstrap CDK**
  - [ ] `cd infrastructure`
  - [ ] `cdk bootstrap --profile avesa-production --context environment=prod`
- [ ] **Deploy infrastructure**
  - [ ] `./scripts/deploy-prod.sh`
  - [ ] Verify all stacks deployed successfully
- [ ] **Validate deployment**
  - [ ] Check CloudFormation stacks
  - [ ] Check Lambda functions exist
  - [ ] Check DynamoDB tables exist
  - [ ] Check S3 bucket exists

### ✅ Phase 3: Data Migration
- [ ] **Dry run migration**
  - [ ] `python3 scripts/migrate-production-data.py --dry-run`
  - [ ] Review what will be migrated
- [ ] **Execute migration**
  - [ ] `python3 scripts/migrate-production-data.py --execute`
  - [ ] Monitor for errors
- [ ] **Validate migration**
  - [ ] `python3 scripts/validate-hybrid-setup.py --environment prod`
  - [ ] Check data integrity

### ✅ Phase 4: Testing & Validation
- [ ] **Test Lambda functions**
  - [ ] Invoke test: `aws lambda invoke --function-name avesa-connectwise-ingestion-prod --payload '{}' response.json --profile avesa-production`
  - [ ] Check response for errors
- [ ] **Monitor logs**
  - [ ] `aws logs tail /aws/lambda/avesa-connectwise-ingestion-prod --follow --profile avesa-production`
  - [ ] Verify no errors in logs
- [ ] **Test data pipeline**
  - [ ] Run end-to-end test with sample data
  - [ ] Verify canonical transformation works
- [ ] **Set up monitoring**
  - [ ] Subscribe to SNS alerts
  - [ ] Access CloudWatch dashboard
  - [ ] Test alarm notifications

### ✅ Phase 5: Security & Compliance
- [ ] **Enable security services**
  - [ ] Run `./setup-production-security.sh`
  - [ ] Enable CloudTrail
  - [ ] Enable GuardDuty
  - [ ] Set up billing alarms
- [ ] **Review access controls**
  - [ ] Verify IAM roles and policies
  - [ ] Check cross-account access
  - [ ] Review resource permissions

### ✅ Phase 6: Go-Live Preparation
- [ ] **Update application configuration**
  - [ ] Point to production account resources
  - [ ] Update environment variables
  - [ ] Test configuration changes
- [ ] **Plan cutover**
  - [ ] Schedule maintenance window
  - [ ] Prepare rollback plan
  - [ ] Notify stakeholders
- [ ] **Final validation**
  - [ ] Run full system test
  - [ ] Verify monitoring works
  - [ ] Check backup procedures

## Migration Commands Quick Reference

```bash
# 1. Setup production account
./scripts/setup-production-account.sh

# 2. Deploy infrastructure
./scripts/deploy-prod.sh

# 3. Migrate data (dry run first)
python3 scripts/migrate-production-data.py --dry-run
python3 scripts/migrate-production-data.py --execute

# 4. Validate setup
python3 scripts/validate-hybrid-setup.py --environment prod

# 5. Test Lambda function
aws lambda invoke \
  --function-name avesa-connectwise-ingestion-prod \
  --payload '{"test": true}' \
  response.json \
  --profile avesa-production

# 6. Monitor logs
aws logs tail /aws/lambda/avesa-connectwise-ingestion-prod \
  --follow \
  --profile avesa-production
```

## Troubleshooting Quick Fixes

### CDK Bootstrap Issues
```bash
# If bootstrap fails, try with explicit account/region
cdk bootstrap aws://ACCOUNT-ID/us-east-1 --profile avesa-production
```

### Migration Script Issues
```bash
# Check source tables exist
aws dynamodb list-tables

# Check destination profile
aws sts get-caller-identity --profile avesa-production
```

### Lambda Function Issues
```bash
# Check function exists
aws lambda list-functions --profile avesa-production

# Check function logs
aws logs describe-log-groups --profile avesa-production
```

## Important Account IDs & Resources

- **Current Account (Dev/Staging):** `CDK_DEFAULT_ACCOUNT`
- **Production Account:** `CDK_PROD_ACCOUNT`
- **Region:** `us-east-1`

### Key Resources Created:
- **S3 Bucket:** `data-storage-msp-prod`
- **DynamoDB Tables:** `TenantServices`, `LastUpdated`
- **Lambda Functions:** `avesa-*-prod`
- **CloudWatch Dashboard:** `AVESA-DataPipeline-PROD`
- **SNS Topic:** `avesa-alerts-prod`

## Post-Migration Tasks

- [ ] Update CI/CD pipelines
- [ ] Update documentation
- [ ] Train team on new account structure
- [ ] Set up regular backup procedures
- [ ] Schedule compliance reviews
- [ ] Plan cost optimization reviews

## Emergency Contacts & Rollback

### Rollback Procedure:
1. Stop production traffic
2. Revert DNS/configuration changes
3. Restart services in original account
4. Sync any new data created during cutover
5. Validate original environment

### Emergency Contacts:
- **AWS Support:** [Your support plan contact]
- **Team Lead:** [Contact information]
- **DevOps Engineer:** [Contact information]

---

**Note:** Keep this checklist updated as you progress through the migration. Check off completed items and note any issues or deviations from the plan.