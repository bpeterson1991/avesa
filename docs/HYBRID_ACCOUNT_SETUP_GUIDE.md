# AVESA Hybrid Account Setup Guide
## Production Account Isolation Implementation

This guide walks you through implementing the hybrid AWS account strategy for AVESA, where production runs in a dedicated AWS account while dev/staging remain in the current account.

## Prerequisites

- [ ] AWS CLI installed and configured
- [ ] Node.js 18+ installed
- [ ] Python 3.9+ installed
- [ ] AWS CDK installed (`npm install -g aws-cdk`)
- [ ] Access to create new AWS accounts

## Phase 1: Account Setup

### Step 1: Create Production AWS Account

1. **Create new AWS account:**
   ```bash
   # Go to AWS Organizations or create standalone account
   # Note the new account ID (e.g., 987654321098)
   ```

2. **Set up AWS CLI profile for production:**
   ```bash
   aws configure --profile avesa-production
   # Enter production account credentials
   ```

3. **Verify access:**
   ```bash
   aws sts get-caller-identity --profile avesa-production
   ```

### Step 2: Set Up Cross-Account IAM Role

1. **In production account, create deployment role:**
   ```bash
   # Create trust policy file
   cat > trust-policy.json << EOF
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": {
           "AWS": "arn:aws:iam::YOUR_CURRENT_ACCOUNT_ID:root"
         },
         "Action": "sts:AssumeRole",
         "Condition": {
           "StringEquals": {
             "sts:ExternalId": "avesa-deployment-key"
           }
         }
       }
     ]
   }
   EOF

   # Create the role
   aws iam create-role \
     --role-name AVESADeploymentRole \
     --assume-role-policy-document file://trust-policy.json \
     --profile avesa-production

   # Attach necessary policies
   aws iam attach-role-policy \
     --role-name AVESADeploymentRole \
     --policy-arn arn:aws:iam::aws:policy/PowerUserAccess \
     --profile avesa-production

   aws iam attach-role-policy \
     --role-name AVESADeploymentRole \
     --policy-arn arn:aws:iam::aws:policy/IAMFullAccess \
     --profile avesa-production
   ```

2. **Update AWS CLI profile to use role:**
   ```bash
   aws configure set role_arn arn:aws:iam::PROD_ACCOUNT_ID:role/AVESADeploymentRole --profile avesa-production
   aws configure set source_profile default --profile avesa-production
   aws configure set external_id avesa-deployment-key --profile avesa-production
   ```

## Phase 2: Infrastructure Deployment

### Step 3: Set Environment Variables

```bash
# Set production account ID
export CDK_PROD_ACCOUNT=987654321098  # Replace with your production account ID
export CDK_DEFAULT_REGION=us-east-1
```

### Step 4: Deploy Infrastructure to Production

1. **Bootstrap CDK in production account:**
   ```bash
   cd infrastructure
   AWS_PROFILE=avesa-production cdk bootstrap --context environment=prod
   ```

2. **Deploy infrastructure:**
   ```bash
   # Using the deployment script
   ./scripts/deploy-prod.sh

   # Or manually
   cd infrastructure
   AWS_PROFILE=avesa-production cdk deploy --context environment=prod --all
   ```

3. **Verify deployment:**
   ```bash
   # Check CloudFormation stacks
   aws cloudformation list-stacks --profile avesa-production

   # Check Lambda functions
   aws lambda list-functions --profile avesa-production

   # Check DynamoDB tables
   aws dynamodb list-tables --profile avesa-production

   # Check S3 bucket
   aws s3 ls --profile avesa-production
   ```

## Phase 3: Data Migration

### Step 5: Create Production Resources

1. **Create S3 bucket:**
   ```bash
   aws s3 mb s3://data-storage-msp-prod --profile avesa-production
   
   # Enable versioning
   aws s3api put-bucket-versioning \
     --bucket data-storage-msp-prod \
     --versioning-configuration Status=Enabled \
     --profile avesa-production
   
   # Enable encryption
   aws s3api put-bucket-encryption \
     --bucket data-storage-msp-prod \
     --server-side-encryption-configuration '{
       "Rules": [
         {
           "ApplyServerSideEncryptionByDefault": {
             "SSEAlgorithm": "AES256"
           }
         }
       ]
     }' \
     --profile avesa-production
   ```

2. **Create DynamoDB tables:**
   ```bash
   # TenantServices table
   aws dynamodb create-table \
     --table-name TenantServices \
     --attribute-definitions AttributeName=tenant_id,AttributeType=S \
     --key-schema AttributeName=tenant_id,KeyType=HASH \
     --billing-mode PAY_PER_REQUEST \
     --profile avesa-production

   # LastUpdated table
   aws dynamodb create-table \
     --table-name LastUpdated \
     --attribute-definitions \
       AttributeName=tenant_id,AttributeType=S \
       AttributeName=service_table,AttributeType=S \
     --key-schema \
       AttributeName=tenant_id,KeyType=HASH \
       AttributeName=service_table,KeyType=RANGE \
     --billing-mode PAY_PER_REQUEST \
     --profile avesa-production
   ```

### Step 6: Migrate Production Data

1. **Run migration in dry-run mode:**
   ```bash
   python3 scripts/migrate-production-data.py --dry-run
   ```

2. **Execute migration:**
   ```bash
   python3 scripts/migrate-production-data.py --execute
   ```

3. **Validate migration:**
   ```bash
   # Check DynamoDB data
   aws dynamodb scan --table-name TenantServices --profile avesa-production
   
   # Check S3 data
   aws s3 ls s3://data-storage-msp-prod --recursive --profile avesa-production
   
   # Check secrets
   aws secretsmanager list-secrets --profile avesa-production
   ```

## Phase 4: Testing and Validation

### Step 7: Test Lambda Functions

1. **Test ConnectWise ingestion:**
   ```bash
   aws lambda invoke \
     --function-name avesa-connectwise-ingestion-prod \
     --payload '{"test": true}' \
     --cli-binary-format raw-in-base64-out \
     response.json \
     --profile avesa-production
   
   cat response.json
   ```

2. **Test canonical transformation:**
   ```bash
   aws lambda invoke \
     --function-name avesa-canonical-transform-tickets-prod \
     --payload '{"test": true}' \
     --cli-binary-format raw-in-base64-out \
     response.json \
     --profile avesa-production
   ```

3. **Monitor logs:**
   ```bash
   aws logs tail /aws/lambda/avesa-connectwise-ingestion-prod --follow --profile avesa-production
   ```

### Step 8: Validate End-to-End Pipeline

1. **Trigger a full pipeline run:**
   ```bash
   # Add a test tenant
   python3 scripts/setup-tenant-only.py \
     --tenant-id test-prod \
     --company-name "Test Production Company" \
     --environment prod \
     --region us-east-1
   
   # Add ConnectWise service
   python3 scripts/setup-service.py \
     --tenant-id test-prod \
     --service connectwise \
     --environment prod \
     --connectwise-url "https://api-na.myconnectwise.net/v4_6_release/apis/3.0" \
     --company-id "TestCompany" \
     --public-key "test-public-key" \
     --private-key "test-private-key" \
     --client-id "test-client-id"
   ```

2. **Monitor pipeline execution:**
   ```bash
   # Check CloudWatch metrics
   aws cloudwatch get-metric-statistics \
     --namespace AWS/Lambda \
     --metric-name Invocations \
     --dimensions Name=FunctionName,Value=avesa-connectwise-ingestion-prod \
     --start-time 2024-01-01T00:00:00Z \
     --end-time 2024-01-01T23:59:59Z \
     --period 3600 \
     --statistics Sum \
     --profile avesa-production
   ```

## Phase 5: CI/CD Setup

### Step 9: Configure GitHub Actions

1. **Set up GitHub secrets:**
   ```bash
   # In your GitHub repository settings, add these secrets:
   # AWS_ACCESS_KEY_ID_PROD
   # AWS_SECRET_ACCESS_KEY_PROD
   # AWS_PROD_DEPLOYMENT_ROLE_ARN
   # SLACK_WEBHOOK_URL (optional)
   ```

2. **Test GitHub Actions workflow:**
   ```bash
   # Push changes to main branch or trigger manually
   git add .
   git commit -m "Implement hybrid account strategy"
   git push origin main
   ```

### Step 10: Set Up Monitoring

1. **Deploy monitoring stack:**
   ```bash
   cd infrastructure
   AWS_PROFILE=avesa-production cdk deploy ConnectWiseCrossAccountMonitoring-prod --context environment=prod
   ```

2. **Subscribe to alerts:**
   ```bash
   # Get SNS topic ARN from CloudFormation outputs
   TOPIC_ARN=$(aws cloudformation describe-stacks \
     --stack-name ConnectWiseCrossAccountMonitoring-prod \
     --query 'Stacks[0].Outputs[?OutputKey==`AlertTopicArn`].OutputValue' \
     --output text \
     --profile avesa-production)
   
   # Subscribe email to alerts
   aws sns subscribe \
     --topic-arn $TOPIC_ARN \
     --protocol email \
     --notification-endpoint your-email@company.com \
     --profile avesa-production
   ```

3. **Access CloudWatch dashboard:**
   ```bash
   # Open AWS Console and navigate to CloudWatch > Dashboards
   # Look for "AVESA-DataPipeline-PROD" dashboard
   ```

## Phase 6: Cutover Planning

### Step 11: Prepare for Production Cutover

1. **Schedule maintenance window:**
   - Notify stakeholders
   - Plan 4-hour window
   - Prepare rollback procedures

2. **Update application configuration:**
   ```bash
   # Update environment variables in your applications
   export BUCKET_NAME=data-storage-msp-prod
   export TENANT_SERVICES_TABLE=TenantServices
   export LAST_UPDATED_TABLE=LastUpdated
   export ENVIRONMENT=prod
   ```

3. **Test rollback procedures:**
   ```bash
   # Ensure you can quickly revert to original account if needed
   # Test data sync back to original account
   ```

### Step 12: Execute Cutover

1. **Stop production ingestion:**
   ```bash
   # Disable EventBridge rules in original account
   aws events disable-rule --name ConnectWiseIngestionSchedule
   ```

2. **Final data sync:**
   ```bash
   python3 scripts/migrate-production-data.py --execute
   ```

3. **Update DNS/endpoints:**
   ```bash
   # Update any external references to point to production account
   # Update load balancer targets if applicable
   ```

4. **Start production ingestion:**
   ```bash
   # EventBridge rules should already be enabled in production account
   # Verify with:
   aws events list-rules --profile avesa-production
   ```

5. **Monitor for 2 hours:**
   ```bash
   # Watch CloudWatch logs and metrics
   aws logs tail /aws/lambda/avesa-connectwise-ingestion-prod --follow --profile avesa-production
   ```

## Phase 7: Post-Cutover Validation

### Step 13: Validate Production Environment

1. **Check data ingestion:**
   ```bash
   # Verify new data is being ingested
   aws s3 ls s3://data-storage-msp-prod/raw/ --recursive --profile avesa-production
   ```

2. **Check canonical transformation:**
   ```bash
   # Verify canonical data is being created
   aws s3 ls s3://data-storage-msp-prod/canonical/ --recursive --profile avesa-production
   ```

3. **Validate tenant data isolation:**
   ```bash
   # Ensure tenant data is properly isolated
   aws s3 ls s3://data-storage-msp-prod/raw/tenant1/ --profile avesa-production
   aws s3 ls s3://data-storage-msp-prod/raw/tenant2/ --profile avesa-production
   ```

### Step 14: Clean Up Original Account (Optional)

1. **Archive production data in original account:**
   ```bash
   # Move to Glacier or delete after validation period
   aws s3 cp s3://data-storage-msp s3://data-storage-msp-archive --recursive
   ```

2. **Remove production resources:**
   ```bash
   # Delete production Lambda functions, DynamoDB tables, etc.
   # Keep for rollback period (30 days recommended)
   ```

## Troubleshooting

### Common Issues

1. **Cross-account role access denied:**
   ```bash
   # Verify trust policy and external ID
   aws iam get-role --role-name AVESADeploymentRole --profile avesa-production
   ```

2. **CDK bootstrap issues:**
   ```bash
   # Re-bootstrap with explicit account/region
   cdk bootstrap aws://ACCOUNT_ID/REGION --profile avesa-production
   ```

3. **Lambda function timeout:**
   ```bash
   # Check CloudWatch logs for specific errors
   aws logs describe-log-groups --profile avesa-production
   ```

4. **DynamoDB access issues:**
   ```bash
   # Verify IAM permissions and table names
   aws dynamodb describe-table --table-name TenantServices --profile avesa-production
   ```

### Rollback Procedures

If issues occur during cutover:

1. **Immediate rollback:**
   ```bash
   # Re-enable original account EventBridge rules
   aws events enable-rule --name ConnectWiseIngestionSchedule
   
   # Revert DNS/endpoint changes
   # Sync any new data back to original account
   ```

2. **Data recovery:**
   ```bash
   # Restore from backups if needed
   # Use point-in-time recovery for DynamoDB
   ```

## Security Considerations

### Production Account Hardening

1. **Enable security services:**
   ```bash
   # Enable GuardDuty
   aws guardduty create-detector --enable --profile avesa-production
   
   # Enable Config
   aws configservice put-configuration-recorder --configuration-recorder name=default,roleARN=arn:aws:iam::ACCOUNT:role/aws-config-role --profile avesa-production
   
   # Enable CloudTrail
   aws cloudtrail create-trail --name avesa-audit-trail --s3-bucket-name avesa-audit-logs --profile avesa-production
   ```

2. **Set up billing alerts:**
   ```bash
   # Create billing alarm
   aws cloudwatch put-metric-alarm \
     --alarm-name "AVESA-Production-Billing" \
     --alarm-description "Alert when monthly costs exceed threshold" \
     --metric-name EstimatedCharges \
     --namespace AWS/Billing \
     --statistic Maximum \
     --period 86400 \
     --threshold 1000 \
     --comparison-operator GreaterThanThreshold \
     --profile avesa-production
   ```

### Access Control

1. **Implement least privilege:**
   - Review and minimize IAM permissions
   - Use resource-based policies where possible
   - Enable MFA for sensitive operations

2. **Monitor access:**
   - Set up CloudTrail log analysis
   - Create alerts for unusual access patterns
   - Regular access reviews

## Success Criteria

- [ ] Production environment isolated in dedicated AWS account
- [ ] All data successfully migrated without loss
- [ ] Lambda functions operational in production account
- [ ] Cross-account monitoring functional
- [ ] CI/CD pipeline updated and working
- [ ] Zero customer-facing downtime during cutover
- [ ] Security posture improved with account isolation
- [ ] Compliance readiness enhanced

## Next Steps

After successful implementation:

1. **Plan staging account separation** (Phase 2 of full multi-account strategy)
2. **Implement AWS Organizations** for centralized management
3. **Set up centralized logging** with CloudWatch Logs cross-account access
4. **Pursue compliance certifications** (SOC 2, ISO 27001)
5. **Implement disaster recovery** procedures across accounts

## Support

For issues during implementation:
- Check CloudWatch logs for detailed error messages
- Review IAM permissions and trust policies
- Validate network connectivity between accounts
- Consult AWS documentation for service-specific requirements

This hybrid approach provides a solid foundation for production data isolation while maintaining operational simplicity for development workflows.