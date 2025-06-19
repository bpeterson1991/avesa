#!/usr/bin/env python3
"""
Hybrid Account Setup Validation Script

This script validates that the hybrid AWS account setup is working correctly.
It checks infrastructure deployment, data migration, and cross-account access.

Usage:
    python3 scripts/validate-hybrid-setup.py --environment prod
    python3 scripts/validate-hybrid-setup.py --environment dev --check-cross-account
"""

import argparse
import boto3
import json
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from botocore.exceptions import ClientError


class HybridSetupValidator:
    """Validates hybrid AWS account setup for AVESA."""
    
    def __init__(self, environment: str, region: str = "us-east-1"):
        """Initialize validator."""
        self.environment = environment
        self.region = region
        self.errors = []
        self.warnings = []
        
        # Set up AWS clients based on environment
        if environment == "prod":
            # Use production profile
            session = boto3.Session(profile_name='avesa-production')
            self.dynamodb = session.client('dynamodb', region_name=region)
            self.s3 = session.client('s3', region_name=region)
            self.lambda_client = session.client('lambda', region_name=region)
            self.secrets = session.client('secretsmanager', region_name=region)
            self.cloudformation = session.client('cloudformation', region_name=region)
            self.cloudwatch = session.client('cloudwatch', region_name=region)
        else:
            # Use default profile for dev/staging
            self.dynamodb = boto3.client('dynamodb', region_name=region)
            self.s3 = boto3.client('s3', region_name=region)
            self.lambda_client = boto3.client('lambda', region_name=region)
            self.secrets = boto3.client('secretsmanager', region_name=region)
            self.cloudformation = boto3.client('cloudformation', region_name=region)
            self.cloudwatch = boto3.client('cloudwatch', region_name=region)
    
    def validate_infrastructure(self) -> bool:
        """Validate that all infrastructure is deployed correctly."""
        print(f"\n🔍 Validating infrastructure for {self.environment}...")
        success = True
        
        # Check CloudFormation stacks
        success &= self._check_cloudformation_stacks()
        
        # Check Lambda functions
        success &= self._check_lambda_functions()
        
        # Check DynamoDB tables
        success &= self._check_dynamodb_tables()
        
        # Check S3 bucket
        success &= self._check_s3_bucket()
        
        # Check EventBridge rules
        success &= self._check_eventbridge_rules()
        
        return success
    
    def validate_data_migration(self) -> bool:
        """Validate that data migration was successful (prod only)."""
        if self.environment != "prod":
            print("📋 Skipping data migration validation (not production)")
            return True
        
        print("\n🔍 Validating data migration...")
        success = True
        
        # Check DynamoDB data
        success &= self._check_dynamodb_data()
        
        # Check S3 data
        success &= self._check_s3_data()
        
        # Check secrets
        success &= self._check_secrets()
        
        return success
    
    def validate_functionality(self) -> bool:
        """Validate that the system is functioning correctly."""
        print(f"\n🔍 Validating functionality for {self.environment}...")
        success = True
        
        # Test Lambda function invocation
        success &= self._test_lambda_invocation()
        
        # Check CloudWatch logs
        success &= self._check_cloudwatch_logs()
        
        # Check monitoring setup
        success &= self._check_monitoring()
        
        return success
    
    def validate_cross_account_access(self) -> bool:
        """Validate cross-account access (dev/staging only)."""
        if self.environment == "prod":
            print("📋 Skipping cross-account validation (production account)")
            return True
        
        print("\n🔍 Validating cross-account access...")
        success = True
        
        # Check if we can assume production monitoring role
        success &= self._check_cross_account_role()
        
        return success
    
    def _check_cloudformation_stacks(self) -> bool:
        """Check that CloudFormation stacks are deployed."""
        try:
            stacks = self.cloudformation.list_stacks(
                StackStatusFilter=['CREATE_COMPLETE', 'UPDATE_COMPLETE']
            )
            
            expected_stacks = [
                f"ConnectWiseDataPipeline-{self.environment}",
                f"ConnectWiseBackfill-{self.environment}",
                f"ConnectWiseMonitoring-{self.environment}",
                f"ConnectWiseCrossAccountMonitoring-{self.environment}"
            ]
            
            deployed_stacks = [stack['StackName'] for stack in stacks['StackSummaries']]
            
            for expected_stack in expected_stacks:
                if expected_stack in deployed_stacks:
                    print(f"  ✅ Stack {expected_stack} deployed")
                else:
                    self.errors.append(f"Stack {expected_stack} not found")
                    print(f"  ❌ Stack {expected_stack} missing")
            
            return len(self.errors) == 0
            
        except Exception as e:
            self.errors.append(f"Error checking CloudFormation stacks: {e}")
            return False
    
    def _check_lambda_functions(self) -> bool:
        """Check that Lambda functions are deployed."""
        try:
            functions = self.lambda_client.list_functions()
            function_names = [f['FunctionName'] for f in functions['Functions']]
            
            expected_functions = [
                f"avesa-connectwise-ingestion-{self.environment}",
                f"avesa-canonical-transform-tickets-{self.environment}",
                f"avesa-canonical-transform-time-entries-{self.environment}",
                f"avesa-canonical-transform-companies-{self.environment}",
                f"avesa-canonical-transform-contacts-{self.environment}"
            ]
            
            # For production, remove environment suffix
            if self.environment == "prod":
                expected_functions = [name.replace("-prod", "-prod") for name in expected_functions]
            
            for expected_function in expected_functions:
                if any(expected_function in name for name in function_names):
                    print(f"  ✅ Function {expected_function} deployed")
                else:
                    self.errors.append(f"Lambda function {expected_function} not found")
                    print(f"  ❌ Function {expected_function} missing")
            
            return len(self.errors) == 0
            
        except Exception as e:
            self.errors.append(f"Error checking Lambda functions: {e}")
            return False
    
    def _check_dynamodb_tables(self) -> bool:
        """Check that DynamoDB tables exist."""
        try:
            if self.environment == "prod":
                expected_tables = ["TenantServices", "LastUpdated"]
            else:
                expected_tables = [f"TenantServices-{self.environment}", f"LastUpdated-{self.environment}"]
            
            for table_name in expected_tables:
                try:
                    response = self.dynamodb.describe_table(TableName=table_name)
                    status = response['Table']['TableStatus']
                    item_count = response['Table'].get('ItemCount', 0)
                    print(f"  ✅ Table {table_name}: {status} ({item_count} items)")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ResourceNotFoundException':
                        self.errors.append(f"DynamoDB table {table_name} not found")
                        print(f"  ❌ Table {table_name} missing")
                    else:
                        raise e
            
            return len(self.errors) == 0
            
        except Exception as e:
            self.errors.append(f"Error checking DynamoDB tables: {e}")
            return False
    
    def _check_s3_bucket(self) -> bool:
        """Check that S3 bucket exists."""
        try:
            if self.environment == "prod":
                bucket_name = "data-storage-msp-prod"
            else:
                bucket_name = f"data-storage-msp-{self.environment}"
            
            # Check bucket exists
            self.s3.head_bucket(Bucket=bucket_name)
            
            # Check bucket versioning
            versioning = self.s3.get_bucket_versioning(Bucket=bucket_name)
            versioning_status = versioning.get('Status', 'Disabled')
            
            # Check bucket encryption
            try:
                encryption = self.s3.get_bucket_encryption(Bucket=bucket_name)
                encryption_status = "Enabled"
            except ClientError as e:
                if e.response['Error']['Code'] == 'ServerSideEncryptionConfigurationNotFoundError':
                    encryption_status = "Disabled"
                    self.warnings.append(f"S3 bucket {bucket_name} encryption not enabled")
                else:
                    raise e
            
            print(f"  ✅ S3 bucket {bucket_name}: versioning={versioning_status}, encryption={encryption_status}")
            return True
            
        except Exception as e:
            self.errors.append(f"Error checking S3 bucket: {e}")
            return False
    
    def _check_eventbridge_rules(self) -> bool:
        """Check that EventBridge rules are configured."""
        try:
            events_client = boto3.client('events', region_name=self.region)
            if self.environment == "prod":
                session = boto3.Session(profile_name='avesa-production')
                events_client = session.client('events', region_name=self.region)
            
            rules = events_client.list_rules()
            rule_names = [rule['Name'] for rule in rules['Rules']]
            
            expected_rules = [
                "ConnectWiseIngestionSchedule",
                "CanonicalTransformTicketsSchedule",
                "CanonicalTransformTimeEntriesSchedule",
                "CanonicalTransformCompaniesSchedule",
                "CanonicalTransformContactsSchedule"
            ]
            
            for expected_rule in expected_rules:
                if any(expected_rule in name for name in rule_names):
                    print(f"  ✅ EventBridge rule {expected_rule} configured")
                else:
                    self.warnings.append(f"EventBridge rule {expected_rule} not found")
                    print(f"  ⚠️  Rule {expected_rule} missing")
            
            return True
            
        except Exception as e:
            self.warnings.append(f"Error checking EventBridge rules: {e}")
            return True  # Non-critical
    
    def _check_dynamodb_data(self) -> bool:
        """Check that DynamoDB contains migrated data."""
        try:
            # Check TenantServices table
            response = self.dynamodb.scan(
                TableName="TenantServices",
                Select='COUNT'
            )
            tenant_count = response['Count']
            
            # Check LastUpdated table
            response = self.dynamodb.scan(
                TableName="LastUpdated",
                Select='COUNT'
            )
            timestamp_count = response['Count']
            
            print(f"  ✅ TenantServices: {tenant_count} tenants")
            print(f"  ✅ LastUpdated: {timestamp_count} timestamps")
            
            if tenant_count == 0:
                self.warnings.append("No tenants found in TenantServices table")
            
            return True
            
        except Exception as e:
            self.errors.append(f"Error checking DynamoDB data: {e}")
            return False
    
    def _check_s3_data(self) -> bool:
        """Check that S3 contains migrated data."""
        try:
            bucket_name = "data-storage-msp-prod"
            
            # List objects in bucket
            response = self.s3.list_objects_v2(Bucket=bucket_name, MaxKeys=10)
            object_count = response.get('KeyCount', 0)
            
            print(f"  ✅ S3 bucket contains {object_count}+ objects")
            
            if object_count == 0:
                self.warnings.append("No objects found in S3 bucket")
            
            return True
            
        except Exception as e:
            self.errors.append(f"Error checking S3 data: {e}")
            return False
    
    def _check_secrets(self) -> bool:
        """Check that secrets are migrated."""
        try:
            secrets = self.secrets.list_secrets()
            secret_names = [secret['Name'] for secret in secrets['SecretList']]
            
            tenant_secrets = [name for name in secret_names if 'tenant/' in name]
            print(f"  ✅ Found {len(tenant_secrets)} tenant secrets")
            
            if len(tenant_secrets) == 0:
                self.warnings.append("No tenant secrets found")
            
            return True
            
        except Exception as e:
            self.errors.append(f"Error checking secrets: {e}")
            return False
    
    def _test_lambda_invocation(self) -> bool:
        """Test Lambda function invocation."""
        try:
            function_name = f"avesa-connectwise-ingestion-{self.environment}"
            if self.environment == "prod":
                function_name = "avesa-connectwise-ingestion-prod"
            
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps({'test': True})
            )
            
            if response['StatusCode'] == 200:
                payload = json.loads(response['Payload'].read())
                if 'errorMessage' in payload:
                    self.warnings.append(f"Lambda function {function_name} returned error: {payload['errorMessage']}")
                    print(f"  ⚠️  Function {function_name} returned error")
                else:
                    print(f"  ✅ Function {function_name} invoked successfully")
                return True
            else:
                self.errors.append(f"Lambda function {function_name} invocation failed")
                return False
                
        except Exception as e:
            self.errors.append(f"Error testing Lambda invocation: {e}")
            return False
    
    def _check_cloudwatch_logs(self) -> bool:
        """Check that CloudWatch logs are being generated."""
        try:
            logs_client = boto3.client('logs', region_name=self.region)
            if self.environment == "prod":
                session = boto3.Session(profile_name='avesa-production')
                logs_client = session.client('logs', region_name=self.region)
            
            function_name = f"avesa-connectwise-ingestion-{self.environment}"
            if self.environment == "prod":
                function_name = "avesa-connectwise-ingestion-prod"
            
            log_group_name = f"/aws/lambda/{function_name}"
            
            # Check if log group exists
            try:
                logs_client.describe_log_groups(logGroupNamePrefix=log_group_name)
                print(f"  ✅ CloudWatch log group {log_group_name} exists")
                
                # Check for recent log events
                end_time = int(time.time() * 1000)
                start_time = end_time - (24 * 60 * 60 * 1000)  # 24 hours ago
                
                try:
                    events = logs_client.filter_log_events(
                        logGroupName=log_group_name,
                        startTime=start_time,
                        endTime=end_time,
                        limit=1
                    )
                    
                    if events['events']:
                        print(f"  ✅ Recent log events found")
                    else:
                        self.warnings.append(f"No recent log events in {log_group_name}")
                        print(f"  ⚠️  No recent log events")
                        
                except Exception:
                    self.warnings.append(f"Could not read log events from {log_group_name}")
                
                return True
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    self.warnings.append(f"CloudWatch log group {log_group_name} not found")
                    return True  # Non-critical
                else:
                    raise e
                    
        except Exception as e:
            self.warnings.append(f"Error checking CloudWatch logs: {e}")
            return True  # Non-critical
    
    def _check_monitoring(self) -> bool:
        """Check that monitoring is set up."""
        try:
            # Check CloudWatch dashboards
            dashboards = self.cloudwatch.list_dashboards()
            dashboard_names = [d['DashboardName'] for d in dashboards['DashboardEntries']]
            
            expected_dashboard = f"AVESA-DataPipeline-{self.environment.upper()}"
            if expected_dashboard in dashboard_names:
                print(f"  ✅ CloudWatch dashboard {expected_dashboard} exists")
            else:
                self.warnings.append(f"CloudWatch dashboard {expected_dashboard} not found")
                print(f"  ⚠️  Dashboard {expected_dashboard} missing")
            
            # Check CloudWatch alarms
            alarms = self.cloudwatch.describe_alarms()
            alarm_names = [alarm['AlarmName'] for alarm in alarms['MetricAlarms']]
            
            avesa_alarms = [name for name in alarm_names if 'AVESA' in name]
            print(f"  ✅ Found {len(avesa_alarms)} AVESA CloudWatch alarms")
            
            return True
            
        except Exception as e:
            self.warnings.append(f"Error checking monitoring: {e}")
            return True  # Non-critical
    
    def _check_cross_account_role(self) -> bool:
        """Check cross-account role access."""
        try:
            # This would require production account ID and role setup
            # For now, just check if the role exists in current account
            iam_client = boto3.client('iam', region_name=self.region)
            
            try:
                role = iam_client.get_role(RoleName=f"AVESACrossAccountMonitoring-{self.environment}")
                print(f"  ✅ Cross-account monitoring role exists")
                return True
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchEntity':
                    self.warnings.append("Cross-account monitoring role not found")
                    print(f"  ⚠️  Cross-account role missing")
                    return True  # Non-critical
                else:
                    raise e
                    
        except Exception as e:
            self.warnings.append(f"Error checking cross-account role: {e}")
            return True  # Non-critical
    
    def print_summary(self) -> bool:
        """Print validation summary."""
        print("\n" + "=" * 60)
        print("VALIDATION SUMMARY")
        print("=" * 60)
        
        if not self.errors and not self.warnings:
            print("🎉 All validations passed! Hybrid setup is working correctly.")
            return True
        
        if self.errors:
            print(f"\n❌ {len(self.errors)} ERRORS found:")
            for error in self.errors:
                print(f"   • {error}")
        
        if self.warnings:
            print(f"\n⚠️  {len(self.warnings)} WARNINGS found:")
            for warning in self.warnings:
                print(f"   • {warning}")
        
        if self.errors:
            print(f"\n🔧 Please fix the errors above before proceeding.")
            return False
        else:
            print(f"\n✅ Setup is functional with minor warnings.")
            return True


def main():
    """Main validation function."""
    parser = argparse.ArgumentParser(description='Validate hybrid AWS account setup')
    parser.add_argument('--environment', required=True, choices=['dev', 'staging', 'prod'],
                       help='Environment to validate')
    parser.add_argument('--region', default='us-east-1',
                       help='AWS region (default: us-east-1)')
    parser.add_argument('--check-cross-account', action='store_true',
                       help='Check cross-account access (dev/staging only)')
    parser.add_argument('--skip-functionality', action='store_true',
                       help='Skip functionality tests')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("AVESA HYBRID ACCOUNT SETUP VALIDATION")
    print("=" * 60)
    print(f"Environment: {args.environment}")
    print(f"Region: {args.region}")
    print(f"Started: {datetime.now().isoformat()}")
    
    try:
        validator = HybridSetupValidator(args.environment, args.region)
        
        success = True
        
        # Run validations
        success &= validator.validate_infrastructure()
        success &= validator.validate_data_migration()
        
        if not args.skip_functionality:
            success &= validator.validate_functionality()
        
        if args.check_cross_account:
            success &= validator.validate_cross_account_access()
        
        # Print summary
        overall_success = validator.print_summary()
        
        if overall_success:
            print(f"\n🎯 Next steps for {args.environment}:")
            if args.environment == "prod":
                print("   1. Update application configuration to use production account")
                print("   2. Set up monitoring alerts and notifications")
                print("   3. Test end-to-end data pipeline with real data")
                print("   4. Plan production cutover")
            else:
                print("   1. Deploy production account infrastructure")
                print("   2. Run data migration to production")
                print("   3. Test cross-account monitoring")
            
            sys.exit(0)
        else:
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Validation failed with error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()