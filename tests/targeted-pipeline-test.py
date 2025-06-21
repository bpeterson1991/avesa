#!/usr/bin/env python3
"""
AVESA Targeted Pipeline Test
===========================

Focused test of the canonical transform and ClickHouse loading with correct function names.
"""

import boto3
import json
import time
import pandas as pd
from datetime import datetime, timezone
from io import BytesIO
import sys
import os

# Configuration
REGION = 'us-east-2'
BUCKET_NAME = 'data-storage-msp-dev'
TENANT_ID = 'sitetechnology'

class TargetedPipelineTest:
    def __init__(self):
        # Use AWS profile from environment
        session = boto3.Session(profile_name=os.environ.get('AWS_PROFILE'))
        self.s3_client = session.client('s3', region_name=REGION)
        self.lambda_client = session.client('lambda', region_name=REGION)
        self.start_time = datetime.now(timezone.utc)
        
    def print_header(self, title):
        print(f"\n{'='*80}")
        print(f"🚀 {title}")
        print(f"{'='*80}")
        
    def print_step(self, step):
        print(f"\n📋 {step}")
        print(f"{'-'*60}")
        
    def print_result(self, message, success=True):
        icon = "✅" if success else "❌"
        print(f"{icon} {message}")
        
    def test_canonical_transform_single_table(self, table_name):
        """Test canonical transform for a single table"""
        function_name = f'avesa-canonical-transform-{table_name}-dev'
        
        try:
            payload = {
                'tenant_id': TENANT_ID,
                'table_name': table_name,
                'source_bucket': BUCKET_NAME,
                'target_bucket': BUCKET_NAME
            }
            
            self.print_result(f"Invoking {function_name}...")
            
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            
            result = json.loads(response['Payload'].read())
            
            if response['StatusCode'] == 200 and 'errorMessage' not in result:
                self.print_result(f"✅ {table_name} transform completed successfully")
                if 'body' in result:
                    body = json.loads(result['body']) if isinstance(result['body'], str) else result['body']
                    if 'processed_files' in body:
                        self.print_result(f"   📄 Processed {body['processed_files']} files")
                return True
            else:
                error_msg = result.get('errorMessage', 'Unknown error')
                self.print_result(f"❌ {table_name} transform failed: {error_msg}", False)
                return False
                
        except Exception as e:
            self.print_result(f"❌ {table_name} transform error: {str(e)}", False)
            return False
    
    def test_clickhouse_loading_single_table(self, table_name):
        """Test ClickHouse loading for a single table"""
        function_name = f'clickhouse-loader-{table_name}-dev'
        
        try:
            payload = {
                'tenant_id': TENANT_ID,
                'table_name': table_name,
                'bucket_name': BUCKET_NAME
            }
            
            self.print_result(f"Invoking {function_name}...")
            
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            
            result = json.loads(response['Payload'].read())
            
            if response['StatusCode'] == 200 and 'errorMessage' not in result:
                self.print_result(f"✅ {table_name} ClickHouse loading completed successfully")
                if 'body' in result:
                    body = json.loads(result['body']) if isinstance(result['body'], str) else result['body']
                    if 'records_processed' in body:
                        self.print_result(f"   📊 Processed {body['records_processed']} records")
                return True
            else:
                error_msg = result.get('errorMessage', 'Unknown error')
                self.print_result(f"❌ {table_name} ClickHouse loading failed: {error_msg}", False)
                return False
                
        except Exception as e:
            self.print_result(f"❌ {table_name} ClickHouse loading error: {str(e)}", False)
            return False
    
    def test_scd_processor(self, table_name):
        """Test the SCD processor for a single table"""
        function_name = 'clickhouse-scd-processor-dev'
        
        try:
            payload = {
                'tenant_id': TENANT_ID,
                'table_name': table_name,
                'bucket_name': BUCKET_NAME
            }
            
            self.print_result(f"Invoking SCD processor for {table_name}...")
            
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            
            result = json.loads(response['Payload'].read())
            
            if response['StatusCode'] == 200 and 'errorMessage' not in result:
                self.print_result(f"✅ {table_name} SCD processing completed successfully")
                return True
            else:
                error_msg = result.get('errorMessage', 'Unknown error')
                self.print_result(f"❌ {table_name} SCD processing failed: {error_msg}", False)
                return False
                
        except Exception as e:
            self.print_result(f"❌ {table_name} SCD processing error: {str(e)}", False)
            return False
    
    def verify_canonical_data(self, table_name):
        """Verify canonical data was created for a table"""
        canonical_prefix = f'{TENANT_ID}/canonical/{table_name}/'
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix=canonical_prefix,
                MaxKeys=10
            )
            
            if 'Contents' in response:
                files = response['Contents']
                self.print_result(f"Found {len(files)} canonical files for {table_name}")
                
                # Check the latest file
                if files:
                    latest_file = max(files, key=lambda x: x['LastModified'])
                    self.print_result(f"Latest file: {latest_file['Key']} ({latest_file['Size']} bytes)")
                    
                    # Examine SCD structure
                    try:
                        response = self.s3_client.get_object(Bucket=BUCKET_NAME, Key=latest_file['Key'])
                        df = pd.read_parquet(BytesIO(response['Body'].read()))
                        
                        scd_fields = ['effective_start_date', 'effective_end_date', 'is_current', 'record_hash']
                        present_fields = [field for field in scd_fields if field in df.columns]
                        
                        self.print_result(f"Records: {len(df)}, SCD fields: {present_fields}")
                        
                        # Check current records
                        if 'is_current' in df.columns:
                            current_count = len(df[df['is_current'] == True])
                            self.print_result(f"Current records: {current_count}")
                        
                        return True
                        
                    except Exception as e:
                        self.print_result(f"Could not examine file: {str(e)}", False)
                        return False
                
            else:
                self.print_result(f"No canonical files found for {table_name}", False)
                return False
                
        except Exception as e:
            self.print_result(f"Error checking canonical data for {table_name}: {str(e)}", False)
            return False
    
    def run_targeted_test(self):
        """Run targeted test for one table through the complete pipeline"""
        print("🚀 AVESA TARGETED PIPELINE TEST")
        print("=" * 80)
        print(f"🕐 Test Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"🏢 Tenant: {TENANT_ID}")
        print(f"🪣 Bucket: {BUCKET_NAME}")
        print(f"🌍 Region: {REGION}")
        
        # Test with companies table first
        test_table = 'companies'
        
        self.print_header(f"TESTING COMPLETE PIPELINE FOR {test_table.upper()}")
        
        # Step 1: Test canonical transform
        self.print_step("Step 1: Canonical Transform")
        transform_success = self.test_canonical_transform_single_table(test_table)
        
        if transform_success:
            # Verify canonical data was created
            self.print_step("Step 1a: Verify Canonical Data")
            canonical_success = self.verify_canonical_data(test_table)
        else:
            canonical_success = False
        
        # Step 2: Test ClickHouse loading (try both methods)
        self.print_step("Step 2a: ClickHouse Direct Loader")
        loader_success = self.test_clickhouse_loading_single_table(test_table)
        
        self.print_step("Step 2b: ClickHouse SCD Processor")
        scd_success = self.test_scd_processor(test_table)
        
        # Summary
        self.print_header("TEST RESULTS SUMMARY")
        
        results = {
            'canonical_transform': transform_success,
            'canonical_verification': canonical_success,
            'clickhouse_loader': loader_success,
            'scd_processor': scd_success
        }
        
        for test_name, success in results.items():
            icon = "✅" if success else "❌"
            print(f"{icon} {test_name.replace('_', ' ').title()}: {'SUCCESS' if success else 'FAILED'}")
        
        overall_success = all(results.values())
        status = "✅ PIPELINE OPERATIONAL" if overall_success else "⚠️ PARTIAL SUCCESS" if any(results.values()) else "❌ PIPELINE FAILED"
        
        print(f"\n🎯 OVERALL STATUS: {status}")
        
        # If successful, test all tables
        if transform_success and canonical_success:
            self.print_header("TESTING ALL TABLES")
            
            tables = ['companies', 'contacts', 'tickets', 'time_entries']
            all_tables_success = True
            
            for table in tables:
                if table != test_table:  # Skip the one we already tested
                    self.print_step(f"Processing {table}")
                    table_success = self.test_canonical_transform_single_table(table)
                    if table_success:
                        self.verify_canonical_data(table)
                    all_tables_success = all_tables_success and table_success
            
            if all_tables_success:
                print(f"\n🎉 ALL TABLES PROCESSED SUCCESSFULLY!")
            else:
                print(f"\n⚠️ SOME TABLES HAD ISSUES")
        
        return overall_success

def main():
    """Main execution function"""
    runner = TargetedPipelineTest()
    success = runner.run_targeted_test()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()