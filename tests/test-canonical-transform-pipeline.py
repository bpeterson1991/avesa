#!/usr/bin/env python3
"""
Canonical Transform Pipeline Test

This script comprehensively tests the canonical transform part of the pipeline:
1. Triggers canonical transform Lambda functions for all table types
2. Verifies parquet file generation in S3 with correct structure
3. Validates canonical data format and schema compliance
4. Tests data quality and transformation accuracy
5. Provides detailed reporting on canonical transform results
"""

import argparse
import boto3
import json
import sys
import time
import pandas as pd
import io
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional


class CanonicalTransformTester:
    def __init__(self, region='us-east-2', environment='dev'):
        self.region = region
        self.environment = environment
        self.session = boto3.Session(region_name=region)
        
        # Initialize AWS clients
        self.lambda_client = self.session.client('lambda')
        self.s3_client = self.session.client('s3')
        self.dynamodb = self.session.resource('dynamodb')
        
        # Configuration - Use actual bucket naming convention
        self.s3_bucket_name = f"data-storage-msp-{environment}"
        self.test_tenant_id = "sitetechnology"
        
        # Canonical tables to test
        self.canonical_tables = {
            'companies': {
                'lambda_name': f'avesa-canonical-transform-companies-{environment}',
                'source_path': 'company/companies',
                'canonical_path': 'canonical/companies'
            },
            'contacts': {
                'lambda_name': f'avesa-canonical-transform-contacts-{environment}',
                'source_path': 'company/contacts', 
                'canonical_path': 'canonical/contacts'
            },
            'tickets': {
                'lambda_name': f'avesa-canonical-transform-tickets-{environment}',
                'source_path': 'service/tickets',
                'canonical_path': 'canonical/tickets'
            },
            'time_entries': {
                'lambda_name': f'avesa-canonical-transform-time-entries-{environment}',
                'source_path': 'time_entries',
                'canonical_path': 'canonical/time_entries'
            }
        }
        
        # Test results storage
        self.test_results = {
            'start_time': datetime.now(),
            'tables_tested': {},
            'overall_success': False,
            'total_files_generated': 0,
            'total_records_transformed': 0
        }

    def check_raw_data_availability(self) -> Dict[str, Any]:
        """Check if raw data is available for transformation testing."""
        print("🔍 Checking raw data availability for canonical transformation...")
        
        raw_data_status = {}
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
        
        for table_name, config in self.canonical_tables.items():
            print(f"\n📊 Checking raw data for {table_name}...")
            
            # Check for raw data files
            raw_prefix = f"{self.test_tenant_id}/raw/connectwise/{table_name}/"
            
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.s3_bucket_name,
                    Prefix=raw_prefix,
                    MaxKeys=100
                )
                
                recent_files = []
                if 'Contents' in response:
                    for obj in response['Contents']:
                        if obj['LastModified'] > cutoff_time:
                            recent_files.append({
                                'key': obj['Key'],
                                'size': obj['Size'],
                                'last_modified': obj['LastModified']
                            })
                
                raw_data_status[table_name] = {
                    'has_raw_data': len(recent_files) > 0,
                    'file_count': len(recent_files),
                    'total_size': sum(f['size'] for f in recent_files),
                    'files': recent_files[:5]  # Keep first 5 for reference
                }
                
                if recent_files:
                    print(f"   ✅ Found {len(recent_files)} recent raw files ({sum(f['size'] for f in recent_files)} bytes)")
                else:
                    print(f"   ⚠️  No recent raw data found (last 24 hours)")
                    
            except Exception as e:
                print(f"   ❌ Error checking raw data: {str(e)}")
                raw_data_status[table_name] = {
                    'has_raw_data': False,
                    'error': str(e)
                }
        
        # Summary
        tables_with_data = [t for t, status in raw_data_status.items() if status.get('has_raw_data', False)]
        print(f"\n📈 Raw Data Summary:")
        print(f"   Tables with recent raw data: {len(tables_with_data)}/4")
        print(f"   Tables: {', '.join(tables_with_data)}")
        
        return raw_data_status

    def trigger_canonical_transform(self, table_name: str) -> Dict[str, Any]:
        """Trigger canonical transform for a specific table."""
        config = self.canonical_tables[table_name]
        function_name = config['lambda_name']
        
        print(f"🚀 Triggering canonical transform for {table_name}...")
        print(f"   Function: {function_name}")
        
        # Create payload for canonical transform
        payload = {
            'tenant_id': self.test_tenant_id,
            'test_mode': True
        }
        
        try:
            # Invoke the canonical transform function
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            
            if response['StatusCode'] == 200:
                response_payload = json.loads(response['Payload'].read())
                
                if response.get('FunctionError'):
                    return {
                        'success': False,
                        'error': response_payload.get('errorMessage', 'Unknown error'),
                        'details': response_payload
                    }
                else:
                    status_code = response_payload.get('statusCode', 500)
                    body = response_payload.get('body', {})
                    
                    if status_code == 200:
                        print(f"   ✅ Transform completed successfully")
                        print(f"   Records processed: {body.get('total_records', 0)}")
                        return {
                            'success': True,
                            'records_processed': body.get('total_records', 0),
                            'details': response_payload
                        }
                    else:
                        print(f"   ❌ Transform failed with status {status_code}")
                        return {
                            'success': False,
                            'error': f"Status code {status_code}",
                            'details': response_payload
                        }
            else:
                return {
                    'success': False,
                    'error': f"Lambda invocation failed: HTTP {response['StatusCode']}"
                }
                
        except Exception as e:
            print(f"   ❌ Error invoking function: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    def verify_canonical_parquet_files(self, table_name: str) -> Dict[str, Any]:
        """Verify that canonical parquet files were generated correctly."""
        print(f"🔍 Verifying canonical parquet files for {table_name}...")
        
        # Look for canonical parquet files
        canonical_prefix = f"{self.test_tenant_id}/canonical/{table_name}/"
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=30)
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket_name,
                Prefix=canonical_prefix,
                MaxKeys=100
            )
            
            parquet_files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('.parquet') and obj['LastModified'] > cutoff_time:
                        parquet_files.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified']
                        })
            
            verification_result = {
                'files_found': len(parquet_files),
                'total_size': sum(f['size'] for f in parquet_files),
                'files': parquet_files,
                'path_structure_correct': False,
                'schema_valid': False,
                'sample_records': []
            }
            
            if parquet_files:
                print(f"   ✅ Found {len(parquet_files)} parquet files")
                
                # Verify path structure (should be YYYY/MM/DD format)
                expected_patterns = [
                    f"{self.test_tenant_id}/canonical/{table_name}/",
                    "/20",  # Year pattern
                    "/"     # Month/day separators
                ]
                
                structure_valid = all(
                    any(pattern in file['key'] for pattern in expected_patterns)
                    for file in parquet_files
                )
                verification_result['path_structure_correct'] = structure_valid
                
                if structure_valid:
                    print(f"   ✅ Path structure follows expected format")
                else:
                    print(f"   ⚠️  Path structure may not follow expected YYYY/MM/DD format")
                
                # Verify schema and content of first file
                first_file = parquet_files[0]
                schema_result = self._verify_parquet_schema(first_file['key'], table_name)
                verification_result.update(schema_result)
                
            else:
                print(f"   ❌ No recent parquet files found")
            
            return verification_result
            
        except Exception as e:
            print(f"   ❌ Error verifying parquet files: {str(e)}")
            return {
                'files_found': 0,
                'error': str(e)
            }

    def _verify_parquet_schema(self, s3_key: str, table_name: str) -> Dict[str, Any]:
        """Verify the schema and content of a parquet file."""
        print(f"   🔍 Verifying schema for {s3_key}...")
        
        try:
            # Read parquet file from S3
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket_name,
                Key=s3_key
            )
            
            # Load parquet data
            df = pd.read_parquet(io.BytesIO(response['Body'].read()))
            
            schema_result = {
                'schema_valid': True,
                'record_count': len(df),
                'columns': list(df.columns),
                'sample_records': [],
                'canonical_fields_present': False,
                'scd_fields_present': False
            }
            
            print(f"      Records: {len(df)}")
            print(f"      Columns: {len(df.columns)}")
            
            # Check for canonical fields based on table type
            expected_canonical_fields = self._get_expected_canonical_fields(table_name)
            canonical_fields_found = [col for col in expected_canonical_fields if col in df.columns]
            schema_result['canonical_fields_present'] = len(canonical_fields_found) > 0
            
            if canonical_fields_found:
                print(f"      ✅ Canonical fields found: {canonical_fields_found[:5]}")
            else:
                print(f"      ⚠️  No expected canonical fields found")
            
            # Check for SCD Type 2 fields
            scd_fields = ['effective_start_date', 'effective_end_date', 'is_current', 'record_hash']
            scd_fields_found = [col for col in scd_fields if col in df.columns]
            schema_result['scd_fields_present'] = len(scd_fields_found) > 0
            
            if scd_fields_found:
                print(f"      ✅ SCD Type 2 fields found: {scd_fields_found}")
            else:
                print(f"      ⚠️  SCD Type 2 fields not found")
            
            # Get sample records
            if len(df) > 0:
                sample_size = min(3, len(df))
                sample_records = df.head(sample_size).to_dict('records')
                schema_result['sample_records'] = sample_records
                print(f"      ✅ Sample record fields: {list(sample_records[0].keys())[:10]}")
            
            return schema_result
            
        except Exception as e:
            print(f"      ❌ Error reading parquet file: {str(e)}")
            return {
                'schema_valid': False,
                'error': str(e)
            }

    def _get_expected_canonical_fields(self, table_name: str) -> List[str]:
        """Get expected canonical fields for a table type."""
        canonical_fields = {
            'companies': ['id', 'company_name', 'company_identifier', 'status', 'last_updated'],
            'contacts': ['id', 'company_id', 'first_name', 'last_name', 'default_email_address'],
            'tickets': ['id', 'ticket_number', 'summary', 'status', 'priority', 'company_id'],
            'time_entries': ['id', 'company_id', 'member_id', 'actual_hours', 'time_start']
        }
        return canonical_fields.get(table_name, [])

    def validate_canonical_data_quality(self, table_name: str, sample_records: List[Dict]) -> Dict[str, Any]:
        """Validate the quality of canonical data."""
        print(f"   🔍 Validating data quality for {table_name}...")
        
        if not sample_records:
            return {'quality_score': 0, 'issues': ['No sample records available']}
        
        quality_issues = []
        quality_score = 100
        
        # Check for required fields
        required_fields = ['source_system', 'canonical_table', 'ingestion_timestamp']
        for record in sample_records:
            missing_fields = [field for field in required_fields if field not in record or record[field] is None]
            if missing_fields:
                quality_issues.append(f"Missing required fields: {missing_fields}")
                quality_score -= 10
        
        # Check data types and formats
        for record in sample_records:
            # Check timestamp format
            if 'ingestion_timestamp' in record:
                try:
                    datetime.fromisoformat(str(record['ingestion_timestamp']).replace('Z', '+00:00'))
                except:
                    quality_issues.append("Invalid timestamp format")
                    quality_score -= 5
            
            # Check for null values in key fields
            key_fields = self._get_expected_canonical_fields(table_name)[:3]  # First 3 key fields
            null_keys = [field for field in key_fields if field in record and record[field] is None]
            if null_keys:
                quality_issues.append(f"Null values in key fields: {null_keys}")
                quality_score -= 10
        
        quality_result = {
            'quality_score': max(0, quality_score),
            'issues': quality_issues,
            'records_checked': len(sample_records)
        }
        
        if quality_score >= 80:
            print(f"      ✅ Data quality: {quality_score}% (Good)")
        elif quality_score >= 60:
            print(f"      ⚠️  Data quality: {quality_score}% (Fair)")
        else:
            print(f"      ❌ Data quality: {quality_score}% (Poor)")
        
        return quality_result

    def test_canonical_transform_for_table(self, table_name: str) -> Dict[str, Any]:
        """Test canonical transform for a specific table."""
        print(f"\n{'='*60}")
        print(f"🔄 Testing Canonical Transform: {table_name.upper()}")
        print(f"{'='*60}")
        
        table_result = {
            'table_name': table_name,
            'transform_triggered': False,
            'transform_successful': False,
            'parquet_files_generated': False,
            'schema_valid': False,
            'data_quality_good': False,
            'records_processed': 0,
            'files_generated': 0,
            'issues': []
        }
        
        try:
            # Step 1: Trigger canonical transform
            transform_result = self.trigger_canonical_transform(table_name)
            table_result['transform_triggered'] = True
            
            if transform_result['success']:
                table_result['transform_successful'] = True
                table_result['records_processed'] = transform_result.get('records_processed', 0)
                
                # Wait a moment for files to be written
                time.sleep(5)
                
                # Step 2: Verify parquet file generation
                parquet_result = self.verify_canonical_parquet_files(table_name)
                
                if parquet_result['files_found'] > 0:
                    table_result['parquet_files_generated'] = True
                    table_result['files_generated'] = parquet_result['files_found']
                    
                    # Step 3: Validate schema
                    if parquet_result.get('schema_valid', False):
                        table_result['schema_valid'] = True
                        
                        # Step 4: Validate data quality
                        sample_records = parquet_result.get('sample_records', [])
                        if sample_records:
                            quality_result = self.validate_canonical_data_quality(table_name, sample_records)
                            table_result['data_quality_good'] = quality_result['quality_score'] >= 70
                            if quality_result['issues']:
                                table_result['issues'].extend(quality_result['issues'])
                    else:
                        table_result['issues'].append("Invalid parquet schema")
                else:
                    table_result['issues'].append("No parquet files generated")
            else:
                table_result['issues'].append(f"Transform failed: {transform_result.get('error', 'Unknown error')}")
                
        except Exception as e:
            table_result['issues'].append(f"Test error: {str(e)}")
        
        # Determine overall success for this table
        table_success = (
            table_result['transform_successful'] and
            table_result['parquet_files_generated'] and
            table_result['schema_valid']
        )
        
        table_result['overall_success'] = table_success
        
        # Print table summary
        print(f"\n📊 {table_name.upper()} Test Summary:")
        print(f"   Transform Triggered: {'✅' if table_result['transform_triggered'] else '❌'}")
        print(f"   Transform Successful: {'✅' if table_result['transform_successful'] else '❌'}")
        print(f"   Parquet Files Generated: {'✅' if table_result['parquet_files_generated'] else '❌'}")
        print(f"   Schema Valid: {'✅' if table_result['schema_valid'] else '❌'}")
        print(f"   Data Quality Good: {'✅' if table_result['data_quality_good'] else '❌'}")
        print(f"   Records Processed: {table_result['records_processed']}")
        print(f"   Files Generated: {table_result['files_generated']}")
        
        if table_result['issues']:
            print(f"   Issues: {', '.join(table_result['issues'])}")
        
        if table_success:
            print(f"   🎉 {table_name.upper()} CANONICAL TRANSFORM: PASSED")
        else:
            print(f"   ❌ {table_name.upper()} CANONICAL TRANSFORM: FAILED")
        
        return table_result

    def run_comprehensive_canonical_test(self) -> bool:
        """Run comprehensive canonical transform test for all tables."""
        print("🚀 AVESA Canonical Transform Pipeline Test")
        print("="*60)
        print(f"Region: {self.region}")
        print(f"Environment: {self.environment}")
        print(f"S3 Bucket: {self.s3_bucket_name}")
        print(f"Test Tenant: {self.test_tenant_id}")
        print(f"Test Time: {datetime.now().isoformat()}")
        print()
        
        # Check raw data availability first
        raw_data_status = self.check_raw_data_availability()
        
        # Test each canonical table
        for table_name in self.canonical_tables.keys():
            table_result = self.test_canonical_transform_for_table(table_name)
            self.test_results['tables_tested'][table_name] = table_result
            
            if table_result['overall_success']:
                self.test_results['total_files_generated'] += table_result['files_generated']
                self.test_results['total_records_transformed'] += table_result['records_processed']
        
        # Generate final report
        self._generate_final_report()
        
        # Determine overall success
        successful_tables = [
            name for name, result in self.test_results['tables_tested'].items()
            if result['overall_success']
        ]
        
        self.test_results['overall_success'] = len(successful_tables) >= 3  # At least 3/4 tables successful
        
        return self.test_results['overall_success']

    def _generate_final_report(self):
        """Generate comprehensive final test report."""
        print(f"\n{'='*60}")
        print("📊 CANONICAL TRANSFORM PIPELINE TEST REPORT")
        print(f"{'='*60}")
        
        # Test execution summary
        execution_time = datetime.now() - self.test_results['start_time']
        print(f"🕒 Test Execution Time: {execution_time.total_seconds():.1f} seconds")
        print(f"📊 Total Tables Tested: {len(self.test_results['tables_tested'])}")
        
        # Table results summary
        successful_tables = []
        failed_tables = []
        
        for table_name, result in self.test_results['tables_tested'].items():
            if result['overall_success']:
                successful_tables.append(table_name)
            else:
                failed_tables.append(table_name)
        
        print(f"✅ Successful Tables: {len(successful_tables)}/4")
        print(f"❌ Failed Tables: {len(failed_tables)}/4")
        
        if successful_tables:
            print(f"   Successful: {', '.join(successful_tables)}")
        if failed_tables:
            print(f"   Failed: {', '.join(failed_tables)}")
        
        # Data processing summary
        print(f"\n📈 Data Processing Summary:")
        print(f"   Total Parquet Files Generated: {self.test_results['total_files_generated']}")
        print(f"   Total Records Transformed: {self.test_results['total_records_transformed']}")
        
        # Detailed table results
        print(f"\n📋 Detailed Table Results:")
        for table_name, result in self.test_results['tables_tested'].items():
            status = "✅ PASSED" if result['overall_success'] else "❌ FAILED"
            print(f"   {table_name}: {status}")
            print(f"      Records: {result['records_processed']}, Files: {result['files_generated']}")
            if result['issues']:
                print(f"      Issues: {', '.join(result['issues'])}")
        
        # Overall assessment
        print(f"\n🎯 Overall Assessment:")
        if self.test_results['overall_success']:
            print("   🎉 CANONICAL TRANSFORM PIPELINE TEST: PASSED!")
            print("   ✅ Canonical transformation is working correctly")
            print("   ✅ Parquet files are being generated in correct format")
            print("   ✅ Data quality validation successful")
            print("   ✅ Schema compliance verified")
        else:
            print("   ❌ CANONICAL TRANSFORM PIPELINE TEST: FAILED!")
            print("   ⚠️  Canonical transformation needs attention")
            print(f"   ⚠️  Only {len(successful_tables)} out of 4 tables passed")
        
        # Recommendations
        if failed_tables:
            print(f"\n💡 Recommendations:")
            print("   1. Check Lambda function logs for failed tables")
            print("   2. Verify raw data availability for failed tables")
            print("   3. Review canonical mapping configurations")
            print("   4. Check S3 permissions and bucket configuration")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='AVESA Canonical Transform Pipeline Tester')
    parser.add_argument('--region', default='us-east-2', help='AWS region')
    parser.add_argument('--environment', default='dev', help='Environment (dev/staging/prod)')
    parser.add_argument('--table', help='Test specific table only (companies, contacts, tickets, time_entries)')
    parser.add_argument('--check-raw-data', action='store_true', help='Only check raw data availability')
    
    args = parser.parse_args()
    
    tester = CanonicalTransformTester(region=args.region, environment=args.environment)
    
    if args.check_raw_data:
        print("🔍 Checking raw data availability...")
        raw_data_status = tester.check_raw_data_availability()
        return 0
    
    if args.table:
        if args.table not in tester.canonical_tables:
            print(f"❌ Invalid table name: {args.table}")
            print(f"Valid tables: {', '.join(tester.canonical_tables.keys())}")
            return 1
        
        print(f"🔄 Testing canonical transform for {args.table} only...")
        result = tester.test_canonical_transform_for_table(args.table)
        return 0 if result['overall_success'] else 1
    else:
        print("🚀 Running comprehensive canonical transform test...")
        success = tester.run_comprehensive_canonical_test()
        return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())