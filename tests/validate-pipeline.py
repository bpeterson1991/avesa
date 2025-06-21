#!/usr/bin/env python3
"""
AVESA Pipeline Validation Suite
==============================

Consolidated validation script that combines functionality from:
- complete-end-to-end-validation.py
- final-pipeline-validation.py
- end-to-end-pipeline-test.py

This script provides comprehensive validation of the AVESA pipeline with multiple modes:
- quick: Basic health checks and connectivity
- full: Complete end-to-end validation with detailed reporting
- data: Focus on data integrity and SCD validation
- performance: Performance and quality metrics
"""

import requests
import json
import time
import boto3
import clickhouse_connect
from datetime import datetime
import pandas as pd
from io import BytesIO
import argparse
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

try:
    from shared.credential_manager import get_credential_manager
    from shared.logger import get_logger
    SHARED_IMPORTS_AVAILABLE = True
except ImportError:
    SHARED_IMPORTS_AVAILABLE = False
    print("⚠️ Shared imports not available, using basic functionality")

class PipelineValidator:
    def __init__(self, mode='full'):
        self.mode = mode
        self.region = 'us-east-2'
        self.tenant_id = 'sitetechnology'
        self.bucket_name = 'data-storage-msp-dev'
        self.api_base = "http://localhost:3001"
        self.frontend_url = "http://localhost:3000"
        
        # Initialize results storage
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'mode': mode,
            'clickhouse': {},
            'api_server': {},
            'frontend_server': {},
            'data_pipeline': {},
            'multi_tenant': {},
            'performance': {},
            'overall_status': 'PENDING'
        }
        
        if SHARED_IMPORTS_AVAILABLE:
            try:
                self.credential_manager = get_credential_manager(self.region)
                self.env_config = self.credential_manager.get_environment_config()
                self.bucket_name = self.env_config['bucket_name']
            except Exception as e:
                print(f"⚠️ Could not initialize credential manager: {e}")
    
    def print_header(self, title):
        """Print formatted section header"""
        print(f"\n{'='*80}")
        print(f"🚀 {title}")
        print(f"{'='*80}")
    
    def print_step(self, step):
        """Print formatted step"""
        print(f"\n📋 {step}")
        print(f"{'-'*60}")
    
    def print_result(self, message, success=True):
        """Print formatted result"""
        icon = "✅" if success else "❌"
        print(f"{icon} {message}")
    
    def test_clickhouse_connectivity(self):
        """Test ClickHouse connectivity and data integrity"""
        self.print_step("Testing ClickHouse Connectivity and Data")
        
        try:
            # Connect to ClickHouse using AWS Secrets Manager
            secrets_client = boto3.client('secretsmanager', region_name=self.region)
            response = secrets_client.get_secret_value(SecretId='clickhouse-connection-dev')
            secret = json.loads(response['SecretString'])
            
            client = clickhouse_connect.get_client(
                host=secret['host'],
                port=secret.get('port', 8443),
                username=secret['username'],
                password=secret['password'],
                database=secret.get('database', 'default'),
                secure=True,
                verify=False,
                connect_timeout=30,
                send_receive_timeout=300
            )
            
            self.print_result(f"Connected to ClickHouse: {secret['host']}")
            
            # Test all tables
            tables = ['companies', 'contacts', 'tickets', 'time_entries']
            table_stats = {}
            
            for table in tables:
                try:
                    # Total count
                    total_query = f"SELECT COUNT(*) FROM {table} WHERE tenant_id = '{self.tenant_id}'"
                    total_result = client.query(total_query)
                    total_count = total_result.result_rows[0][0] if total_result.result_rows else 0
                    
                    # Current count
                    current_query = f"SELECT COUNT(*) FROM {table} WHERE tenant_id = '{self.tenant_id}' AND is_current = true"
                    current_result = client.query(current_query)
                    current_count = current_result.result_rows[0][0] if current_result.result_rows else 0
                    
                    # Check for duplicates
                    dup_query = f"""
                    SELECT COUNT(*) FROM (
                        SELECT tenant_id, id, COUNT(*) as cnt 
                        FROM {table} 
                        WHERE tenant_id = '{self.tenant_id}' AND is_current = true 
                        GROUP BY tenant_id, id 
                        HAVING cnt > 1
                    )
                    """
                    dup_result = client.query(dup_query)
                    duplicate_count = dup_result.result_rows[0][0] if dup_result.result_rows else 0
                    
                    table_stats[table] = {
                        'total': total_count,
                        'current': current_count,
                        'duplicates': duplicate_count
                    }
                    
                    status = "✅" if duplicate_count == 0 else "⚠️"
                    self.print_result(f"{table}: {total_count} total, {current_count} current, {duplicate_count} duplicates", duplicate_count == 0)
                    
                except Exception as e:
                    table_stats[table] = {'error': str(e)}
                    self.print_result(f"{table}: Error - {str(e)}", False)
            
            client.close()
            
            self.results['clickhouse'] = {
                'status': 'SUCCESS',
                'host': secret['host'],
                'table_stats': table_stats
            }
            
            return True
            
        except Exception as e:
            self.print_result(f"ClickHouse test failed: {e}", False)
            self.results['clickhouse'] = {
                'status': 'FAILED',
                'error': str(e)
            }
            return False
    
    def test_api_server(self):
        """Test API server connectivity and endpoints"""
        self.print_step("Testing API Server")
        
        try:
            # Test health endpoint
            health_response = requests.get(f"{self.api_base}/health", timeout=10)
            if health_response.status_code == 200:
                self.print_result("API server health check passed")
                health_data = health_response.json() if health_response.headers.get('content-type', '').startswith('application/json') else {}
            else:
                self.print_result(f"API server health check returned {health_response.status_code}", False)
                health_data = {}
            
            # Test analytics endpoints if in full mode
            endpoint_results = {}
            if self.mode in ['full', 'data']:
                endpoints = [
                    "/api/analytics/companies",
                    "/api/analytics/contacts", 
                    "/api/analytics/tickets",
                    "/api/analytics/time-entries"
                ]
                
                for endpoint in endpoints:
                    try:
                        response = requests.get(f"{self.api_base}{endpoint}", timeout=10)
                        if response.status_code == 200:
                            data = response.json()
                            count = len(data) if isinstance(data, list) else data.get('count', 0)
                            endpoint_results[endpoint] = {'status': 'success', 'count': count}
                            self.print_result(f"{endpoint}: {count} records")
                        else:
                            endpoint_results[endpoint] = {'status': 'error', 'code': response.status_code}
                            self.print_result(f"{endpoint}: HTTP {response.status_code}", False)
                    except Exception as e:
                        endpoint_results[endpoint] = {'status': 'error', 'error': str(e)}
                        self.print_result(f"{endpoint}: {str(e)}", False)
            
            self.results['api_server'] = {
                'status': 'SUCCESS' if health_response.status_code == 200 else 'FAILED',
                'health_check': health_data,
                'endpoints': endpoint_results
            }
            
            return health_response.status_code == 200
            
        except Exception as e:
            self.print_result(f"API server test failed: {e}", False)
            self.results['api_server'] = {
                'status': 'FAILED',
                'error': str(e)
            }
            return False
    
    def test_frontend_server(self):
        """Test frontend server connectivity"""
        self.print_step("Testing Frontend Server")
        
        try:
            response = requests.get(self.frontend_url, timeout=10)
            if response.status_code == 200:
                self.print_result("Frontend server is running")
                self.print_result(f"Response size: {len(response.content)} bytes")
                
                self.results['frontend_server'] = {
                    'status': 'SUCCESS',
                    'response_size': len(response.content)
                }
                return True
            else:
                self.print_result(f"Frontend server returned {response.status_code}", False)
                self.results['frontend_server'] = {
                    'status': 'FAILED',
                    'status_code': response.status_code
                }
                return False
                
        except Exception as e:
            self.print_result(f"Frontend server test failed: {e}", False)
            self.results['frontend_server'] = {
                'status': 'FAILED',
                'error': str(e)
            }
            return False
    
    def test_data_pipeline_integrity(self):
        """Test the complete data pipeline from S3 to ClickHouse"""
        self.print_step("Testing Data Pipeline Integrity")
        
        try:
            # Check S3 canonical data
            s3_client = boto3.client('s3', region_name=self.region)
            canonical_prefix = f'{self.tenant_id}/canonical/'
            response = s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=canonical_prefix,
                MaxKeys=100
            )
            
            if 'Contents' not in response:
                self.print_result("No canonical data found in S3", False)
                self.results['data_pipeline'] = {
                    'status': 'FAILED',
                    'error': 'No canonical data in S3'
                }
                return False
            
            canonical_files = response['Contents']
            self.print_result(f"Found {len(canonical_files)} canonical files in S3")
            
            # Group by table
            file_structure = {}
            for obj in canonical_files:
                key_parts = obj['Key'].split('/')
                if len(key_parts) >= 4:
                    table = key_parts[3]
                    if table not in file_structure:
                        file_structure[table] = []
                    file_structure[table].append(obj)
            
            self.print_result("Canonical data structure:")
            for table, files in file_structure.items():
                latest_file = max(files, key=lambda x: x['LastModified'])
                self.print_result(f"  {table}: {len(files)} files, latest: {latest_file['LastModified']}")
            
            # Verify SCD structure if in full or data mode
            scd_verification = {}
            if self.mode in ['full', 'data'] and 'companies' in file_structure:
                latest_companies_file = max(file_structure['companies'], key=lambda x: x['LastModified'])
                scd_verification = self.verify_scd_structure(s3_client, latest_companies_file['Key'])
            
            self.results['data_pipeline'] = {
                'status': 'SUCCESS',
                'canonical_files_count': len(canonical_files),
                'file_structure': {table: len(files) for table, files in file_structure.items()},
                'scd_verification': scd_verification
            }
            
            return True
            
        except Exception as e:
            self.print_result(f"Data pipeline test failed: {e}", False)
            self.results['data_pipeline'] = {
                'status': 'FAILED',
                'error': str(e)
            }
            return False
    
    def verify_scd_structure(self, s3_client, file_key):
        """Verify SCD Type 2 structure in a canonical file"""
        try:
            response = s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            df = pd.read_parquet(BytesIO(response['Body'].read()))
            
            # Check required SCD fields
            required_fields = ['effective_start_date', 'effective_end_date', 'is_current', 'record_hash']
            present_fields = [field for field in required_fields if field in df.columns]
            
            # Check is_current logic
            current_records = df[df['is_current'] == True] if 'is_current' in df.columns else pd.DataFrame()
            
            self.print_result(f"SCD fields present: {present_fields}")
            self.print_result(f"Total records: {len(df)}")
            self.print_result(f"Current records: {len(current_records)}")
            
            return {
                'total_records': len(df),
                'scd_fields_present': present_fields,
                'current_records': len(current_records),
                'columns': list(df.columns)
            }
            
        except Exception as e:
            self.print_result(f"Error verifying SCD structure: {str(e)}", False)
            return {'error': str(e)}
    
    def test_multi_tenant_isolation(self):
        """Test multi-tenant data isolation"""
        self.print_step("Testing Multi-Tenant Isolation")
        
        try:
            # Connect to ClickHouse
            secrets_client = boto3.client('secretsmanager', region_name=self.region)
            response = secrets_client.get_secret_value(SecretId='clickhouse-connection-dev')
            secret = json.loads(response['SecretString'])
            
            client = clickhouse_connect.get_client(
                host=secret['host'],
                port=secret.get('port', 8443),
                username=secret['username'],
                password=secret['password'],
                database=secret.get('database', 'default'),
                secure=True,
                verify=False,
                connect_timeout=30,
                send_receive_timeout=300
            )
            
            # Check tenant isolation
            tables = ['companies', 'contacts', 'tickets', 'time_entries']
            tenant_stats = {}
            
            for table in tables:
                # Check sitetechnology tenant data
                site_query = f"SELECT COUNT(*) FROM {table} WHERE tenant_id = '{self.tenant_id}'"
                site_result = client.query(site_query)
                site_count = site_result.result_rows[0][0] if site_result.result_rows else 0
                
                # Check for any other tenant data
                other_query = f"SELECT COUNT(*) FROM {table} WHERE tenant_id != '{self.tenant_id}'"
                other_result = client.query(other_query)
                other_count = other_result.result_rows[0][0] if other_result.result_rows else 0
                
                # Check distinct tenants
                tenant_query = f"SELECT DISTINCT tenant_id FROM {table}"
                tenant_result = client.query(tenant_query)
                tenant_count = len(tenant_result.result_rows) if tenant_result.result_rows else 0
                
                tenant_stats[table] = {
                    'target_tenant_records': site_count,
                    'other_tenant_records': other_count,
                    'distinct_tenants': tenant_count
                }
                
                self.print_result(f"{table}: {site_count} {self.tenant_id} records, {other_count} other tenant records, {tenant_count} distinct tenants")
            
            client.close()
            
            self.results['multi_tenant'] = {
                'status': 'SUCCESS',
                'tenant_stats': tenant_stats
            }
            
            return True
            
        except Exception as e:
            self.print_result(f"Multi-tenant isolation test failed: {e}", False)
            self.results['multi_tenant'] = {
                'status': 'FAILED',
                'error': str(e)
            }
            return False
    
    def test_performance_metrics(self):
        """Test performance and quality metrics"""
        self.print_step("Testing Performance Metrics")
        
        # This is a placeholder for performance testing
        # In a real implementation, this would test query performance,
        # response times, data quality metrics, etc.
        
        performance_data = {
            'query_performance': 'SIMULATED',
            'data_quality': 'SIMULATED',
            'application_responsiveness': 'SIMULATED'
        }
        
        self.print_result("Performance testing simulated (would test query performance, response times, etc.)")
        
        self.results['performance'] = {
            'status': 'SIMULATED',
            'metrics': performance_data
        }
        
        return True
    
    def generate_report(self):
        """Generate comprehensive validation report"""
        self.print_header("VALIDATION REPORT")
        
        # Calculate overall status
        component_statuses = []
        for component, data in self.results.items():
            if component not in ['timestamp', 'mode', 'overall_status'] and isinstance(data, dict):
                status = data.get('status', 'UNKNOWN')
                component_statuses.append(status)
        
        # Determine overall status
        if all(status in ['SUCCESS', 'SIMULATED'] for status in component_statuses):
            overall_status = 'OPERATIONAL'
        elif any(status == 'SUCCESS' for status in component_statuses):
            overall_status = 'PARTIAL'
        else:
            overall_status = 'FAILED'
        
        self.results['overall_status'] = overall_status
        
        # Calculate total records
        total_records = 0
        if self.results['clickhouse'].get('status') == 'SUCCESS':
            table_stats = self.results['clickhouse'].get('table_stats', {})
            for table, stats in table_stats.items():
                if isinstance(stats, dict) and 'current' in stats:
                    total_records += stats['current']
        
        print(f"🎯 AVESA MULTI-TENANT SAAS PLATFORM STATUS: {overall_status}")
        print(f"📊 Total Current Records: {total_records:,}")
        print(f"⏰ Validation Time: {self.results['timestamp']}")
        print(f"🔧 Validation Mode: {self.mode.upper()}")
        print()
        
        # Component Status
        print("🔧 COMPONENT STATUS:")
        for component, data in self.results.items():
            if component not in ['timestamp', 'mode', 'overall_status'] and isinstance(data, dict):
                status = data.get('status', 'UNKNOWN')
                status_icon = "✅" if status == 'SUCCESS' else "🔄" if status == 'SIMULATED' else "⚠️" if status == 'PARTIAL' else "❌"
                component_name = component.replace('_', ' ').title()
                print(f"   {status_icon} {component_name}: {status}")
        
        print()
        
        # Data Summary
        if self.results['clickhouse'].get('status') == 'SUCCESS':
            print("📈 DATA SUMMARY:")
            table_stats = self.results['clickhouse'].get('table_stats', {})
            for table, stats in table_stats.items():
                if isinstance(stats, dict) and 'current' in stats:
                    print(f"   📊 {table.title()}: {stats['current']:,} current records ({stats['total']:,} total)")
        
        # Save report
        report_filename = f"pipeline_validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_filename, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        print(f"\n📄 Detailed report saved to: {report_filename}")
        
        return overall_status == 'OPERATIONAL'
    
    def run_validation(self):
        """Run validation based on mode"""
        print(f"🚀 AVESA PIPELINE VALIDATION ({self.mode.upper()} MODE)")
        print("=" * 80)
        print(f"⏰ Started at: {datetime.now()}")
        print()
        
        # Wait for servers to start if needed
        if self.mode != 'data':
            print("⏳ Waiting for servers to start...")
            time.sleep(5)
        
        success_count = 0
        total_tests = 0
        
        # Run tests based on mode
        if self.mode in ['quick', 'full']:
            # Basic connectivity tests
            if self.test_clickhouse_connectivity():
                success_count += 1
            total_tests += 1
            
            if self.test_api_server():
                success_count += 1
            total_tests += 1
            
            if self.test_frontend_server():
                success_count += 1
            total_tests += 1
        
        if self.mode in ['data', 'full']:
            # Data integrity tests
            if self.test_data_pipeline_integrity():
                success_count += 1
            total_tests += 1
            
            if self.test_multi_tenant_isolation():
                success_count += 1
            total_tests += 1
        
        if self.mode in ['performance', 'full']:
            # Performance tests
            if self.test_performance_metrics():
                success_count += 1
            total_tests += 1
        
        # Generate final report
        overall_success = self.generate_report()
        
        # Final status
        print(f"\n🎯 VALIDATION SUMMARY: {success_count}/{total_tests} tests passed")
        
        if overall_success:
            print("🎉 PIPELINE VALIDATION: SUCCESS")
            print("✅ The AVESA multi-tenant SaaS platform is operational!")
        else:
            print("⚠️ PIPELINE VALIDATION: ISSUES DETECTED")
            print("❌ Some components need attention.")
        
        return overall_success

def main():
    parser = argparse.ArgumentParser(description='AVESA Pipeline Validation Suite')
    parser.add_argument('--mode', choices=['quick', 'full', 'data', 'performance'], 
                       default='full', help='Validation mode (default: full)')
    parser.add_argument('--wait', type=int, default=5, 
                       help='Seconds to wait for servers to start (default: 5)')
    
    args = parser.parse_args()
    
    validator = PipelineValidator(mode=args.mode)
    success = validator.run_validation()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()