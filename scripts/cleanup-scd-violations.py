#!/usr/bin/env python3
"""
SCD Type 2 Violation Cleanup Script
===================================

This script fixes SCD Type 2 violations where records have:
- Same tenant_id, ID, and last_updated timestamp
- But different is_current values (one true, one false)

The script keeps the record with is_current=true and removes the is_current=false
version, since they represent the same data version.
"""

import os
import sys
import json
import boto3
import clickhouse_connect
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional

class SCDViolationCleanup:
    def __init__(self, use_aws_secrets=True, dry_run=True):
        self.use_aws_secrets = use_aws_secrets
        self.dry_run = dry_run
        self.region = 'us-east-2'
        
        self.cleanup_results = {
            'timestamp': datetime.now().isoformat(),
            'dry_run': dry_run,
            'scd_violations_found': 0,
            'records_to_delete': 0,
            'cleanup_queries': [],
            'affected_tenants': set(),
            'errors': []
        }
        
    def get_clickhouse_client(self):
        """Get ClickHouse client using appropriate credential method"""
        if self.use_aws_secrets:
            return self.get_clickhouse_client_aws()
        else:
            return self.get_clickhouse_client_env()
    
    def get_clickhouse_client_aws(self):
        """Get ClickHouse client using AWS Secrets Manager"""
        try:
            secrets_client = boto3.client('secretsmanager', region_name=self.region)
            secret_name = os.getenv('CLICKHOUSE_SECRET_NAME', 'clickhouse-connection-dev')
            
            response = secrets_client.get_secret_value(SecretId=secret_name)
            credentials = json.loads(response['SecretString'])
            
            client = clickhouse_connect.get_client(
                host=credentials['host'],
                port=int(credentials.get('port', 8443)),
                username=credentials['username'],
                password=credentials['password'],
                database=credentials.get('database', 'default'),
                secure=True,
                verify=False,  # Disable SSL verification for ClickHouse Cloud
                connect_timeout=30,
                send_receive_timeout=300
            )
            
            return client
            
        except Exception as e:
            print(f"Failed to connect via AWS Secrets: {e}")
            return None
    
    def get_clickhouse_client_env(self):
        """Get ClickHouse client using environment variables"""
        try:
            credentials = {
                'host': os.getenv('CLICKHOUSE_HOST', 'clickhouse.avesa.dev'),
                'port': int(os.getenv('CLICKHOUSE_PORT', '8443')),
                'username': os.getenv('CLICKHOUSE_USER', 'avesa_user'),
                'password': os.getenv('CLICKHOUSE_PASSWORD'),
                'database': os.getenv('CLICKHOUSE_DATABASE', 'default')
            }
            
            if not credentials['password']:
                print("CLICKHOUSE_PASSWORD environment variable not set")
                return None
            
            client = clickhouse_connect.get_client(
                host=credentials['host'],
                port=credentials['port'],
                username=credentials['username'],
                password=credentials['password'],
                database=credentials['database'],
                secure=True,
                verify=False,  # For development
                connect_timeout=30,
                send_receive_timeout=300
            )
            
            return client
            
        except Exception as e:
            print(f"Failed to connect via environment variables: {e}")
            return None
    
    def identify_scd_violations(self, client) -> List[Dict[str, Any]]:
        """Identify SCD violations that need to be cleaned up"""
        
        print("🔍 Identifying SCD Type 2 violations for cleanup...")
        
        # Find records to delete (keep is_current=true, delete is_current=false for same ID+last_updated)
        violations_to_fix_query = """
        SELECT
            tenant_id,
            id,
            last_updated,
            effective_date,
            expiration_date,
            is_current,
            date_entered,
            data_hash,
            record_version
        FROM time_entries
        WHERE (tenant_id, id, last_updated) IN (
            SELECT tenant_id, id, last_updated
            FROM time_entries
            GROUP BY tenant_id, id, last_updated
            HAVING COUNT(DISTINCT is_current) > 1
        )
        ORDER BY tenant_id, id, last_updated, is_current DESC
        """
        
        result = client.query(violations_to_fix_query)
        
        records_to_delete = []
        violation_groups = {}
        
        for row in result.result_rows:
            (tenant_id, record_id, last_updated, effective_date, expiration_date, 
             is_current, date_entered, data_hash, record_version) = row
            
            group_key = f"{tenant_id}:{record_id}:{last_updated}"
            
            if group_key not in violation_groups:
                violation_groups[group_key] = {
                    'tenant_id': tenant_id,
                    'id': record_id,
                    'last_updated': last_updated,
                    'records': []
                }
            
            record_info = {
                'tenant_id': tenant_id,
                'id': record_id,
                'last_updated': last_updated,
                'effective_date': effective_date,
                'expiration_date': expiration_date,
                'is_current': is_current,
                'date_entered': date_entered,
                'data_hash': data_hash,
                'record_version': record_version
            }
            
            violation_groups[group_key]['records'].append(record_info)
        
        # For each violation group, keep is_current=true and delete is_current=false
        for group_key, group_info in violation_groups.items():
            records = group_info['records']
            
            # Find current and non-current records
            current_records = [r for r in records if r['is_current']]
            non_current_records = [r for r in records if not r['is_current']]
            
            if len(current_records) == 1 and len(non_current_records) == 1:
                # Standard case: 1 current + 1 non-current = SCD violation
                # Delete the non-current record since they have same ID+last_updated
                records_to_delete.extend(non_current_records)
                self.cleanup_results['affected_tenants'].add(group_info['tenant_id'])
            elif len(current_records) > 1:
                # Multiple current records - keep the one with earliest effective_date
                current_records.sort(key=lambda x: x['effective_date'])
                records_to_delete.extend(current_records[1:])  # Delete all but the first
                records_to_delete.extend(non_current_records)  # Delete all non-current
                self.cleanup_results['affected_tenants'].add(group_info['tenant_id'])
            elif len(non_current_records) > 1 and len(current_records) == 0:
                # Multiple non-current, no current - keep the one with latest effective_date as current
                non_current_records.sort(key=lambda x: x['effective_date'], reverse=True)
                records_to_delete.extend(non_current_records[1:])  # Delete all but the latest
                # Note: We'll need to update the kept record to is_current=true
                self.cleanup_results['affected_tenants'].add(group_info['tenant_id'])
        
        self.cleanup_results['scd_violations_found'] = len(violation_groups)
        self.cleanup_results['records_to_delete'] = len(records_to_delete)
        
        print(f"   📊 Found {len(violation_groups)} SCD violation groups")
        print(f"   🗑️  {len(records_to_delete)} records marked for deletion")
        print(f"   🏢 {len(self.cleanup_results['affected_tenants'])} tenants affected")
        
        return records_to_delete
    
    def generate_cleanup_queries(self, records_to_delete: List[Dict[str, Any]]) -> List[str]:
        """Generate DELETE queries for SCD violations"""
        
        print("📝 Generating SCD violation cleanup queries...")
        
        # Group records by tenant for efficient deletion
        tenant_groups = {}
        for record in records_to_delete:
            tenant_id = record['tenant_id']
            if tenant_id not in tenant_groups:
                tenant_groups[tenant_id] = []
            tenant_groups[tenant_id].append(record)
        
        cleanup_queries = []
        
        for tenant_id, tenant_records in tenant_groups.items():
            # Create a batch DELETE query for this tenant
            # We'll delete by exact match on all key fields to be absolutely safe
            
            conditions = []
            for record in tenant_records:
                condition = f"""(
                    tenant_id = '{tenant_id}' AND
                    id = '{record['id']}' AND
                    last_updated = '{record['last_updated']}' AND
                    is_current = {1 if record['is_current'] else 0} AND
                    effective_date = '{record['effective_date']}' AND
                    date_entered = '{record['date_entered']}'
                )"""
                conditions.append(condition)
            
            # Batch conditions in groups of 50 to avoid query size limits
            batch_size = 50
            for i in range(0, len(conditions), batch_size):
                batch_conditions = conditions[i:i + batch_size]
                
                delete_query = f"""
                DELETE FROM time_entries 
                WHERE {' OR '.join(batch_conditions)}
                """
                
                cleanup_queries.append({
                    'tenant_id': tenant_id,
                    'query': delete_query,
                    'records_in_batch': len(batch_conditions),
                    'batch_number': (i // batch_size) + 1,
                    'query_type': 'scd_violation_cleanup'
                })
        
        self.cleanup_results['cleanup_queries'] = cleanup_queries
        
        print(f"   📋 Generated {len(cleanup_queries)} SCD cleanup queries")
        
        return cleanup_queries
    
    def execute_cleanup(self, client, cleanup_queries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute the SCD violation cleanup queries"""
        
        if self.dry_run:
            print("🔍 DRY RUN MODE - No actual deletions will be performed")
            print("   Use --execute flag to perform actual cleanup")
            return {'dry_run': True, 'queries_generated': len(cleanup_queries)}
        
        print("🚨 EXECUTING SCD VIOLATION CLEANUP - This will permanently delete violating records!")
        
        execution_results = {
            'queries_executed': 0,
            'records_deleted': 0,
            'errors': [],
            'successful_batches': [],
            'failed_batches': []
        }
        
        for i, query_info in enumerate(cleanup_queries, 1):
            try:
                print(f"   Executing batch {i}/{len(cleanup_queries)} for tenant {query_info['tenant_id']}...")
                
                result = client.command(query_info['query'])
                
                execution_results['queries_executed'] += 1
                execution_results['records_deleted'] += query_info['records_in_batch']
                execution_results['successful_batches'].append({
                    'batch': i,
                    'tenant_id': query_info['tenant_id'],
                    'records_deleted': query_info['records_in_batch']
                })
                
                print(f"      ✅ Deleted {query_info['records_in_batch']} SCD violating records")
                
            except Exception as e:
                error_msg = f"Failed to execute batch {i} for tenant {query_info['tenant_id']}: {str(e)}"
                print(f"      ❌ {error_msg}")
                
                execution_results['errors'].append(error_msg)
                execution_results['failed_batches'].append({
                    'batch': i,
                    'tenant_id': query_info['tenant_id'],
                    'error': str(e)
                })
        
        return execution_results
    
    def print_cleanup_summary(self, execution_results: Dict[str, Any]):
        """Print cleanup summary"""
        
        print(f"\n{'='*60}")
        print(f"📋 SCD VIOLATION CLEANUP SUMMARY")
        print(f"{'='*60}")
        
        print(f"🔍 Mode: {'DRY RUN' if self.dry_run else 'EXECUTION'}")
        print(f"📊 SCD violation groups found: {self.cleanup_results['scd_violations_found']:,}")
        print(f"🗑️  Records marked for deletion: {self.cleanup_results['records_to_delete']:,}")
        print(f"🏢 Affected tenants: {len(self.cleanup_results['affected_tenants']):,}")
        
        if not self.dry_run:
            print(f"✅ Queries executed: {execution_results.get('queries_executed', 0):,}")
            print(f"🗑️  Records actually deleted: {execution_results.get('records_deleted', 0):,}")
            
            errors = execution_results.get('errors', [])
            if errors:
                print(f"❌ Errors encountered: {len(errors)}")
                for error in errors[:5]:  # Show first 5 errors
                    print(f"   • {error}")
            else:
                print(f"✅ No errors encountered")
        
        print(f"\n🏢 Affected Tenants:")
        for tenant_id in sorted(self.cleanup_results['affected_tenants']):
            print(f"   • {tenant_id}")
        
        if self.dry_run:
            print(f"\n⚠️  This was a DRY RUN - no actual changes were made")
            print(f"   To execute cleanup, run with --execute flag")
        
        print(f"\n{'='*60}")
    
    def run_cleanup(self) -> Dict[str, Any]:
        """Run the complete SCD violation cleanup process"""
        
        print("🧹 Starting SCD Type 2 Violation Cleanup")
        print("=" * 60)
        
        client = self.get_clickhouse_client()
        if not client:
            return {'status': 'error', 'message': 'Failed to connect to ClickHouse'}
        
        try:
            # Step 1: Identify SCD violations
            records_to_delete = self.identify_scd_violations(client)
            
            if not records_to_delete:
                print("✅ No SCD violations found - SCD logic is working correctly!")
                return {'status': 'success', 'message': 'No SCD violations found'}
            
            # Step 2: Generate cleanup queries
            cleanup_queries = self.generate_cleanup_queries(records_to_delete)
            
            # Step 3: Execute cleanup (or show what would be done in dry run)
            execution_results = self.execute_cleanup(client, cleanup_queries)
            
            # Step 4: Print summary
            self.print_cleanup_summary(execution_results)
            
            # Combine results
            final_results = {**self.cleanup_results}
            final_results['execution_results'] = execution_results
            final_results['status'] = 'success'
            
            return final_results
            
        except Exception as e:
            error_msg = f"SCD cleanup failed: {str(e)}"
            print(f"❌ {error_msg}")
            self.cleanup_results['errors'].append(error_msg)
            return {**self.cleanup_results, 'status': 'error', 'message': error_msg}
            
        finally:
            client.close()


def main():
    parser = argparse.ArgumentParser(description='Cleanup SCD Type 2 Violations in ClickHouse time_entries')
    parser.add_argument('--execute', action='store_true',
                       help='Execute actual cleanup (default is dry run)')
    parser.add_argument('--credentials', choices=['aws', 'env'], default='aws',
                       help='Credential source (default: aws)')
    parser.add_argument('--save-report', action='store_true',
                       help='Save detailed report to JSON file')
    
    args = parser.parse_args()
    
    # Set AWS environment if using AWS secrets
    use_aws_secrets = args.credentials == 'aws'
    if use_aws_secrets:
        os.environ['AWS_SDK_LOAD_CONFIG'] = '1'
        if not os.environ.get('AWS_PROFILE'):
            os.environ['AWS_PROFILE'] = 'AdministratorAccess-123938354448'
    
    # Confirm execution if not dry run
    if args.execute:
        print("⚠️" * 20)
        print("🚨 WARNING: This will permanently delete SCD violating records!")
        print("⚠️" * 20)
        confirmation = input("Type 'FIX SCD VIOLATIONS' to confirm: ")
        if confirmation != 'FIX SCD VIOLATIONS':
            print("❌ SCD cleanup cancelled")
            sys.exit(1)
    
    cleanup = SCDViolationCleanup(
        use_aws_secrets=use_aws_secrets,
        dry_run=not args.execute
    )
    
    results = cleanup.run_cleanup()
    
    if args.save_report:
        report_file = f"scd_violations_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n📄 Detailed report saved to: {report_file}")
    
    # Exit with appropriate code
    if results.get('status') == 'error':
        sys.exit(2)
    elif results.get('records_to_delete', 0) > 0:
        sys.exit(1 if args.execute else 0)  # Exit 1 if violations found and executed
    else:
        sys.exit(0)  # No violations found


if __name__ == "__main__":
    main()