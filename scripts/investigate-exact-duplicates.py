#!/usr/bin/env python3
"""
Exact Duplicate Investigation Script
===================================

This script investigates the exact duplicate issue where records have:
- Same ID
- Same lastUpdated time
- Same is_current = false

This should NEVER happen and indicates a serious data loading issue.
"""

import os
import sys
import json
import boto3
import clickhouse_connect
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional

class ExactDuplicateInvestigator:
    def __init__(self, use_aws_secrets=True):
        self.use_aws_secrets = use_aws_secrets
        self.region = 'us-east-2'
        
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
    
    def investigate_exact_duplicates(self, client) -> Dict[str, Any]:
        """Investigate exact duplicates with same ID and lastUpdated"""
        
        print("🔍 Investigating Exact Duplicates in time_entries")
        print("=" * 60)
        
        investigation_results = {
            'timestamp': datetime.now().isoformat(),
            'exact_duplicates_found': 0,
            'affected_ids': [],
            'sample_duplicates': [],
            'patterns': {},
            'recommendations': []
        }
        
        try:
            # Find exact duplicates: same ID, same last_updated, same tenant_id
            exact_duplicates_query = """
            SELECT
                tenant_id,
                id,
                last_updated,
                is_current,
                COUNT(*) as duplicate_count,
                MIN(effective_date) as first_effective_date,
                MAX(effective_date) as last_effective_date,
                MIN(date_entered) as first_date_entered,
                MAX(date_entered) as last_date_entered
            FROM time_entries
            GROUP BY tenant_id, id, last_updated, is_current
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC, tenant_id, id
            LIMIT 50
            """
            
            result = client.query(exact_duplicates_query)
            exact_duplicates = []
            
            for row in result.result_rows:
                tenant_id, record_id, last_updated, is_current, count, first_eff, last_eff, first_created, last_created = row
                
                duplicate_info = {
                    'tenant_id': tenant_id,
                    'id': record_id,
                    'lastUpdated': str(last_updated),
                    'is_current': is_current,
                    'duplicate_count': count,
                    'first_effective_date': str(first_eff),
                    'last_effective_date': str(last_eff),
                    'first_created_date': str(first_created),
                    'last_created_date': str(last_created),
                    'time_span_hours': (last_created - first_created).total_seconds() / 3600 if last_created and first_created else 0
                }
                
                exact_duplicates.append(duplicate_info)
                investigation_results['affected_ids'].append(f"{tenant_id}:{record_id}")
            
            investigation_results['exact_duplicates_found'] = len(exact_duplicates)
            investigation_results['sample_duplicates'] = exact_duplicates[:10]  # First 10 for analysis
            
            # Analyze patterns
            if exact_duplicates:
                # Pattern 1: is_current distribution
                current_true_count = sum(1 for d in exact_duplicates if d['is_current'])
                current_false_count = sum(1 for d in exact_duplicates if not d['is_current'])
                
                investigation_results['patterns']['is_current_distribution'] = {
                    'true_count': current_true_count,
                    'false_count': current_false_count
                }
                
                # Pattern 2: Time span analysis
                time_spans = [d['time_span_hours'] for d in exact_duplicates if d['time_span_hours'] > 0]
                if time_spans:
                    investigation_results['patterns']['time_span_analysis'] = {
                        'min_hours': min(time_spans),
                        'max_hours': max(time_spans),
                        'avg_hours': sum(time_spans) / len(time_spans),
                        'same_time_duplicates': sum(1 for span in time_spans if span < 0.01)  # Less than 36 seconds
                    }
                
                # Pattern 3: Tenant distribution
                tenant_counts = {}
                for d in exact_duplicates:
                    tenant_id = d['tenant_id']
                    tenant_counts[tenant_id] = tenant_counts.get(tenant_id, 0) + 1
                
                investigation_results['patterns']['tenant_distribution'] = dict(sorted(tenant_counts.items(), key=lambda x: x[1], reverse=True)[:10])
            
            # Get total counts for context
            total_query = """
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT tenant_id, id, last_updated) as unique_combinations,
                COUNT(*) - COUNT(DISTINCT tenant_id, id, last_updated) as total_exact_duplicates
            FROM time_entries
            """
            
            total_result = client.query(total_query)
            if total_result.result_rows:
                total_records, unique_combinations, total_exact_duplicates = total_result.result_rows[0]
                investigation_results['total_records'] = total_records
                investigation_results['unique_combinations'] = unique_combinations
                investigation_results['total_exact_duplicates'] = total_exact_duplicates
                investigation_results['exact_duplicate_percentage'] = (total_exact_duplicates / total_records * 100) if total_records > 0 else 0
            
            # Generate recommendations
            self.generate_recommendations(investigation_results)
            
            return investigation_results
            
        except Exception as e:
            print(f"Error during investigation: {e}")
            investigation_results['error'] = str(e)
            return investigation_results
    
    def generate_recommendations(self, results: Dict[str, Any]):
        """Generate recommendations based on investigation results"""
        
        recommendations = []
        
        exact_duplicates = results.get('exact_duplicates_found', 0)
        total_exact_duplicates = results.get('total_exact_duplicates', 0)
        
        if exact_duplicates > 0:
            recommendations.append({
                'priority': 'CRITICAL',
                'issue': 'Exact duplicates detected',
                'description': f'Found {exact_duplicates} groups of exact duplicates (total {total_exact_duplicates} duplicate records)',
                'action': 'Immediate cleanup required - these should never exist'
            })
            
            # Check if mostly is_current = false
            patterns = results.get('patterns', {})
            is_current_dist = patterns.get('is_current_distribution', {})
            
            if is_current_dist.get('false_count', 0) > is_current_dist.get('true_count', 0):
                recommendations.append({
                    'priority': 'HIGH',
                    'issue': 'Historical duplicates predominant',
                    'description': f"Most duplicates have is_current=false ({is_current_dist.get('false_count', 0)} vs {is_current_dist.get('true_count', 0)})",
                    'action': 'Investigate data loading process - likely inserting duplicates during historical loads'
                })
            
            # Check time span patterns
            time_analysis = patterns.get('time_span_analysis', {})
            same_time_count = time_analysis.get('same_time_duplicates', 0)
            
            if same_time_count > 0:
                recommendations.append({
                    'priority': 'CRITICAL',
                    'issue': 'Simultaneous duplicate creation',
                    'description': f'{same_time_count} duplicate groups created within seconds of each other',
                    'action': 'Check for race conditions or bulk loading issues in data pipeline'
                })
            
            # Check tenant concentration
            tenant_dist = patterns.get('tenant_distribution', {})
            if tenant_dist:
                top_tenant = max(tenant_dist.items(), key=lambda x: x[1])
                if top_tenant[1] > exact_duplicates * 0.5:  # One tenant has >50% of duplicates
                    recommendations.append({
                        'priority': 'HIGH',
                        'issue': 'Tenant-specific duplication',
                        'description': f'Tenant {top_tenant[0]} has {top_tenant[1]} duplicate groups ({top_tenant[1]/exact_duplicates*100:.1f}%)',
                        'action': 'Investigate data source or processing for this specific tenant'
                    })
        
        results['recommendations'] = recommendations
    
    def print_investigation_summary(self, results: Dict[str, Any]):
        """Print investigation summary"""
        
        print(f"\n{'='*60}")
        print(f"📋 EXACT DUPLICATE INVESTIGATION SUMMARY")
        print(f"{'='*60}")
        
        if 'error' in results:
            print(f"❌ Investigation failed: {results['error']}")
            return
        
        total_records = results.get('total_records', 0)
        exact_duplicates = results.get('exact_duplicates_found', 0)
        total_exact_duplicates = results.get('total_exact_duplicates', 0)
        percentage = results.get('exact_duplicate_percentage', 0)
        
        print(f"📊 Total Records: {total_records:,}")
        print(f"🔍 Exact Duplicate Groups: {exact_duplicates:,}")
        print(f"📈 Total Duplicate Records: {total_exact_duplicates:,}")
        print(f"📉 Duplicate Percentage: {percentage:.2f}%")
        
        # Print patterns
        patterns = results.get('patterns', {})
        
        if 'is_current_distribution' in patterns:
            dist = patterns['is_current_distribution']
            print(f"\n🔄 is_current Distribution:")
            print(f"   ✅ is_current=true: {dist.get('true_count', 0):,} groups")
            print(f"   ❌ is_current=false: {dist.get('false_count', 0):,} groups")
        
        if 'time_span_analysis' in patterns:
            time_analysis = patterns['time_span_analysis']
            print(f"\n⏰ Time Span Analysis:")
            print(f"   📅 Min span: {time_analysis.get('min_hours', 0):.2f} hours")
            print(f"   📅 Max span: {time_analysis.get('max_hours', 0):.2f} hours")
            print(f"   📅 Avg span: {time_analysis.get('avg_hours', 0):.2f} hours")
            print(f"   ⚡ Same-time duplicates: {time_analysis.get('same_time_duplicates', 0):,}")
        
        if 'tenant_distribution' in patterns:
            tenant_dist = patterns['tenant_distribution']
            print(f"\n🏢 Top Affected Tenants:")
            for tenant_id, count in list(tenant_dist.items())[:5]:
                print(f"   • {tenant_id}: {count:,} duplicate groups")
        
        # Print sample duplicates
        samples = results.get('sample_duplicates', [])
        if samples:
            print(f"\n📋 Sample Duplicates (first 5):")
            for i, sample in enumerate(samples[:5], 1):
                print(f"   {i}. Tenant: {sample['tenant_id']}, ID: {sample['id']}")
                print(f"      LastUpdated: {sample['lastUpdated']}, Count: {sample['duplicate_count']}")
                print(f"      is_current: {sample['is_current']}, Time span: {sample['time_span_hours']:.2f}h")
        
        # Print recommendations
        recommendations = results.get('recommendations', [])
        if recommendations:
            print(f"\n🚨 RECOMMENDATIONS:")
            for rec in recommendations:
                priority_icon = {'CRITICAL': '🔴', 'HIGH': '🟡', 'MEDIUM': '🟠'}.get(rec['priority'], '⚠️')
                print(f"   {priority_icon} {rec['priority']}: {rec['issue']}")
                print(f"      {rec['description']}")
                print(f"      Action: {rec['action']}")
        
        print(f"\n{'='*60}")
    
    def run_investigation(self) -> Dict[str, Any]:
        """Run complete exact duplicate investigation"""
        
        client = self.get_clickhouse_client()
        if not client:
            return {'status': 'error', 'message': 'Failed to connect to ClickHouse'}
        
        try:
            results = self.investigate_exact_duplicates(client)
            self.print_investigation_summary(results)
            return results
            
        finally:
            client.close()


def main():
    parser = argparse.ArgumentParser(description='Investigate Exact Duplicates in ClickHouse')
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
    
    investigator = ExactDuplicateInvestigator(use_aws_secrets=use_aws_secrets)
    results = investigator.run_investigation()
    
    if args.save_report:
        report_file = f"exact_duplicates_investigation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n📄 Detailed report saved to: {report_file}")
    
    # Exit with appropriate code based on findings
    if 'error' in results:
        sys.exit(2)
    elif results.get('exact_duplicates_found', 0) > 0:
        sys.exit(1)  # Duplicates found
    else:
        sys.exit(0)  # No duplicates


if __name__ == "__main__":
    main()