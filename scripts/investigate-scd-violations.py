#!/usr/bin/env python3
"""
SCD Type 2 Violation Investigation Script
========================================

This script identifies records that violate SCD Type 2 logic:
- Same tenant_id, ID, and last_updated timestamp
- But different is_current values (one true, one false)

This should never happen - records with same ID and last_updated 
should represent the same version and have the same is_current status.
"""

import os
import sys
import json
import boto3
import clickhouse_connect
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional

class SCDViolationInvestigator:
    def __init__(self, use_aws_secrets=True):
        self.use_aws_secrets = use_aws_secrets
        self.region = 'us-east-2'
        
        self.investigation_results = {
            'timestamp': datetime.now().isoformat(),
            'scd_violations_found': 0,
            'affected_tenants': set(),
            'violation_patterns': {},
            'sample_violations': []
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
    
    def investigate_scd_violations(self, client) -> List[Dict[str, Any]]:
        """Identify SCD Type 2 violations where same ID+last_updated have different is_current values"""
        
        print("🔍 Investigating SCD Type 2 violations...")
        
        # Find records with same tenant_id, id, last_updated but different is_current values
        scd_violations_query = """
        SELECT 
            tenant_id,
            id,
            last_updated,
            COUNT(*) as total_records,
            COUNT(DISTINCT is_current) as distinct_is_current_values,
            SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) as current_count,
            SUM(CASE WHEN is_current = false THEN 1 ELSE 0 END) as non_current_count,
            MIN(effective_date) as first_effective_date,
            MAX(effective_date) as last_effective_date,
            MIN(date_entered) as first_date_entered,
            MAX(date_entered) as last_date_entered,
            groupArray(effective_date) as all_effective_dates,
            groupArray(is_current) as all_is_current_values
        FROM time_entries
        GROUP BY tenant_id, id, last_updated
        HAVING COUNT(DISTINCT is_current) > 1
        ORDER BY total_records DESC, tenant_id, id
        LIMIT 50
        """
        
        result = client.query(scd_violations_query)
        
        violations = []
        for row in result.result_rows:
            (tenant_id, record_id, last_updated, total_records, distinct_is_current, 
             current_count, non_current_count, first_effective, last_effective,
             first_entered, last_entered, all_effective_dates, all_is_current_values) = row
            
            violation = {
                'tenant_id': tenant_id,
                'id': record_id,
                'last_updated': last_updated,
                'total_records': total_records,
                'current_count': current_count,
                'non_current_count': non_current_count,
                'first_effective_date': first_effective,
                'last_effective_date': last_effective,
                'first_date_entered': first_entered,
                'last_date_entered': last_entered,
                'effective_date_span_hours': (last_effective - first_effective).total_seconds() / 3600 if last_effective != first_effective else 0,
                'all_effective_dates': all_effective_dates,
                'all_is_current_values': all_is_current_values
            }
            
            violations.append(violation)
            self.investigation_results['affected_tenants'].add(tenant_id)
        
        self.investigation_results['scd_violations_found'] = len(violations)
        self.investigation_results['sample_violations'] = violations[:10]  # Store first 10 for reporting
        
        print(f"   📊 Found {len(violations)} SCD Type 2 violations")
        print(f"   🏢 {len(self.investigation_results['affected_tenants'])} tenants affected")
        
        return violations
    
    def analyze_violation_patterns(self, violations: List[Dict[str, Any]]):
        """Analyze patterns in SCD violations"""
        
        print("📊 Analyzing SCD violation patterns...")
        
        patterns = {
            'by_tenant': {},
            'by_current_non_current_ratio': {},
            'by_effective_date_span': {
                'same_effective_date': 0,
                'different_effective_dates': 0,
                'span_less_than_1_hour': 0,
                'span_1_to_24_hours': 0,
                'span_more_than_24_hours': 0
            },
            'by_record_count': {}
        }
        
        for violation in violations:
            tenant_id = violation['tenant_id']
            current_count = violation['current_count']
            non_current_count = violation['non_current_count']
            total_records = violation['total_records']
            span_hours = violation['effective_date_span_hours']
            
            # By tenant
            if tenant_id not in patterns['by_tenant']:
                patterns['by_tenant'][tenant_id] = 0
            patterns['by_tenant'][tenant_id] += 1
            
            # By current/non-current ratio
            ratio_key = f"{current_count}current_{non_current_count}non_current"
            if ratio_key not in patterns['by_current_non_current_ratio']:
                patterns['by_current_non_current_ratio'][ratio_key] = 0
            patterns['by_current_non_current_ratio'][ratio_key] += 1
            
            # By effective date span
            if span_hours == 0:
                patterns['by_effective_date_span']['same_effective_date'] += 1
            else:
                patterns['by_effective_date_span']['different_effective_dates'] += 1
                if span_hours < 1:
                    patterns['by_effective_date_span']['span_less_than_1_hour'] += 1
                elif span_hours <= 24:
                    patterns['by_effective_date_span']['span_1_to_24_hours'] += 1
                else:
                    patterns['by_effective_date_span']['span_more_than_24_hours'] += 1
            
            # By record count
            if total_records not in patterns['by_record_count']:
                patterns['by_record_count'][total_records] = 0
            patterns['by_record_count'][total_records] += 1
        
        self.investigation_results['violation_patterns'] = patterns
        
        print(f"   📈 Pattern analysis complete")
        return patterns
    
    def print_investigation_summary(self, violations: List[Dict[str, Any]], patterns: Dict[str, Any]):
        """Print comprehensive investigation summary"""
        
        print(f"\n{'='*80}")
        print(f"📋 SCD TYPE 2 VIOLATION INVESTIGATION SUMMARY")
        print(f"{'='*80}")
        
        print(f"🔍 SCD Violations Found: {self.investigation_results['scd_violations_found']:,}")
        print(f"🏢 Affected Tenants: {len(self.investigation_results['affected_tenants']):,}")
        
        if violations:
            print(f"\n🏢 Top Affected Tenants:")
            for tenant_id, count in sorted(patterns['by_tenant'].items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"   • {tenant_id}: {count} violations")
            
            print(f"\n📊 Current/Non-Current Distribution:")
            for ratio, count in sorted(patterns['by_current_non_current_ratio'].items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"   • {ratio}: {count} violations")
            
            print(f"\n⏰ Effective Date Span Analysis:")
            span_patterns = patterns['by_effective_date_span']
            print(f"   • Same effective date: {span_patterns['same_effective_date']} ({span_patterns['same_effective_date']/len(violations)*100:.1f}%)")
            print(f"   • Different effective dates: {span_patterns['different_effective_dates']} ({span_patterns['different_effective_dates']/len(violations)*100:.1f}%)")
            if span_patterns['different_effective_dates'] > 0:
                print(f"     - <1 hour span: {span_patterns['span_less_than_1_hour']}")
                print(f"     - 1-24 hour span: {span_patterns['span_1_to_24_hours']}")
                print(f"     - >24 hour span: {span_patterns['span_more_than_24_hours']}")
            
            print(f"\n📋 Sample SCD Violations (first 5):")
            for i, violation in enumerate(violations[:5], 1):
                print(f"   {i}. Tenant: {violation['tenant_id']}, ID: {violation['id']}")
                print(f"      LastUpdated: {violation['last_updated']}")
                print(f"      Records: {violation['current_count']} current + {violation['non_current_count']} non-current = {violation['total_records']} total")
                print(f"      Effective date span: {violation['effective_date_span_hours']:.2f} hours")
                print(f"      Effective dates: {violation['all_effective_dates']}")
                print(f"      is_current values: {violation['all_is_current_values']}")
        
        print(f"\n🚨 RECOMMENDATIONS:")
        if violations:
            print(f"   🔴 CRITICAL: SCD Type 2 violations detected")
            print(f"      Found {len(violations)} groups with same ID+last_updated but different is_current values")
            print(f"      Action: Run cleanup to fix SCD logic violations")
            
            same_effective_count = patterns['by_effective_date_span']['same_effective_date']
            if same_effective_count > 0:
                print(f"   🟡 HIGH: {same_effective_count} violations have identical effective dates")
                print(f"      These are likely true duplicates that should be merged")
            
            different_effective_count = patterns['by_effective_date_span']['different_effective_dates']
            if different_effective_count > 0:
                print(f"   🟡 MEDIUM: {different_effective_count} violations have different effective dates")
                print(f"      These may need manual review to determine correct is_current status")
        else:
            print(f"   ✅ No SCD Type 2 violations found - SCD logic is working correctly")
        
        print(f"\n{'='*80}")
    
    def run_investigation(self) -> Dict[str, Any]:
        """Run the complete SCD violation investigation"""
        
        print("🔍 Starting SCD Type 2 Violation Investigation")
        print("=" * 80)
        
        client = self.get_clickhouse_client()
        if not client:
            return {'status': 'error', 'message': 'Failed to connect to ClickHouse'}
        
        try:
            # Step 1: Investigate SCD violations
            violations = self.investigate_scd_violations(client)
            
            # Step 2: Analyze patterns
            patterns = self.analyze_violation_patterns(violations)
            
            # Step 3: Print summary
            self.print_investigation_summary(violations, patterns)
            
            # Return results
            final_results = {**self.investigation_results}
            final_results['violations'] = violations
            final_results['patterns'] = patterns
            final_results['status'] = 'success'
            
            return final_results
            
        except Exception as e:
            error_msg = f"Investigation failed: {str(e)}"
            print(f"❌ {error_msg}")
            return {'status': 'error', 'message': error_msg}
            
        finally:
            client.close()


def main():
    parser = argparse.ArgumentParser(description='Investigate SCD Type 2 Violations in ClickHouse time_entries')
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
    
    investigator = SCDViolationInvestigator(use_aws_secrets=use_aws_secrets)
    results = investigator.run_investigation()
    
    if args.save_report:
        report_file = f"scd_violations_investigation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n📄 Detailed report saved to: {report_file}")
    
    # Exit with appropriate code
    if results.get('status') == 'error':
        sys.exit(2)
    elif results.get('scd_violations_found', 0) > 0:
        sys.exit(1)  # Exit 1 if violations found
    else:
        sys.exit(0)  # No violations found


if __name__ == "__main__":
    main()