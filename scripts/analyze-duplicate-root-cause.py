#!/usr/bin/env python3
"""
Duplicate Root Cause Analysis Script
===================================

This script analyzes the patterns in exact duplicates to identify the root cause
of why records with same ID and lastUpdated are being created multiple times.
"""

import os
import sys
import json
import boto3
import clickhouse_connect
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

class DuplicateRootCauseAnalyzer:
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
    
    def analyze_creation_patterns(self, client) -> Dict[str, Any]:
        """Analyze when and how duplicates are being created"""
        
        print("🔍 Analyzing duplicate creation patterns...")
        
        # Get detailed information about duplicate groups
        duplicate_analysis_query = """
        WITH duplicate_groups AS (
            SELECT
                tenant_id,
                id,
                last_updated,
                is_current,
                COUNT(*) as duplicate_count,
                MIN(date_entered) as first_created,
                MAX(date_entered) as last_created,
                MAX(date_entered) - MIN(date_entered) as time_span,
                groupArray(date_entered) as all_date_entereds,
                groupArray(effective_date) as all_effective_dates
            FROM time_entries
            GROUP BY tenant_id, id, last_updated, is_current
            HAVING COUNT(*) > 1
        )
        SELECT
            tenant_id,
            id,
            last_updated,
            is_current,
            duplicate_count,
            first_created,
            last_created,
            time_span,
            all_date_entereds,
            all_effective_dates
        FROM duplicate_groups
        ORDER BY duplicate_count DESC, time_span DESC
        LIMIT 20
        """
        
        result = client.query(duplicate_analysis_query)
        
        patterns = {
            'total_duplicate_groups': 0,
            'simultaneous_creation': 0,  # Created within 1 minute
            'rapid_creation': 0,         # Created within 1 hour
            'delayed_creation': 0,       # Created more than 1 hour apart
            'same_effective_date': 0,    # Same effective_date
            'different_effective_date': 0, # Different effective_date
            'sample_groups': []
        }
        
        for row in result.result_rows:
            tenant_id, record_id, last_updated, is_current, dup_count, first_created, last_created, time_span, date_entereds, effective_dates = row
            
            patterns['total_duplicate_groups'] += 1
            
            # Analyze time span
            time_span_seconds = time_span.total_seconds() if time_span else 0
            
            if time_span_seconds < 60:  # Less than 1 minute
                patterns['simultaneous_creation'] += 1
            elif time_span_seconds < 3600:  # Less than 1 hour
                patterns['rapid_creation'] += 1
            else:
                patterns['delayed_creation'] += 1
            
            # Analyze effective dates
            unique_effective_dates = len(set(str(d) for d in effective_dates))
            if unique_effective_dates == 1:
                patterns['same_effective_date'] += 1
            else:
                patterns['different_effective_date'] += 1
            
            # Store sample for detailed analysis
            patterns['sample_groups'].append({
                'tenant_id': tenant_id,
                'id': record_id,
                'lastUpdated': str(last_updated),
                'is_current': is_current,
                'duplicate_count': dup_count,
                'time_span_seconds': time_span_seconds,
                'date_entereds': [str(d) for d in date_entereds],
                'effective_dates': [str(d) for d in effective_dates],
                'unique_effective_dates': unique_effective_dates
            })
        
        return patterns
    
    def analyze_loading_frequency(self, client) -> Dict[str, Any]:
        """Analyze how frequently data is being loaded"""
        
        print("📊 Analyzing data loading frequency...")
        
        # Look at creation patterns over time
        loading_frequency_query = """
        SELECT
            toDate(date_entered) as load_date,
            toHour(date_entered) as load_hour,
            COUNT(*) as records_loaded,
            COUNT(DISTINCT tenant_id, id, last_updated) as unique_records,
            COUNT(*) - COUNT(DISTINCT tenant_id, id, last_updated) as duplicates_created
        FROM time_entries
        WHERE date_entered >= subtractDays(now(), 7)
        GROUP BY toDate(date_entered), toHour(date_entered)
        HAVING duplicates_created > 0
        ORDER BY load_date DESC, load_hour DESC
        LIMIT 50
        """
        
        result = client.query(loading_frequency_query)
        
        frequency_analysis = {
            'problematic_loads': [],
            'total_problematic_hours': 0,
            'peak_duplicate_hour': None,
            'max_duplicates_in_hour': 0
        }
        
        for row in result.result_rows:
            load_date, load_hour, records_loaded, unique_records, duplicates_created = row
            
            frequency_analysis['total_problematic_hours'] += 1
            
            load_info = {
                'date': str(load_date),
                'hour': load_hour,
                'records_loaded': records_loaded,
                'unique_records': unique_records,
                'duplicates_created': duplicates_created,
                'duplicate_rate': (duplicates_created / records_loaded * 100) if records_loaded > 0 else 0
            }
            
            frequency_analysis['problematic_loads'].append(load_info)
            
            if duplicates_created > frequency_analysis['max_duplicates_in_hour']:
                frequency_analysis['max_duplicates_in_hour'] = duplicates_created
                frequency_analysis['peak_duplicate_hour'] = load_info
        
        return frequency_analysis
    
    def analyze_pipeline_issues(self, client) -> Dict[str, Any]:
        """Analyze potential pipeline issues causing duplicates"""
        
        print("🔧 Analyzing potential pipeline issues...")
        
        # Check for records with identical content but different timestamps
        pipeline_analysis_query = """
        WITH content_groups AS (
            SELECT
                tenant_id,
                id,
                last_updated,
                is_current,
                -- Group by actual content fields to see if content is identical
                groupArray(date_entered) as date_entereds,
                groupArray(effective_date) as effective_dates,
                COUNT(*) as record_count
            FROM time_entries
            WHERE (tenant_id, id, last_updated, is_current) IN (
                SELECT tenant_id, id, last_updated, is_current
                FROM time_entries
                GROUP BY tenant_id, id, last_updated, is_current
                HAVING COUNT(*) > 1
            )
            GROUP BY tenant_id, id, last_updated, is_current
        )
        SELECT
            COUNT(*) as total_groups,
            SUM(record_count) as total_duplicate_records,
            AVG(record_count) as avg_duplicates_per_group,
            MAX(record_count) as max_duplicates_per_group
        FROM content_groups
        """
        
        result = client.query(pipeline_analysis_query)
        
        pipeline_issues = {}
        if result.result_rows:
            total_groups, total_dup_records, avg_dups, max_dups = result.result_rows[0]
            pipeline_issues = {
                'total_duplicate_groups': total_groups,
                'total_duplicate_records': total_dup_records,
                'average_duplicates_per_group': float(avg_dups) if avg_dups else 0,
                'max_duplicates_per_group': max_dups
            }
        
        return pipeline_issues
    
    def generate_root_cause_hypothesis(self, patterns: Dict[str, Any], frequency: Dict[str, Any], pipeline: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate hypotheses about the root cause"""
        
        hypotheses = []
        
        # Hypothesis 1: Simultaneous processing
        simultaneous_pct = (patterns['simultaneous_creation'] / patterns['total_duplicate_groups'] * 100) if patterns['total_duplicate_groups'] > 0 else 0
        if simultaneous_pct > 50:
            hypotheses.append({
                'hypothesis': 'Race Condition in Data Loading',
                'confidence': 'HIGH',
                'evidence': f"{simultaneous_pct:.1f}% of duplicates created within 1 minute",
                'likely_cause': 'Multiple Lambda functions or processes loading the same data simultaneously',
                'recommendation': 'Implement proper locking or deduplication in the data loader'
            })
        
        # Hypothesis 2: Retry mechanism
        rapid_pct = (patterns['rapid_creation'] / patterns['total_duplicate_groups'] * 100) if patterns['total_duplicate_groups'] > 0 else 0
        if rapid_pct > 30:
            hypotheses.append({
                'hypothesis': 'Retry Mechanism Creating Duplicates',
                'confidence': 'MEDIUM',
                'evidence': f"{rapid_pct:.1f}% of duplicates created within 1 hour",
                'likely_cause': 'Failed processing being retried without proper deduplication',
                'recommendation': 'Add idempotency checks to prevent duplicate insertions on retry'
            })
        
        # Hypothesis 3: Same effective date issue
        same_eff_pct = (patterns['same_effective_date'] / patterns['total_duplicate_groups'] * 100) if patterns['total_duplicate_groups'] > 0 else 0
        if same_eff_pct > 70:
            hypotheses.append({
                'hypothesis': 'SCD Logic Failure',
                'confidence': 'HIGH',
                'evidence': f"{same_eff_pct:.1f}% of duplicates have identical effective_date",
                'likely_cause': 'SCD Type 2 logic not properly checking for existing records',
                'recommendation': 'Fix SCD processor to check for existing records before inserting'
            })
        
        # Hypothesis 4: Bulk loading issue
        if frequency['max_duplicates_in_hour'] > 100:
            hypotheses.append({
                'hypothesis': 'Bulk Loading Without Deduplication',
                'confidence': 'HIGH',
                'evidence': f"Peak of {frequency['max_duplicates_in_hour']} duplicates created in single hour",
                'likely_cause': 'Bulk data loads not checking for existing records',
                'recommendation': 'Implement DISTINCT or UPSERT logic in bulk loading process'
            })
        
        # Hypothesis 5: Pipeline reprocessing
        avg_dups = pipeline.get('average_duplicates_per_group', 0)
        if avg_dups > 3:
            hypotheses.append({
                'hypothesis': 'Pipeline Reprocessing Same Data',
                'confidence': 'MEDIUM',
                'evidence': f"Average of {avg_dups:.1f} duplicates per group",
                'likely_cause': 'Pipeline reprocessing the same source data multiple times',
                'recommendation': 'Add processing state tracking to prevent reprocessing'
            })
        
        return hypotheses
    
    def print_analysis_results(self, patterns: Dict[str, Any], frequency: Dict[str, Any], pipeline: Dict[str, Any], hypotheses: List[Dict[str, Any]]):
        """Print comprehensive analysis results"""
        
        print(f"\n{'='*70}")
        print(f"📋 DUPLICATE ROOT CAUSE ANALYSIS")
        print(f"{'='*70}")
        
        # Creation Patterns
        print(f"\n🔍 CREATION PATTERNS:")
        print(f"   📊 Total duplicate groups: {patterns['total_duplicate_groups']:,}")
        print(f"   ⚡ Simultaneous creation (<1 min): {patterns['simultaneous_creation']:,} ({patterns['simultaneous_creation']/patterns['total_duplicate_groups']*100:.1f}%)")
        print(f"   🏃 Rapid creation (<1 hour): {patterns['rapid_creation']:,} ({patterns['rapid_creation']/patterns['total_duplicate_groups']*100:.1f}%)")
        print(f"   🐌 Delayed creation (>1 hour): {patterns['delayed_creation']:,} ({patterns['delayed_creation']/patterns['total_duplicate_groups']*100:.1f}%)")
        print(f"   📅 Same effective_date: {patterns['same_effective_date']:,} ({patterns['same_effective_date']/patterns['total_duplicate_groups']*100:.1f}%)")
        
        # Loading Frequency
        print(f"\n📊 LOADING FREQUENCY:")
        print(f"   🕐 Problematic hours: {frequency['total_problematic_hours']:,}")
        print(f"   📈 Peak duplicates/hour: {frequency['max_duplicates_in_hour']:,}")
        if frequency['peak_duplicate_hour']:
            peak = frequency['peak_duplicate_hour']
            print(f"   📅 Peak occurred: {peak['date']} at {peak['hour']}:00")
            print(f"   📊 Peak duplicate rate: {peak['duplicate_rate']:.1f}%")
        
        # Pipeline Issues
        print(f"\n🔧 PIPELINE ANALYSIS:")
        print(f"   📊 Total duplicate groups: {pipeline.get('total_duplicate_groups', 0):,}")
        print(f"   📈 Total duplicate records: {pipeline.get('total_duplicate_records', 0):,}")
        print(f"   📊 Avg duplicates/group: {pipeline.get('average_duplicates_per_group', 0):.1f}")
        print(f"   📈 Max duplicates/group: {pipeline.get('max_duplicates_per_group', 0):,}")
        
        # Root Cause Hypotheses
        print(f"\n🎯 ROOT CAUSE HYPOTHESES:")
        for i, hypothesis in enumerate(hypotheses, 1):
            confidence_icon = {'HIGH': '🔴', 'MEDIUM': '🟡', 'LOW': '🟢'}.get(hypothesis['confidence'], '⚪')
            print(f"   {i}. {confidence_icon} {hypothesis['hypothesis']} ({hypothesis['confidence']} confidence)")
            print(f"      Evidence: {hypothesis['evidence']}")
            print(f"      Likely cause: {hypothesis['likely_cause']}")
            print(f"      Recommendation: {hypothesis['recommendation']}")
            print()
        
        # Sample Groups
        print(f"\n📋 SAMPLE DUPLICATE GROUPS (first 5):")
        for i, sample in enumerate(patterns['sample_groups'][:5], 1):
            print(f"   {i}. Tenant: {sample['tenant_id']}, ID: {sample['id']}")
            print(f"      Duplicates: {sample['duplicate_count']}, Time span: {sample['time_span_seconds']:.0f}s")
            print(f"      Created: {sample['date_entereds'][0]} → {sample['date_entereds'][-1]}")
            print(f"      Effective dates: {sample['unique_effective_dates']} unique")
        
        print(f"\n{'='*70}")
    
    def run_analysis(self) -> Dict[str, Any]:
        """Run complete root cause analysis"""
        
        print("🔍 Starting Duplicate Root Cause Analysis")
        print("=" * 70)
        
        client = self.get_clickhouse_client()
        if not client:
            return {'status': 'error', 'message': 'Failed to connect to ClickHouse'}
        
        try:
            # Analyze creation patterns
            patterns = self.analyze_creation_patterns(client)
            
            # Analyze loading frequency
            frequency = self.analyze_loading_frequency(client)
            
            # Analyze pipeline issues
            pipeline = self.analyze_pipeline_issues(client)
            
            # Generate hypotheses
            hypotheses = self.generate_root_cause_hypothesis(patterns, frequency, pipeline)
            
            # Print results
            self.print_analysis_results(patterns, frequency, pipeline, hypotheses)
            
            return {
                'status': 'success',
                'patterns': patterns,
                'frequency': frequency,
                'pipeline': pipeline,
                'hypotheses': hypotheses,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            error_msg = f"Analysis failed: {str(e)}"
            print(f"❌ {error_msg}")
            return {'status': 'error', 'message': error_msg}
            
        finally:
            client.close()


def main():
    parser = argparse.ArgumentParser(description='Analyze Root Cause of Exact Duplicates')
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
    
    analyzer = DuplicateRootCauseAnalyzer(use_aws_secrets=use_aws_secrets)
    results = analyzer.run_analysis()
    
    if args.save_report:
        report_file = f"duplicate_root_cause_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n📄 Detailed report saved to: {report_file}")
    
    # Exit with appropriate code
    if results.get('status') == 'error':
        sys.exit(2)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()