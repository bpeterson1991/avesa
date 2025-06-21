#!/usr/bin/env python3
"""
ClickHouse Schema Management Suite
=================================

Unified schema management script that consolidates functionality from:
- aws-clickhouse-schema-migration.py
- comprehensive-schema-migration.py

This script provides comprehensive ClickHouse schema management with multiple modes:
- migrate: Synchronize schema with canonical mappings
- validate: Validate current schema against mappings
- analyze: Analyze schema differences and generate reports
- init: Initialize schema from scratch
"""

import os
import sys
import json
import boto3
import clickhouse_connect
import argparse
from pathlib import Path
from typing import Dict, List, Set, Any
from datetime import datetime

class ClickHouseSchemaManager:
    def __init__(self, mode='migrate', use_aws_secrets=True):
        self.mode = mode
        self.use_aws_secrets = use_aws_secrets
        self.region = 'us-east-2'
        self.canonical_tables = ['companies', 'contacts', 'tickets', 'time_entries']
        
        # Initialize results storage
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'mode': mode,
            'tables_processed': 0,
            'tables_successful': 0,
            'migration_results': {},
            'validation_results': {},
            'schema_analysis': {}
        }
    
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
    
    def get_clickhouse_credentials_aws(self):
        """Get ClickHouse credentials from AWS Secrets Manager"""
        try:
            secrets_client = boto3.client('secretsmanager', region_name=self.region)
            secret_name = os.getenv('CLICKHOUSE_SECRET_NAME', 'clickhouse-connection-dev')
            
            self.print_result(f"Fetching ClickHouse credentials from: {secret_name}")
            
            response = secrets_client.get_secret_value(SecretId=secret_name)
            credentials = json.loads(response['SecretString'])
            
            self.print_result("Successfully retrieved ClickHouse credentials from AWS Secrets Manager")
            return credentials
            
        except Exception as e:
            self.print_result(f"Failed to get ClickHouse credentials from AWS: {e}", False)
            return None
    
    def get_clickhouse_credentials_env(self):
        """Get ClickHouse credentials from environment variables"""
        credentials = {
            'host': os.getenv('CLICKHOUSE_HOST', 'clickhouse.avesa.dev'),
            'port': int(os.getenv('CLICKHOUSE_PORT', '8443')),
            'username': os.getenv('CLICKHOUSE_USER', 'avesa_user'),
            'password': os.getenv('CLICKHOUSE_PASSWORD'),
            'database': os.getenv('CLICKHOUSE_DATABASE', 'default')
        }
        
        if not credentials['password']:
            self.print_result("CLICKHOUSE_PASSWORD environment variable not set", False)
            self.print_result("Please set your ClickHouse password:")
            print("export CLICKHOUSE_PASSWORD='your_password'")
            return None
        
        return credentials
    
    def get_clickhouse_client(self):
        """Get ClickHouse client using appropriate credential method"""
        
        # Get credentials
        if self.use_aws_secrets:
            credentials = self.get_clickhouse_credentials_aws()
        else:
            credentials = self.get_clickhouse_credentials_env()
        
        if not credentials:
            sys.exit(1)
        
        # Try to connect with SSL verification
        try:
            client = clickhouse_connect.get_client(
                host=credentials['host'],
                port=int(credentials.get('port', 8443)),
                username=credentials['username'],
                password=credentials['password'],
                database=credentials.get('database', 'default'),
                secure=True,
                verify=True,
                connect_timeout=30,
                send_receive_timeout=300
            )
            
            # Test connection
            result = client.query("SELECT 1 as test")
            self.print_result(f"Connected to ClickHouse at {credentials['host']}:{credentials.get('port', 8443)}")
            return client
            
        except Exception as e:
            self.print_result(f"Failed to connect with SSL verification: {e}", False)
            self.print_result("Trying with SSL verification disabled...")
            
            try:
                # Try with SSL verification disabled for development
                client = clickhouse_connect.get_client(
                    host=credentials['host'],
                    port=int(credentials.get('port', 8443)),
                    username=credentials['username'],
                    password=credentials['password'],
                    database=credentials.get('database', 'default'),
                    secure=True,
                    verify=False,
                    connect_timeout=30,
                    send_receive_timeout=300
                )
                
                # Test connection
                result = client.query("SELECT 1 as test")
                self.print_result(f"Connected to ClickHouse at {credentials['host']}:{credentials.get('port', 8443)} (SSL verification disabled)")
                return client
                
            except Exception as e2:
                self.print_result(f"Failed to connect to ClickHouse: {e2}", False)
                sys.exit(1)
    
    def load_canonical_mappings(self) -> Dict[str, Dict[str, Any]]:
        """Load all canonical mappings"""
        
        mappings = {}
        
        for table in self.canonical_tables:
            mapping_file = Path(__file__).parent.parent / 'mappings' / 'canonical' / f'{table}.json'
            
            if mapping_file.exists():
                with open(mapping_file, 'r') as f:
                    mappings[table] = json.load(f)
                    self.print_result(f"Loaded canonical mapping for {table}")
            else:
                self.print_result(f"Canonical mapping not found for {table}: {mapping_file}", False)
                mappings[table] = {}
        
        return mappings
    
    def extract_canonical_fields(self, mapping: Dict[str, Any]) -> Set[str]:
        """Extract all canonical field names from a mapping"""
        
        fields = set()
        
        for service_name, service_mapping in mapping.items():
            if isinstance(service_mapping, dict):
                for endpoint_path, field_mappings in service_mapping.items():
                    if isinstance(field_mappings, dict):
                        # The keys are the canonical field names
                        fields.update(field_mappings.keys())
        
        return fields
    
    def get_current_table_schema(self, client, table_name: str) -> Dict[str, str]:
        """Get current table schema from ClickHouse"""
        
        try:
            schema_query = """
            SELECT name, type 
            FROM system.columns 
            WHERE table = %s AND database = currentDatabase()
            ORDER BY name
            """
            
            result = client.query(schema_query, [table_name])
            schema = {row[0]: row[1] for row in result.result_rows}
            
            self.print_result(f"Current {table_name} table has {len(schema)} columns")
            return schema
            
        except Exception as e:
            self.print_result(f"Failed to get schema for {table_name}: {e}", False)
            return {}
    
    def determine_clickhouse_type(self, field_name: str) -> str:
        """Determine appropriate ClickHouse type for a field"""
        
        # Special handling for known field types
        type_mappings = {
            # Date fields
            'date_acquired': 'Nullable(Date)',
            'birth_day': 'Nullable(Date)',
            'anniversary': 'Nullable(Date)',
            'created_date': 'DateTime',
            'closed_date': 'Nullable(DateTime)',
            'required_date': 'Nullable(DateTime)',
            'time_start': 'Nullable(DateTime)',
            'time_end': 'Nullable(DateTime)',
            'date_entered': 'DateTime',
            'last_updated': 'DateTime',
            'effective_date': 'DateTime',
            'expiration_date': 'Nullable(DateTime)',
            'effective_start_date': 'DateTime',
            'effective_end_date': 'Nullable(DateTime)',
            'updated_date': 'DateTime',
            
            # Boolean fields
            'lead_flag': 'Nullable(Bool)',
            'unsubscribe_flag': 'Nullable(Bool)',
            'married_flag': 'Nullable(Bool)',
            'children_flag': 'Nullable(Bool)',
            'disable_portal_login_flag': 'Nullable(Bool)',
            'inactive_flag': 'Nullable(Bool)',
            'approved': 'Nullable(Bool)',
            'is_current': 'Bool',
            
            # Numeric fields
            'annual_revenue': 'Nullable(Float64)',
            'number_of_employees': 'Nullable(UInt32)',
            'budget_hours': 'Nullable(Float64)',
            'actual_hours': 'Nullable(Float64)',
            'hours_deduct': 'Nullable(Float64)',
            'record_version': 'UInt32',
            
            # ID fields
            'tenant_id': 'String',
            'id': 'String',
            'source_id': 'String',
            'record_hash': 'String',
        }
        
        # Check exact match first
        if field_name in type_mappings:
            return type_mappings[field_name]
        
        # Pattern-based matching
        if field_name.endswith('_id'):
            return 'Nullable(String)'
        elif field_name.endswith('_flag'):
            return 'Nullable(Bool)'
        elif field_name.endswith('_date'):
            return 'Nullable(DateTime)'
        elif field_name.endswith('_hours'):
            return 'Nullable(Float64)'
        elif field_name.endswith('_count'):
            return 'Nullable(UInt32)'
        elif 'revenue' in field_name or 'amount' in field_name:
            return 'Nullable(Float64)'
        elif 'number' in field_name and 'phone' not in field_name:
            return 'Nullable(UInt32)'
        else:
            # Default to nullable string
            return 'Nullable(String)'
    
    def analyze_schema_differences(self, table_name: str, current_schema: Dict[str, str], 
                                 canonical_fields: Set[str]) -> Dict[str, Any]:
        """Analyze differences between current schema and canonical fields"""
        
        missing_fields = canonical_fields - set(current_schema.keys())
        extra_fields = set(current_schema.keys()) - canonical_fields
        
        analysis = {
            'table_name': table_name,
            'current_columns': len(current_schema),
            'canonical_fields': len(canonical_fields),
            'missing_fields': sorted(missing_fields),
            'extra_fields': sorted(extra_fields),
            'missing_count': len(missing_fields),
            'extra_count': len(extra_fields),
            'is_synchronized': len(missing_fields) == 0
        }
        
        return analysis
    
    def generate_migration_statements(self, table_name: str, current_schema: Dict[str, str], 
                                    canonical_fields: Set[str]) -> List[str]:
        """Generate ALTER TABLE statements for missing fields"""
        
        missing_fields = canonical_fields - set(current_schema.keys())
        
        if not missing_fields:
            self.print_result(f"{table_name} table already has all canonical fields")
            return []
        
        self.print_result(f"{table_name} table missing {len(missing_fields)} fields: {sorted(missing_fields)}", False)
        
        statements = []
        for field in sorted(missing_fields):
            field_type = self.determine_clickhouse_type(field)
            statement = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {field} {field_type}"
            statements.append(statement)
        
        return statements
    
    def execute_migration_statements(self, client, table_name: str, statements: List[str]) -> bool:
        """Execute migration statements for a table"""
        
        if not statements:
            return True
        
        print(f"\n🔧 Migrating {table_name} table ({len(statements)} statements)")
        
        success = True
        for i, statement in enumerate(statements, 1):
            try:
                print(f"   {i}. {statement}")
                client.query(statement)
                print(f"      ✅ Success")
            except Exception as e:
                print(f"      ❌ Error: {e}")
                success = False
        
        return success
    
    def verify_table_schema(self, client, table_name: str, canonical_fields: Set[str]) -> bool:
        """Verify that table now has all canonical fields"""
        
        current_schema = self.get_current_table_schema(client, table_name)
        missing_fields = canonical_fields - set(current_schema.keys())
        
        if missing_fields:
            self.print_result(f"{table_name} still missing fields: {sorted(missing_fields)}", False)
            return False
        else:
            self.print_result(f"{table_name} has all {len(canonical_fields)} canonical fields")
            return True
    
    def validate_data_integrity(self, client) -> bool:
        """Validate that data integrity is maintained"""
        
        self.print_step("Validating Data Integrity")
        
        try:
            for table in self.canonical_tables:
                count_query = f"SELECT count() as total FROM {table} WHERE tenant_id = 'sitetechnology'"
                result = client.query(count_query)
                count = result.result_rows[0][0] if result.result_rows else 0
                
                self.print_result(f"{table}: {count} records")
                
                if count > 0:
                    # Check SCD integrity
                    scd_query = f"""
                    SELECT 
                        countIf(is_current = true) as current_records,
                        countIf(is_current = false) as historical_records
                    FROM {table} 
                    WHERE tenant_id = 'sitetechnology'
                    """
                    scd_result = client.query(scd_query)
                    if scd_result.result_rows:
                        current, historical = scd_result.result_rows[0]
                        self.print_result(f"  SCD: {current} current, {historical} historical")
            
            return True
            
        except Exception as e:
            self.print_result(f"Data integrity validation failed: {e}", False)
            return False
    
    def create_schema_utilities(self, client) -> bool:
        """Create schema management utilities"""
        
        self.print_step("Creating Schema Management Utilities")
        
        try:
            # Create schema status view
            schema_status_view = """
            CREATE OR REPLACE VIEW schema_status AS
            SELECT 
                table AS table_name,
                count() AS column_count,
                groupArray(name) AS columns
            FROM system.columns 
            WHERE database = currentDatabase()
              AND table IN ('companies', 'contacts', 'tickets', 'time_entries')
            GROUP BY table
            ORDER BY table
            """
            
            client.query(schema_status_view)
            self.print_result("Created schema_status view")
            
            # Create field analysis view
            field_analysis_view = """
            CREATE OR REPLACE VIEW field_analysis AS
            SELECT 
                table AS table_name,
                name AS field_name,
                type AS field_type,
                is_in_partition_key,
                is_in_sorting_key,
                is_in_primary_key
            FROM system.columns 
            WHERE database = currentDatabase()
              AND table IN ('companies', 'contacts', 'tickets', 'time_entries')
            ORDER BY table, name
            """
            
            client.query(field_analysis_view)
            self.print_result("Created field_analysis view")
            
            return True
            
        except Exception as e:
            self.print_result(f"Failed to create schema utilities: {e}", False)
            return False
    
    def run_migration(self):
        """Run schema migration"""
        
        self.print_header(f"CLICKHOUSE SCHEMA MIGRATION")
        
        # Set AWS environment if using AWS secrets
        if self.use_aws_secrets:
            os.environ['AWS_SDK_LOAD_CONFIG'] = '1'
            if not os.environ.get('AWS_PROFILE'):
                os.environ['AWS_PROFILE'] = 'AdministratorAccess-123938354448'
        
        # Get ClickHouse client
        client = self.get_clickhouse_client()
        
        # Load canonical mappings
        self.print_step("Loading Canonical Mappings")
        canonical_mappings = self.load_canonical_mappings()
        
        if not canonical_mappings:
            self.print_result("No canonical mappings found", False)
            return False
        
        # Process each table
        for table_name, mapping in canonical_mappings.items():
            if not mapping:
                self.print_result(f"Skipping {table_name} - no mapping data", False)
                continue
            
            self.print_step(f"Processing {table_name} Table")
            self.results['tables_processed'] += 1
            
            # Extract canonical fields
            canonical_fields = self.extract_canonical_fields(mapping)
            self.print_result(f"Found {len(canonical_fields)} canonical fields for {table_name}")
            
            # Get current schema
            current_schema = self.get_current_table_schema(client, table_name)
            
            # Analyze differences
            analysis = self.analyze_schema_differences(table_name, current_schema, canonical_fields)
            self.results['schema_analysis'][table_name] = analysis
            
            # Generate migration statements
            migration_statements = self.generate_migration_statements(
                table_name, current_schema, canonical_fields
            )
            
            # Execute migration
            migration_success = self.execute_migration_statements(client, table_name, migration_statements)
            
            # Verify migration
            if migration_success:
                verification_success = self.verify_table_schema(client, table_name, canonical_fields)
                if verification_success:
                    self.results['tables_successful'] += 1
                    self.results['migration_results'][table_name] = 'SUCCESS'
                    self.print_result(f"{table_name} migration completed successfully")
                else:
                    self.results['migration_results'][table_name] = 'VERIFICATION_FAILED'
                    self.print_result(f"{table_name} migration verification failed", False)
            else:
                self.results['migration_results'][table_name] = 'MIGRATION_FAILED'
                self.print_result(f"{table_name} migration failed", False)
        
        # Create schema utilities
        self.create_schema_utilities(client)
        
        # Validate data integrity
        self.validate_data_integrity(client)
        
        client.close()
        
        return self.results['tables_successful'] == self.results['tables_processed']
    
    def run_validation(self):
        """Run schema validation"""
        
        self.print_header("CLICKHOUSE SCHEMA VALIDATION")
        
        # Get ClickHouse client
        client = self.get_clickhouse_client()
        
        # Load canonical mappings
        canonical_mappings = self.load_canonical_mappings()
        
        # Validate each table
        for table_name, mapping in canonical_mappings.items():
            if not mapping:
                continue
            
            self.print_step(f"Validating {table_name} Schema")
            
            canonical_fields = self.extract_canonical_fields(mapping)
            current_schema = self.get_current_table_schema(client, table_name)
            
            analysis = self.analyze_schema_differences(table_name, current_schema, canonical_fields)
            self.results['validation_results'][table_name] = analysis
            
            if analysis['is_synchronized']:
                self.print_result(f"{table_name} schema is synchronized")
            else:
                self.print_result(f"{table_name} schema needs {analysis['missing_count']} fields", False)
        
        client.close()
        return True
    
    def run_analysis(self):
        """Run schema analysis"""
        
        self.print_header("CLICKHOUSE SCHEMA ANALYSIS")
        
        # Get ClickHouse client
        client = self.get_clickhouse_client()
        
        # Load canonical mappings
        canonical_mappings = self.load_canonical_mappings()
        
        # Analyze each table
        for table_name, mapping in canonical_mappings.items():
            if not mapping:
                continue
            
            self.print_step(f"Analyzing {table_name} Schema")
            
            canonical_fields = self.extract_canonical_fields(mapping)
            current_schema = self.get_current_table_schema(client, table_name)
            
            analysis = self.analyze_schema_differences(table_name, current_schema, canonical_fields)
            self.results['schema_analysis'][table_name] = analysis
            
            print(f"   📊 Current columns: {analysis['current_columns']}")
            print(f"   📊 Canonical fields: {analysis['canonical_fields']}")
            print(f"   📊 Missing fields: {analysis['missing_count']}")
            print(f"   📊 Extra fields: {analysis['extra_count']}")
            
            if analysis['missing_fields']:
                print(f"   ❌ Missing: {analysis['missing_fields']}")
            if analysis['extra_fields']:
                print(f"   ⚠️  Extra: {analysis['extra_fields']}")
        
        client.close()
        return True
    
    def generate_report(self):
        """Generate comprehensive report"""
        
        self.print_header("SCHEMA MANAGEMENT REPORT")
        
        print(f"🕐 Timestamp: {self.results['timestamp']}")
        print(f"🔧 Mode: {self.mode.upper()}")
        print(f"📊 Tables Processed: {self.results['tables_processed']}")
        
        if self.mode == 'migrate':
            print(f"✅ Tables Successful: {self.results['tables_successful']}")
            
            for table, result in self.results['migration_results'].items():
                icon = "✅" if result == 'SUCCESS' else "❌"
                print(f"   {icon} {table}: {result}")
        
        # Save detailed results
        report_file = f"schema_management_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        print(f"\n📄 Detailed report saved to: {report_file}")
        
        return self.results['tables_successful'] == self.results['tables_processed'] if self.mode == 'migrate' else True

def main():
    parser = argparse.ArgumentParser(description='ClickHouse Schema Management Suite')
    parser.add_argument('--mode', choices=['migrate', 'validate', 'analyze', 'init'], 
                       default='migrate', help='Operation mode (default: migrate)')
    parser.add_argument('--credentials', choices=['aws', 'env'], 
                       default='aws', help='Credential source (default: aws)')
    
    args = parser.parse_args()
    
    use_aws_secrets = args.credentials == 'aws'
    manager = ClickHouseSchemaManager(mode=args.mode, use_aws_secrets=use_aws_secrets)
    
    if args.mode == 'migrate':
        success = manager.run_migration()
    elif args.mode == 'validate':
        success = manager.run_validation()
    elif args.mode == 'analyze':
        success = manager.run_analysis()
    else:
        print(f"Mode '{args.mode}' not yet implemented")
        success = False
    
    # Generate report
    manager.generate_report()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()