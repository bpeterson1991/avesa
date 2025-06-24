#!/usr/bin/env python3
"""
Dynamic ClickHouse Schema Manager

Replaces static SQL files with dynamic schema generation from canonical mappings.
This ensures ClickHouse tables always match the canonical data structure.
"""

import json
import os
import sys
from typing import Dict, List, Any, Set
from datetime import datetime
import clickhouse_connect
from clickhouse_connect.driver.client import Client

# Add shared modules to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))

class DynamicClickHouseSchemaManager:
    """Manage ClickHouse schemas dynamically from canonical mappings"""
    
    def __init__(self, clickhouse_client: Client, mappings_dir: str = None):
        self.client = clickhouse_client
        self.canonical_tables = ['companies', 'contacts', 'tickets', 'time_entries']
        
        # Determine mappings directory
        if mappings_dir:
            self.mappings_dir = mappings_dir
        else:
            # Default to project mappings directory
            current_dir = os.path.dirname(__file__)
            project_root = os.path.join(current_dir, '..', '..', '..')
            self.mappings_dir = os.path.join(project_root, 'mappings', 'canonical')
    
    def load_canonical_mapping(self, table_name: str) -> Dict[str, Any]:
        """Load canonical mapping for a table"""
        mapping_file = os.path.join(self.mappings_dir, f"{table_name}.json")
        
        if not os.path.exists(mapping_file):
            raise FileNotFoundError(f"Canonical mapping not found: {mapping_file}")
            
        with open(mapping_file, 'r') as f:
            return json.load(f)
    
    def extract_canonical_fields(self, mapping: Dict[str, Any]) -> Set[str]:
        """Extract all canonical field names from mapping"""
        fields = set()
        
        # Skip scd_type key
        for service_name, service_mapping in mapping.items():
            if service_name == 'scd_type':
                continue
                
            if isinstance(service_mapping, dict):
                for endpoint_path, field_mappings in service_mapping.items():
                    if isinstance(field_mappings, dict):
                        # The keys are the canonical field names
                        fields.update(field_mappings.keys())
        
        return fields
    
    def determine_clickhouse_type(self, field_name: str) -> str:
        """Determine appropriate ClickHouse type for a field"""
        
        # Required fields (non-nullable)
        required_fields = {
            'tenant_id': 'String',
            'id': 'String',
            'company_name': 'String',
            'ingestion_timestamp': 'DateTime DEFAULT now()',
            'record_hash': 'String',
            'source_table': 'String',
            'updated_date': 'DateTime DEFAULT now()'
        }
        
        # Check required fields first
        if field_name in required_fields:
            return required_fields[field_name]
        
        # Type mappings for specific fields
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
            'effective_start_date': 'DateTime DEFAULT now()',
            'effective_end_date': 'Nullable(DateTime)',
            
            # Boolean fields
            'lead_flag': 'Nullable(Bool)',
            'unsubscribe_flag': 'Nullable(Bool)',
            'married_flag': 'Nullable(Bool)',
            'children_flag': 'Nullable(Bool)',
            'disable_portal_login_flag': 'Nullable(Bool)',
            'inactive_flag': 'Nullable(Bool)',
            'approved': 'Nullable(Bool)',
            'is_current': 'Bool DEFAULT true',
            
            # Numeric fields
            'annual_revenue': 'Nullable(Float64)',
            'number_of_employees': 'Nullable(UInt32)',
            'budget_hours': 'Nullable(Float64)',
            'actual_hours': 'Nullable(Float64)',
            'hours_deduct': 'Nullable(Float64)',
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
    
    def get_scd_type(self, mapping: Dict[str, Any]) -> str:
        """Get SCD type from mapping"""
        return mapping.get('scd_type', 'type_1')
    
    def get_table_fields(self, table_name: str) -> List[str]:
        """Get list of fields for a table"""
        mapping = self.load_canonical_mapping(table_name)
        canonical_fields = self.extract_canonical_fields(mapping)
        scd_type = self.get_scd_type(mapping)
        
        # Required metadata fields
        required_fields = {
            'tenant_id', 'id', 'ingestion_timestamp', 'record_hash', 
            'source_table', 'updated_date'
        }
        
        # Add SCD Type 2 fields if needed
        if scd_type == 'type_2':
            required_fields.update(['effective_start_date', 'effective_end_date', 'is_current'])
        
        # Combine and sort
        all_fields = canonical_fields.union(required_fields)
        return sorted(all_fields)
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in ClickHouse"""
        try:
            query = f"EXISTS TABLE {table_name}"
            result = self.client.query(query)
            return bool(result.result_rows[0][0])
        except Exception:
            return False
    
    def get_existing_table_columns(self, table_name: str) -> Set[str]:
        """Get existing table columns"""
        try:
            query = f"DESCRIBE TABLE {table_name}"
            result = self.client.query(query)
            return {row[0] for row in result.result_rows}
        except Exception:
            return set()
    
    def create_table_from_mapping(self, table_name: str) -> bool:
        """Create ClickHouse table from canonical mapping"""
        try:
            print(f"Creating table {table_name} from canonical mapping...")
            
            # Load canonical mapping
            mapping = self.load_canonical_mapping(table_name)
            
            # Extract canonical fields
            canonical_fields = self.extract_canonical_fields(mapping)
            
            # Get SCD type
            scd_type = self.get_scd_type(mapping)
            
            # Required metadata fields
            required_fields = {
                'tenant_id', 'id', 'ingestion_timestamp', 'record_hash', 
                'source_table', 'updated_date'
            }
            
            # Add SCD Type 2 fields if needed
            if scd_type == 'type_2':
                required_fields.update(['effective_start_date', 'effective_end_date', 'is_current'])
            
            # Combine canonical and required fields
            all_fields = canonical_fields.union(required_fields)
            
            print(f"  Fields: {len(all_fields)} total ({len(canonical_fields)} canonical + {len(required_fields)} metadata)")
            print(f"  SCD Type: {scd_type}")
            
            # Generate CREATE TABLE statement
            field_definitions = []
            
            # Sort fields for consistent output
            sorted_fields = sorted(all_fields)
            
            for field_name in sorted_fields:
                field_type = self.determine_clickhouse_type(field_name)
                field_definitions.append(f"    {field_name} {field_type}")
            
            create_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
{',\n'.join(field_definitions)}
)
ENGINE = MergeTree()
ORDER BY (tenant_id, id, {'effective_start_date' if scd_type == 'type_2' else 'updated_date'})
SETTINGS index_granularity = 8192
"""
            
            # Execute CREATE TABLE
            self.client.query(create_sql)
            print(f"✅ Successfully created table {table_name}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to create table {table_name}: {e}")
            return False
    
    def update_table_schema(self, table_name: str) -> bool:
        """Update existing table schema to match canonical mapping"""
        try:
            print(f"Updating table {table_name} schema...")
            
            # Get expected fields from mapping
            expected_fields = set(self.get_table_fields(table_name))
            
            # Get existing fields
            existing_fields = self.get_existing_table_columns(table_name)
            
            # Find missing fields
            missing_fields = expected_fields - existing_fields
            
            if not missing_fields:
                print(f"  Table {table_name} schema is up to date")
                return True
            
            print(f"  Adding {len(missing_fields)} missing fields: {sorted(missing_fields)}")
            
            # Add missing columns
            for field_name in sorted(missing_fields):
                field_type = self.determine_clickhouse_type(field_name)
                alter_sql = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {field_name} {field_type}"
                
                try:
                    self.client.query(alter_sql)
                    print(f"    ✅ Added column: {field_name}")
                except Exception as e:
                    print(f"    ❌ Failed to add column {field_name}: {e}")
                    return False
            
            print(f"✅ Successfully updated table {table_name}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to update table {table_name}: {e}")
            return False
    
    def initialize_all_tables(self) -> bool:
        """Initialize all canonical tables"""
        print("=== DYNAMIC CLICKHOUSE SCHEMA INITIALIZATION ===")
        
        success_count = 0
        
        for table_name in self.canonical_tables:
            try:
                if self.table_exists(table_name):
                    # Update existing table
                    if self.update_table_schema(table_name):
                        success_count += 1
                else:
                    # Create new table
                    if self.create_table_from_mapping(table_name):
                        success_count += 1
                        
            except Exception as e:
                print(f"❌ Error processing table {table_name}: {e}")
        
        print(f"\n=== SUMMARY ===")
        print(f"Tables processed: {len(self.canonical_tables)}")
        print(f"Tables successful: {success_count}")
        print(f"Success rate: {success_count}/{len(self.canonical_tables)}")
        
        return success_count == len(self.canonical_tables)

def get_clickhouse_client() -> Client:
    """Get ClickHouse client from environment or AWS Secrets Manager"""
    import boto3
    
    try:
        # Try AWS Secrets Manager first
        secrets_client = boto3.client('secretsmanager', region_name='us-east-2')
        secret_name = os.environ.get('CLICKHOUSE_SECRET_NAME', 'clickhouse-connection-dev')
        
        response = secrets_client.get_secret_value(SecretId=secret_name)
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
        
        print(f"✅ Connected to ClickHouse via AWS Secrets Manager")
        return client
        
    except Exception as e:
        print(f"❌ Failed to connect to ClickHouse: {e}")
        raise

def main():
    """Main function for testing"""
    try:
        # Get ClickHouse client
        client = get_clickhouse_client()
        
        # Initialize schema manager
        manager = DynamicClickHouseSchemaManager(client)
        
        # Initialize all tables
        success = manager.initialize_all_tables()
        
        if success:
            print("\n🎉 All tables initialized successfully!")
        else:
            print("\n⚠️ Some tables failed to initialize")
            
    except Exception as e:
        print(f"❌ Schema initialization failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()