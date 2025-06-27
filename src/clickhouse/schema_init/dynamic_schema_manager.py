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

from canonical_schema import CanonicalSchemaManager, CanonicalFieldTypeMapper

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
        """Load canonical mapping for a table using shared schema manager"""
        return CanonicalSchemaManager.load_canonical_mapping(table_name, self.mappings_dir)
    
    def extract_canonical_fields(self, mapping: Dict[str, Any]) -> Set[str]:
        """Extract all canonical field names from mapping using shared schema manager"""
        return CanonicalSchemaManager.extract_canonical_fields(mapping)
    
    def determine_clickhouse_type(self, field_name: str, mapping: Dict[str, Any] = None) -> str:
        """Determine appropriate ClickHouse type for a field using shared field type mapper"""
        return CanonicalFieldTypeMapper.determine_clickhouse_type(field_name, mapping)
    
    def get_scd_type(self, mapping: Dict[str, Any]) -> str:
        """Get SCD type from mapping using shared schema manager"""
        return CanonicalSchemaManager.get_scd_type(mapping)
    
    def get_table_fields(self, table_name: str) -> List[str]:
        """Get list of fields for a table using shared schema manager"""
        mapping = self.load_canonical_mapping(table_name)
        canonical_fields = self.extract_canonical_fields(mapping)
        scd_type = self.get_scd_type(mapping)
        
        # Use shared schema manager for consistent field definitions
        return CanonicalSchemaManager.get_complete_schema(table_name, canonical_fields, scd_type)
    
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
    
    def generate_ordered_table_schema(self, table_name: str) -> str:
        """
        Generate CREATE TABLE statement with fields in mapping file order.
        
        This ensures perfect column ordering alignment between mapping files
        and ClickHouse table schemas for consistency and maintainability.
        
        Args:
            table_name: Name of the canonical table
            
        Returns:
            Complete CREATE TABLE SQL statement
        """
        try:
            # Load canonical mapping
            mapping = self.load_canonical_mapping(table_name)
            
            # Get SCD type
            scd_type = self.get_scd_type(mapping)
            
            # Extract field types in order from mapping
            field_types = mapping.get('field_types', {})
            
            # Get metadata fields that should be appended
            metadata_fields = CanonicalSchemaManager.get_standard_metadata_fields(scd_type)
            
            print(f"Generating ordered schema for {table_name}:")
            print(f"  Business fields: {len(field_types)} (from mapping file order)")
            print(f"  Metadata fields: {len(metadata_fields)} (appended)")
            print(f"  SCD Type: {scd_type}")
            
            # Build field definitions respecting mapping file order
            field_definitions = []
            
            # 1. Add business fields in mapping file order
            for field_name, field_type in field_types.items():
                field_definitions.append(f"    {field_name} {field_type}")
            
            # 2. Add metadata fields (always at end)
            for field_name in metadata_fields:
                field_type = self.determine_clickhouse_type(field_name, mapping)
                field_definitions.append(f"    {field_name} {field_type}")
            
            # Generate CREATE TABLE statement
            create_sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
{',\n'.join(field_definitions)}
)
ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (tenant_id, id, last_updated)
SETTINGS index_granularity = 8192"""
            
            return create_sql
            
        except Exception as e:
            print(f"❌ Failed to generate ordered schema for {table_name}: {e}")
            raise

    def create_table_from_mapping(self, table_name: str, use_ordered_schema: bool = False) -> bool:
        """Create ClickHouse table from canonical mapping"""
        try:
            print(f"Creating table {table_name} from canonical mapping...")
            
            if use_ordered_schema:
                # Use new ordered schema generation
                create_sql = self.generate_ordered_table_schema(table_name)
            else:
                # Legacy schema generation (alphabetical order)
                # Load canonical mapping
                mapping = self.load_canonical_mapping(table_name)
                
                # Extract canonical fields
                canonical_fields = self.extract_canonical_fields(mapping)
                
                # Get SCD type
                scd_type = self.get_scd_type(mapping)
                
                # Use shared schema manager for consistent field definitions
                all_fields = set(CanonicalSchemaManager.get_complete_schema(table_name, canonical_fields, scd_type))
                
                metadata_fields = CanonicalSchemaManager.get_standard_metadata_fields(scd_type)
                print(f"  Fields: {len(all_fields)} total ({len(canonical_fields)} canonical + {len(metadata_fields)} metadata)")
                print(f"  SCD Type: {scd_type}")
                
                # Generate CREATE TABLE statement
                field_definitions = []
                
                # Sort fields for consistent output
                sorted_fields = sorted(all_fields)
                
                for field_name in sorted_fields:
                    field_type = self.determine_clickhouse_type(field_name, mapping)
                    field_definitions.append(f"    {field_name} {field_type}")
                
                create_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
{',\n'.join(field_definitions)}
)
ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (tenant_id, id, last_updated)
SETTINGS index_granularity = 8192
"""
            
            # Execute CREATE TABLE
            self.client.query(create_sql)
            print(f"✅ Successfully created table {table_name}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to create table {table_name}: {e}")
            return False

    def drop_table(self, table_name: str) -> bool:
        """
        Drop ClickHouse table.
        
        Args:
            table_name: Name of the table to drop
            
        Returns:
            True if successful, False otherwise
        """
        try:
            print(f"Dropping table {table_name}...")
            drop_sql = f"DROP TABLE IF EXISTS {table_name}"
            self.client.query(drop_sql)
            print(f"✅ Successfully dropped table {table_name}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to drop table {table_name}: {e}")
            return False

    def recreate_table_with_ordered_schema(self, table_name: str) -> bool:
        """
        Drop and recreate table with perfect column ordering from mapping file.
        
        Args:
            table_name: Name of the table to recreate
            
        Returns:
            True if successful, False otherwise
        """
        try:
            print(f"=== RECREATING TABLE {table_name.upper()} WITH ORDERED SCHEMA ===")
            
            # Step 1: Drop existing table
            if not self.drop_table(table_name):
                return False
                
            # Step 2: Create table with ordered schema
            if not self.create_table_from_mapping(table_name, use_ordered_schema=True):
                return False
                
            print(f"✅ Successfully recreated {table_name} with perfect column ordering")
            return True
            
        except Exception as e:
            print(f"❌ Failed to recreate table {table_name}: {e}")
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
            mapping = self.load_canonical_mapping(table_name)
            for field_name in sorted(missing_fields):
                field_type = self.determine_clickhouse_type(field_name, mapping)
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