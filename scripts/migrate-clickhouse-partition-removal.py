#!/usr/bin/env python3
"""
ClickHouse Partition Removal Migration Script

This script safely migrates ClickHouse tables from partitioned to non-partitioned
with correct SCD Type configurations using a blue-green deployment approach.

Migration Strategy:
- Current: Partitioned tables with incorrect SCD configurations
- Target: Non-partitioned tables with correct SCD configurations
  - Companies: SCD Type 1 (no SCD fields)
  - Contacts: SCD Type 1 (no SCD fields)  
  - Tickets: SCD Type 2 (with SCD fields: effective_date, expiration_date, is_current)
  - Time Entries: SCD Type 1 (no SCD fields)

Features:
- Blue-green deployment approach for zero-downtime migration
- Comprehensive data validation and integrity checks
- Rollback capability if migration fails
- Progress monitoring and detailed logging
- Dry-run mode for testing
- Backup creation and management
"""

import json
import os
import sys
import time
import logging
import argparse
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from contextlib import contextmanager

try:
    import clickhouse_connect
    import boto3
except ImportError as e:
    print(f"❌ Required packages not installed: {e}")
    print("Please install: pip install clickhouse-connect boto3")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class MigrationConfig:
    """Configuration for the migration process."""
    secret_name: str = 'clickhouse-connection-dev'
    region_name: str = 'us-east-2'
    dry_run: bool = False
    backup_suffix: str = '_backup'
    temp_suffix: str = '_new'
    batch_size: int = 10000
    validation_sample_size: int = 1000
    max_retries: int = 3
    retry_delay: int = 5


@dataclass
class TableConfig:
    """Configuration for individual table migration."""
    name: str
    scd_type: str
    has_partitions: bool
    expected_scd_fields: List[str]
    
    @property
    def backup_name(self) -> str:
        return f"{self.name}_backup"
    
    @property
    def temp_name(self) -> str:
        return f"{self.name}_new"


class ClickHouseMigrationError(Exception):
    """Custom exception for migration errors."""
    pass


class ClickHouseMigrator:
    """
    Main migration class for ClickHouse partition removal.
    
    Handles the complete migration process from partitioned to non-partitioned tables
    with proper SCD Type configurations.
    """
    
    def __init__(self, config: MigrationConfig):
        """Initialize the migrator with configuration."""
        self.config = config
        self.client = None
        self.migration_start_time = None
        self.migration_stats = {
            'tables_migrated': 0,
            'total_records_migrated': 0,
            'errors': [],
            'warnings': []
        }
        
        # Define table configurations based on canonical mappings
        self.tables = [
            TableConfig(
                name='companies',
                scd_type='type_1',
                has_partitions=True,
                expected_scd_fields=[]
            ),
            TableConfig(
                name='contacts',
                scd_type='type_1',
                has_partitions=True,
                expected_scd_fields=[]
            ),
            TableConfig(
                name='tickets',
                scd_type='type_2',
                has_partitions=True,
                expected_scd_fields=['effective_start_date', 'effective_end_date', 'is_current']
            ),
            TableConfig(
                name='time_entries',
                scd_type='type_1',
                has_partitions=True,
                expected_scd_fields=[]
            )
        ]
    
    def _get_clickhouse_credentials(self) -> Dict[str, Any]:
        """Retrieve ClickHouse credentials from AWS Secrets Manager."""
        try:
            logger.info(f"🔐 Retrieving ClickHouse credentials from secret: {self.config.secret_name}")
            
            secrets_client = boto3.client('secretsmanager', region_name=self.config.region_name)
            response = secrets_client.get_secret_value(SecretId=self.config.secret_name)
            credentials = json.loads(response['SecretString'])
            
            # Validate required fields
            required_fields = ['host', 'username', 'password']
            missing_fields = [field for field in required_fields if field not in credentials]
            
            if missing_fields:
                raise ClickHouseMigrationError(
                    f"Missing required credential fields: {', '.join(missing_fields)}"
                )
            
            logger.info(f"✅ Successfully retrieved credentials for {credentials['host']}")
            return credentials
            
        except Exception as e:
            raise ClickHouseMigrationError(f"Failed to retrieve ClickHouse credentials: {e}")
    
    def _create_connection(self) -> clickhouse_connect.driver.Client:
        """Create ClickHouse connection."""
        try:
            credentials = self._get_clickhouse_credentials()
            
            connection_params = {
                'host': credentials['host'],
                'port': credentials.get('port', 8443),
                'username': credentials['username'],
                'password': credentials['password'],
                'database': credentials.get('database', 'default'),
                'secure': True,
                'verify': False,
                'connect_timeout': 30,
                'send_receive_timeout': 600,  # Longer timeout for migration operations
                'compress': True
            }
            
            logger.info(f"🔗 Connecting to ClickHouse: {credentials['host']}:{connection_params['port']}")
            client = clickhouse_connect.get_client(**connection_params)
            
            # Test connection
            client.ping()
            logger.info("✅ ClickHouse connection established successfully")
            
            return client
            
        except Exception as e:
            raise ClickHouseMigrationError(f"Failed to connect to ClickHouse: {e}")
    
    @contextmanager
    def get_client(self):
        """Context manager for ClickHouse client."""
        if self.client is None:
            self.client = self._create_connection()
        
        try:
            yield self.client
        finally:
            # Keep connection open for the duration of migration
            pass
    
    def execute_query(self, query: str, description: str = "") -> Any:
        """Execute a ClickHouse query with error handling."""
        try:
            if description:
                logger.debug(f"🔍 {description}")
            
            logger.debug(f"Executing query: {query[:100]}...")
            
            if self.config.dry_run and any(keyword in query.upper() for keyword in ['DROP', 'CREATE', 'INSERT', 'DELETE', 'ALTER']):
                logger.info(f"🔄 DRY RUN - Would execute: {query[:100]}...")
                return None
            
            with self.get_client() as client:
                result = client.query(query)
                return result
                
        except Exception as e:
            error_msg = f"Query failed: {e}\nQuery: {query}"
            logger.error(error_msg)
            raise ClickHouseMigrationError(error_msg)
    
    def execute_command(self, command: str, description: str = "") -> None:
        """Execute a ClickHouse command with error handling."""
        try:
            if description:
                logger.debug(f"🔍 {description}")
            
            logger.debug(f"Executing command: {command[:100]}...")
            
            if self.config.dry_run and any(keyword in command.upper() for keyword in ['DROP', 'CREATE', 'INSERT', 'DELETE', 'ALTER']):
                logger.info(f"🔄 DRY RUN - Would execute: {command[:100]}...")
                return
            
            with self.get_client() as client:
                client.command(command)
                
        except Exception as e:
            error_msg = f"Command failed: {e}\nCommand: {command}"
            logger.error(error_msg)
            raise ClickHouseMigrationError(error_msg)
    
    def validate_current_state(self) -> Dict[str, Any]:
        """Validate the current state of ClickHouse tables."""
        logger.info("🔍 Validating current ClickHouse table state...")
        
        validation_results = {
            'tables_found': [],
            'tables_missing': [],
            'partition_info': {},
            'record_counts': {},
            'schema_info': {},
            'total_records': 0
        }
        
        for table_config in self.tables:
            table_name = table_config.name
            
            try:
                # Check if table exists
                exists_query = f"EXISTS TABLE {table_name}"
                result = self.execute_query(exists_query, f"Checking if table {table_name} exists")
                
                if result and result.result_rows and result.result_rows[0][0]:
                    validation_results['tables_found'].append(table_name)
                    logger.info(f"✅ Table {table_name} exists")
                    
                    # Get record count
                    count_query = f"SELECT COUNT(*) FROM {table_name}"
                    count_result = self.execute_query(count_query, f"Getting record count for {table_name}")
                    record_count = count_result.result_rows[0][0] if count_result.result_rows else 0
                    validation_results['record_counts'][table_name] = record_count
                    validation_results['total_records'] += record_count
                    
                    logger.info(f"📊 Table {table_name}: {record_count:,} records")
                    
                    # Get table schema
                    schema_query = f"DESCRIBE TABLE {table_name}"
                    schema_result = self.execute_query(schema_query, f"Getting schema for {table_name}")
                    validation_results['schema_info'][table_name] = schema_result.result_rows if schema_result else []
                    
                    # Check for partition information
                    partition_query = f"""
                        SELECT partition, rows 
                        FROM system.parts 
                        WHERE database = currentDatabase() AND table = '{table_name}' AND active = 1
                        ORDER BY partition
                    """
                    partition_result = self.execute_query(partition_query, f"Getting partition info for {table_name}")
                    
                    if partition_result and partition_result.result_rows:
                        validation_results['partition_info'][table_name] = partition_result.result_rows
                        logger.info(f"📂 Table {table_name} has {len(partition_result.result_rows)} active partitions")
                    else:
                        validation_results['partition_info'][table_name] = []
                        logger.info(f"📂 Table {table_name} has no partitions (already non-partitioned)")
                    
                else:
                    validation_results['tables_missing'].append(table_name)
                    logger.warning(f"⚠️  Table {table_name} does not exist")
                    
            except Exception as e:
                error_msg = f"Error validating table {table_name}: {e}"
                logger.error(error_msg)
                validation_results['tables_missing'].append(table_name)
                self.migration_stats['errors'].append(error_msg)
        
        # Summary
        logger.info(f"📊 Validation Summary:")
        logger.info(f"   ✅ Tables found: {len(validation_results['tables_found'])}")
        logger.info(f"   ❌ Tables missing: {len(validation_results['tables_missing'])}")
        logger.info(f"   📊 Total records: {validation_results['total_records']:,}")
        
        if validation_results['tables_missing']:
            logger.warning(f"⚠️  Missing tables: {', '.join(validation_results['tables_missing'])}")
        
        return validation_results
    
    def create_backup_tables(self, validation_results: Dict[str, Any]) -> Dict[str, bool]:
        """Create backup tables with current data."""
        logger.info("💾 Creating backup tables...")
        
        backup_results = {}
        
        for table_config in self.tables:
            table_name = table_config.name
            backup_name = table_config.backup_name
            
            if table_name not in validation_results['tables_found']:
                logger.warning(f"⚠️  Skipping backup for missing table: {table_name}")
                backup_results[table_name] = False
                continue
            
            try:
                logger.info(f"💾 Creating backup table: {backup_name}")
                
                # Drop existing backup if it exists
                drop_backup_query = f"DROP TABLE IF EXISTS {backup_name}"
                self.execute_command(drop_backup_query, f"Dropping existing backup table {backup_name}")
                
                # Create backup table with same structure and data
                backup_query = f"CREATE TABLE {backup_name} AS {table_name}"
                self.execute_command(backup_query, f"Creating backup table {backup_name}")
                
                # Verify backup
                if not self.config.dry_run:
                    original_count = validation_results['record_counts'].get(table_name, 0)
                    backup_count_query = f"SELECT COUNT(*) FROM {backup_name}"
                    backup_count_result = self.execute_query(backup_count_query)
                    backup_count = backup_count_result.result_rows[0][0] if backup_count_result.result_rows else 0
                    
                    if backup_count == original_count:
                        logger.info(f"✅ Backup verified: {backup_name} ({backup_count:,} records)")
                        backup_results[table_name] = True
                    else:
                        error_msg = f"Backup verification failed: {backup_name} has {backup_count} records, expected {original_count}"
                        logger.error(error_msg)
                        backup_results[table_name] = False
                        self.migration_stats['errors'].append(error_msg)
                else:
                    backup_results[table_name] = True
                    
            except Exception as e:
                error_msg = f"Failed to create backup for {table_name}: {e}"
                logger.error(error_msg)
                backup_results[table_name] = False
                self.migration_stats['errors'].append(error_msg)
        
        successful_backups = sum(1 for success in backup_results.values() if success)
        logger.info(f"💾 Backup Summary: {successful_backups}/{len(self.tables)} tables backed up successfully")
        
        return backup_results
    
    def get_new_table_schema(self, table_config: TableConfig) -> str:
        """Get the new non-partitioned schema for a table."""
        
        # Read the corrected schema file
        schema_file_path = 'src/clickhouse/schemas/shared_tables_no_partition_corrected.sql'
        
        try:
            with open(schema_file_path, 'r') as f:
                schema_content = f.read()
        except FileNotFoundError:
            raise ClickHouseMigrationError(f"Schema file not found: {schema_file_path}")
        
        # Extract the specific table schema
        table_name = table_config.name.upper()
        # Handle special case for time_entries
        if table_name == 'TIME_ENTRIES':
            start_marker = "-- TIME ENTRIES TABLE"
        else:
            start_marker = f"-- {table_name} TABLE"
        
        lines = schema_content.split('\n')
        table_lines = []
        in_table_section = False
        found_create_table = False
        
        for line in lines:
            if start_marker in line.upper():
                in_table_section = True
                continue
            elif in_table_section and line.strip().startswith('-- ============='):
                # Skip the separator line, continue looking for CREATE TABLE
                continue
            elif in_table_section and f"CREATE TABLE IF NOT EXISTS {table_config.name}" in line:
                # Found the actual table definition
                found_create_table = True
                table_lines.append(line)
            elif found_create_table and line.strip():
                table_lines.append(line)
                # Stop when we hit the end of the table definition (after SETTINGS clause)
                if line.strip().endswith(';') and ('SETTINGS' in line or 'ENGINE' in line):
                    break
                # Also stop when we hit the next section or another CREATE statement
                elif (line.strip().startswith('-- ') and ('TABLE' in line.upper() or 'INDEX' in line.upper())) or \
                     (line.strip().startswith('CREATE TABLE') and table_config.name not in line):
                    table_lines.pop()  # Remove the last line as it's not part of this table
                    break
            elif found_create_table and not line.strip():
                # Empty line, continue
                table_lines.append(line)
        
        if not table_lines:
            raise ClickHouseMigrationError(f"Could not find schema for table {table_config.name}")
        
        # Replace table name with temp name
        table_schema = '\n'.join(table_lines)
        table_schema = table_schema.replace(f"CREATE TABLE IF NOT EXISTS {table_config.name}", 
                                          f"CREATE TABLE IF NOT EXISTS {table_config.temp_name}")
        
        return table_schema
    
    def create_new_tables(self) -> Dict[str, bool]:
        """Create new non-partitioned tables with correct SCD configurations."""
        logger.info("🏗️  Creating new non-partitioned tables...")
        
        creation_results = {}
        
        for table_config in self.tables:
            table_name = table_config.name
            temp_name = table_config.temp_name
            
            try:
                logger.info(f"🏗️  Creating new table: {temp_name}")
                
                # Drop existing temp table if it exists
                drop_temp_query = f"DROP TABLE IF EXISTS {temp_name}"
                self.execute_command(drop_temp_query, f"Dropping existing temp table {temp_name}")
                
                # Get new table schema
                table_schema = self.get_new_table_schema(table_config)
                
                # Create new table
                self.execute_command(table_schema, f"Creating new table {temp_name}")
                
                # Verify table creation
                if not self.config.dry_run:
                    exists_query = f"EXISTS TABLE {temp_name}"
                    exists_result = self.execute_query(exists_query)
                    
                    if exists_result and exists_result.result_rows and exists_result.result_rows[0][0]:
                        logger.info(f"✅ New table created successfully: {temp_name}")
                        creation_results[table_name] = True
                    else:
                        error_msg = f"Failed to verify creation of table {temp_name}"
                        logger.error(error_msg)
                        creation_results[table_name] = False
                        self.migration_stats['errors'].append(error_msg)
                else:
                    creation_results[table_name] = True
                    
            except Exception as e:
                error_msg = f"Failed to create new table for {table_name}: {e}"
                logger.error(error_msg)
                creation_results[table_name] = False
                self.migration_stats['errors'].append(error_msg)
        
        successful_creations = sum(1 for success in creation_results.values() if success)
        logger.info(f"🏗️  Creation Summary: {successful_creations}/{len(self.tables)} new tables created successfully")
        
        return creation_results
    
    def migrate_table_data(self, table_config: TableConfig, validation_results: Dict[str, Any]) -> bool:
        """Migrate data from old table to new table with SCD corrections."""
        table_name = table_config.name
        temp_name = table_config.temp_name
        
        logger.info(f"📊 Migrating data: {table_name} -> {temp_name}")
        
        if table_name not in validation_results['tables_found']:
            logger.warning(f"⚠️  Skipping data migration for missing table: {table_name}")
            return False
        
        try:
            # Get source record count
            source_count = validation_results['record_counts'].get(table_name, 0)
            
            if source_count == 0:
                logger.info(f"📊 No data to migrate for {table_name}")
                return True
            
            logger.info(f"📊 Migrating {source_count:,} records from {table_name}")
            
            # Build migration query based on SCD type
            if table_config.scd_type == 'type_2':
                # For SCD Type 2 tables (tickets), add SCD fields if they don't exist
                migration_query = f"""
                    INSERT INTO {temp_name}
                    SELECT
                        *,
                        CASE
                            WHEN hasColumn('{table_name}', 'effective_start_date') THEN effective_start_date
                            WHEN hasColumn('{table_name}', 'effective_date') THEN effective_date
                            ELSE now()
                        END as effective_start_date,
                        CASE
                            WHEN hasColumn('{table_name}', 'effective_end_date') THEN effective_end_date
                            WHEN hasColumn('{table_name}', 'expiration_date') THEN expiration_date
                            ELSE NULL
                        END as effective_end_date,
                        CASE
                            WHEN hasColumn('{table_name}', 'is_current') THEN is_current
                            ELSE true
                        END as is_current
                    FROM {table_name}
                """
            else:
                # For SCD Type 1 tables, direct copy (remove SCD fields if they exist)
                # Get column list excluding SCD fields
                schema_info = validation_results['schema_info'].get(table_name, [])
                columns = [row[0] for row in schema_info if row[0] not in ['effective_start_date', 'effective_end_date', 'effective_date', 'expiration_date', 'is_current']]
                
                if columns:
                    column_list = ', '.join(columns)
                    migration_query = f"""
                        INSERT INTO {temp_name} ({column_list})
                        SELECT {column_list}
                        FROM {table_name}
                    """
                else:
                    # Fallback to SELECT *
                    migration_query = f"""
                        INSERT INTO {temp_name}
                        SELECT *
                        FROM {table_name}
                    """
            
            # Execute migration in batches for large tables
            if source_count > self.config.batch_size:
                logger.info(f"📊 Large table detected, migrating in batches of {self.config.batch_size:,}")
                
                # Use LIMIT and OFFSET for batching
                total_migrated = 0
                offset = 0
                
                while offset < source_count:
                    batch_query = f"{migration_query} LIMIT {self.config.batch_size} OFFSET {offset}"
                    
                    if not self.config.dry_run:
                        self.execute_command(batch_query, f"Migrating batch {offset//self.config.batch_size + 1}")
                        total_migrated += min(self.config.batch_size, source_count - offset)
                        logger.info(f"📊 Migrated {total_migrated:,}/{source_count:,} records")
                    
                    offset += self.config.batch_size
            else:
                # Small table, migrate all at once
                if not self.config.dry_run:
                    self.execute_command(migration_query, f"Migrating all data for {table_name}")
            
            # Verify migration
            if not self.config.dry_run:
                target_count_query = f"SELECT COUNT(*) FROM {temp_name}"
                target_count_result = self.execute_query(target_count_query)
                target_count = target_count_result.result_rows[0][0] if target_count_result.result_rows else 0
                
                if target_count == source_count:
                    logger.info(f"✅ Data migration verified: {temp_name} ({target_count:,} records)")
                    self.migration_stats['total_records_migrated'] += target_count
                    return True
                else:
                    error_msg = f"Data migration verification failed: {temp_name} has {target_count} records, expected {source_count}"
                    logger.error(error_msg)
                    self.migration_stats['errors'].append(error_msg)
                    return False
            else:
                logger.info(f"🔄 DRY RUN - Would migrate {source_count:,} records")
                return True
                
        except Exception as e:
            error_msg = f"Failed to migrate data for {table_name}: {e}"
            logger.error(error_msg)
            self.migration_stats['errors'].append(error_msg)
            return False
    
    def validate_migrated_data(self, table_config: TableConfig, validation_results: Dict[str, Any]) -> bool:
        """Validate migrated data integrity."""
        table_name = table_config.name
        temp_name = table_config.temp_name
        
        logger.info(f"🔍 Validating migrated data for {table_name}")
        
        if self.config.dry_run:
            logger.info(f"🔄 DRY RUN - Would validate data for {temp_name}")
            return True
        
        try:
            # Check record counts
            source_count = validation_results['record_counts'].get(table_name, 0)
            
            target_count_query = f"SELECT COUNT(*) FROM {temp_name}"
            target_count_result = self.execute_query(target_count_query)
            target_count = target_count_result.result_rows[0][0] if target_count_result.result_rows else 0
            
            if target_count != source_count:
                error_msg = f"Record count mismatch: {temp_name} has {target_count}, expected {source_count}"
                logger.error(error_msg)
                return False
            
            # Sample data validation
            if source_count > 0:
                sample_size = min(self.config.validation_sample_size, source_count)
                
                # Compare sample records
                sample_query = f"""
                    SELECT tenant_id, id, last_updated 
                    FROM {table_name} 
                    ORDER BY tenant_id, id 
                    LIMIT {sample_size}
                """
                source_sample = self.execute_query(sample_query)
                
                target_sample_query = f"""
                    SELECT tenant_id, id, last_updated 
                    FROM {temp_name} 
                    ORDER BY tenant_id, id 
                    LIMIT {sample_size}
                """
                target_sample = self.execute_query(target_sample_query)
                
                if source_sample.result_rows != target_sample.result_rows:
                    error_msg = f"Sample data mismatch detected for {table_name}"
                    logger.error(error_msg)
                    return False
            
            # Validate SCD Type 2 fields for tickets
            if table_config.scd_type == 'type_2':
                scd_validation_query = f"""
                    SELECT
                        COUNT(*) as total_records,
                        COUNT(effective_start_date) as has_effective_start_date,
                        SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) as current_records
                    FROM {temp_name}
                """
                scd_result = self.execute_query(scd_validation_query)
                
                if scd_result.result_rows:
                    total, has_effective, current = scd_result.result_rows[0]
                    
                    if has_effective != total:
                        error_msg = f"SCD validation failed: {temp_name} missing effective_date values"
                        logger.error(error_msg)
                        return False
                    
                    if current == 0 and total > 0:
                        error_msg = f"SCD validation failed: {temp_name} has no current records"
                        logger.error(error_msg)
                        return False
                    
                    logger.info(f"✅ SCD Type 2 validation passed: {current}/{total} current records")
            
            logger.info(f"✅ Data validation passed for {table_name}")
            return True
            
        except Exception as e:
            error_msg = f"Data validation failed for {table_name}: {e}"
            logger.error(error_msg)
            self.migration_stats['errors'].append(error_msg)
            return False
    
    def swap_tables(self) -> Dict[str, bool]:
        """Swap old tables with new tables (blue-green deployment)."""
        logger.info("🔄 Swapping tables (blue-green deployment)...")
        
        swap_results = {}
        
        for table_config in self.tables:
            table_name = table_config.name
            temp_name = table_config.temp_name
            old_name = f"{table_name}_old"
            
            try:
                logger.info(f"🔄 Swapping {table_name} with {temp_name}")
                
                # Step 1: Rename original table to _old
                rename_old_query = f"RENAME TABLE {table_name} TO {old_name}"
                self.execute_command(rename_old_query, f"Renaming {table_name} to {old_name}")
                
                # Step 2: Rename new table to original name
                rename_new_query = f"RENAME TABLE {temp_name} TO {table_name}"
                self.execute_command(rename_new_query, f"Renaming {temp_name} to {table_name}")
                
                logger.info(f"✅ Table swap completed: {table_name}")
                swap_results[table_name] = True
                
            except Exception as e:
                error_msg = f"Failed to swap tables for {table_name}: {e}"
                logger.error(error_msg)
                swap_results[table_name] = False
                self.migration_stats['errors'].append(error_msg)
                
                # Attempt rollback for this table
                try:
                    rollback_query = f"RENAME TABLE {old_name} TO {table_name}"
                    self.execute_command(rollback_query, f"Rolling back {table_name}")
                    logger.warning(f"⚠️  Rolled back table swap for {table_name}")
                except Exception as rollback_error:
                    logger.error(f"Failed to rollback table swap for {table_name}: {rollback_error}")
        
        successful_swaps = sum(1 for success in swap_results.values() if success)
        logger.info(f"🔄 Swap Summary: {successful_swaps}/{len(self.tables)} tables swapped successfully")
        
        return swap_results
    
    def recreate_materialized_views(self) -> bool:
        """Recreate materialized views without partitioning."""
        logger.info("🔍 Recreating materialized views...")
        
        try:
            # Drop existing materialized views
            views_to_recreate = ['company_summary', 'ticket_metrics']
            
            for view_name in views_to_recreate:
                drop_view_query = f"DROP VIEW IF EXISTS {view_name}"
                self.execute_command(drop_view_query, f"Dropping materialized view {view_name}")
            
            # Recreate views with corrected schema
            schema_file_path = 'src/clickhouse/schemas/shared_tables_no_partition_corrected.sql'
            
            try:
                with open(schema_file_path, 'r') as f:
                    schema_content = f.read()
            except FileNotFoundError:
                logger.warning(f"⚠️  Schema file not found: {schema_file_path}")
                return False
            
            # Extract materialized view definitions
            lines = schema_content.split('\n')
            view_lines = []
            in_view_section = False
            
            for line in lines:
                if 'CREATE MATERIALIZED VIEW' in line.upper():
                    in_view_section = True
                    view_lines.append(line)
                elif in_view_section and line.strip() and not line.startswith('--'):
                    view_lines.append(line)
                    if ';' in line:
                        # End of view definition
                        view_sql = '\n'.join(view_lines)
                        self.execute_command(view_sql, f"Creating materialized view")
                        view_lines = []
                        in_view_section = False
            
            logger.info("✅ Materialized views recreated successfully")
            return True
            
        except Exception as e:
            error_msg = f"Failed to recreate materialized views: {e}"
            logger.error(error_msg)
            self.migration_stats['errors'].append(error_msg)
            return False
    
    def cleanup_old_tables(self) -> bool:
        """Clean up old tables and temporary objects."""
        logger.info("🧹 Cleaning up old tables and temporary objects...")
        
        try:
            # Clean up _old tables
            for table_config in self.tables:
                old_name = f"{table_config.name}_old"
                
                try:
                    drop_old_query = f"DROP TABLE IF EXISTS {old_name}"
                    self.execute_command(drop_old_query, f"Dropping old table {old_name}")
                    logger.info(f"🗑️  Dropped old table: {old_name}")
                except Exception as e:
                    logger.warning(f"⚠️  Failed to drop old table {old_name}: {e}")
            
            logger.info("✅ Cleanup completed successfully")
            return True
            
        except Exception as e:
            error_msg = f"Cleanup failed: {e}"
            logger.error(error_msg)
            self.migration_stats['warnings'].append(error_msg)
            return False
    
    def rollback_migration(self, validation_results: Dict[str, Any]) -> bool:
        """Rollback migration in case of failure."""
        logger.error("🔄 Rolling back migration...")
        
        try:
            rollback_success = True
            
            for table_config in self.tables:
                table_name = table_config.name
                backup_name = table_config.backup_name
                temp_name = table_config.temp_name
                old_name = f"{table_name}_old"
                
                try:
                    # Check what exists and restore accordingly
                    
                    # If backup exists, restore from backup
                    backup_exists_query = f"EXISTS TABLE {backup_name}"
                    backup_exists_result = self.execute_query(backup_exists_query)
                    
                    if backup_exists_result and backup_exists_result.result_rows and backup_exists_result.result_rows[0][0]:
                        # Drop current table if it exists
                        drop_current_query = f"DROP TABLE IF EXISTS {table_name}"
                        self.execute_command(drop_current_query, f"Dropping current table {table_name}")
                        
                        # Restore from backup
                        restore_query = f"RENAME TABLE {backup_name} TO {table_name}"
                        self.execute_command(restore_query, f"Restoring {table_name} from backup")
                        
                        logger.info(f"✅ Restored {table_name} from backup")
                    
                    # Clean up temporary tables
                    cleanup_queries = [
                        f"DROP TABLE IF EXISTS {temp_name}",
                        f"DROP TABLE IF EXISTS {old_name}"
                    ]
                    
                    for cleanup_query in cleanup_queries:
                        try:
                            self.execute_command(cleanup_query, "Cleaning up temporary table")
                        except Exception:
                            pass  # Ignore cleanup errors
                            
                except Exception as e:
                    logger.error(f"Failed to rollback table {table_name}: {e}")
                    rollback_success = False
            
            if rollback_success:
                logger.info("✅ Migration rollback completed successfully")
            else:
                logger.error("❌ Migration rollback completed with errors")
            
            return rollback_success
            
        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            return False
    
    def run_migration(self) -> bool:
        """Run the complete migration process."""
        self.migration_start_time = datetime.now(timezone.utc)
        
        logger.info("🚀 Starting ClickHouse Partition Removal Migration")
        logger.info("=" * 80)
        logger.info(f"Migration started at: {self.migration_start_time}")
        logger.info(f"Dry run mode: {self.config.dry_run}")
        logger.info(f"Target tables: {', '.join([t.name for t in self.tables])}")
        logger.info("=" * 80)
        
        try:
            # Phase 1: Pre-Migration Validation
            logger.info("\n📋 PHASE 1: Pre-Migration Validation")
            validation_results = self.validate_current_state()
            
            if not validation_results['tables_found']:
                raise ClickHouseMigrationError("No tables found to migrate")
            
            # Phase 2: Create Backups
            logger.info("\n💾 PHASE 2: Creating Backup Tables")
            backup_results = self.create_backup_tables(validation_results)
            
            failed_backups = [table for table, success in backup_results.items() if not success]
            if failed_backups and not self.config.dry_run:
                raise ClickHouseMigrationError(f"Failed to create backups for: {', '.join(failed_backups)}")
            
            # Phase 3: Create New Tables
            logger.info("\n🏗️  PHASE 3: Creating New Non-Partitioned Tables")
            creation_results = self.create_new_tables()
            
            failed_creations = [table for table, success in creation_results.items() if not success]
            if failed_creations:
                raise ClickHouseMigrationError(f"Failed to create new tables for: {', '.join(failed_creations)}")
            
            # Phase 4: Migrate Data
            logger.info("\n📊 PHASE 4: Migrating Data with SCD Corrections")
            migration_success = True
            
            for table_config in self.tables:
                if table_config.name in validation_results['tables_found']:
                    success = self.migrate_table_data(table_config, validation_results)
                    if not success:
                        migration_success = False
                        break
            
            if not migration_success:
                raise ClickHouseMigrationError("Data migration failed")
            
            # Phase 5: Validate Migrated Data
            logger.info("\n🔍 PHASE 5: Validating Migrated Data")
            validation_success = True
            
            for table_config in self.tables:
                if table_config.name in validation_results['tables_found']:
                    success = self.validate_migrated_data(table_config, validation_results)
                    if not success:
                        validation_success = False
                        break
            
            if not validation_success:
                raise ClickHouseMigrationError("Data validation failed")
            
            # Phase 6: Swap Tables (Blue-Green Deployment)
            logger.info("\n🔄 PHASE 6: Swapping Tables (Blue-Green Deployment)")
            swap_results = self.swap_tables()
            
            failed_swaps = [table for table, success in swap_results.items() if not success]
            if failed_swaps:
                raise ClickHouseMigrationError(f"Failed to swap tables for: {', '.join(failed_swaps)}")
            
            # Phase 7: Recreate Materialized Views
            logger.info("\n🔍 PHASE 7: Recreating Materialized Views")
            views_success = self.recreate_materialized_views()
            
            if not views_success:
                logger.warning("⚠️  Failed to recreate materialized views, but migration can continue")
                self.migration_stats['warnings'].append("Failed to recreate materialized views")
            
            # Phase 8: Final Validation
            logger.info("\n✅ PHASE 8: Final Validation")
            final_validation = self.validate_current_state()
            
            # Update migration stats
            self.migration_stats['tables_migrated'] = len([t for t in self.tables if t.name in validation_results['tables_found']])
            
            # Phase 9: Cleanup (optional)
            if not self.config.dry_run:
                logger.info("\n🧹 PHASE 9: Cleanup")
                cleanup_success = self.cleanup_old_tables()
                
                if not cleanup_success:
                    logger.warning("⚠️  Cleanup completed with warnings")
            
            # Migration completed successfully
            migration_end_time = datetime.now(timezone.utc)
            migration_duration = migration_end_time - self.migration_start_time
            
            logger.info("\n🎉 MIGRATION COMPLETED SUCCESSFULLY!")
            logger.info("=" * 80)
            logger.info(f"Migration completed at: {migration_end_time}")
            logger.info(f"Total duration: {migration_duration}")
            logger.info(f"Tables migrated: {self.migration_stats['tables_migrated']}")
            logger.info(f"Records migrated: {self.migration_stats['total_records_migrated']:,}")
            logger.info(f"Errors: {len(self.migration_stats['errors'])}")
            logger.info(f"Warnings: {len(self.migration_stats['warnings'])}")
            
            if self.config.dry_run:
                logger.info("🔄 DRY RUN MODE - No actual changes were made")
            
            logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            logger.error(f"\n❌ MIGRATION FAILED: {e}")
            
            # Attempt rollback if not in dry run mode
            if not self.config.dry_run:
                logger.info("Attempting rollback...")
                rollback_success = self.rollback_migration(validation_results if 'validation_results' in locals() else {})
                
                if rollback_success:
                    logger.info("✅ Rollback completed successfully")
                else:
                    logger.error("❌ Rollback failed - manual intervention required")
            
            return False
        
        finally:
            # Close connection
            if self.client:
                try:
                    self.client.close()
                except Exception:
                    pass


def main():
    """Main function to run the migration."""
    parser = argparse.ArgumentParser(
        description="ClickHouse Partition Removal Migration Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run to test migration
  python migrate-clickhouse-partition-removal.py --dry-run
  
  # Run actual migration
  python migrate-clickhouse-partition-removal.py
  
  # Run with custom configuration
  python migrate-clickhouse-partition-removal.py --secret-name clickhouse-prod --region us-west-2
        """
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run in dry-run mode (no actual changes)'
    )
    
    parser.add_argument(
        '--secret-name',
        default='clickhouse-connection-dev',
        help='AWS Secrets Manager secret name for ClickHouse credentials'
    )
    
    parser.add_argument(
        '--region',
        default='us-east-2',
        help='AWS region for Secrets Manager'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10000,
        help='Batch size for data migration'
    )
    
    parser.add_argument(
        '--validation-sample-size',
        type=int,
        default=1000,
        help='Sample size for data validation'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Create migration configuration
    config = MigrationConfig(
        secret_name=args.secret_name,
        region_name=args.region,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
        validation_sample_size=args.validation_sample_size
    )
    
    # Create and run migrator
    migrator = ClickHouseMigrator(config)
    
    try:
        success = migrator.run_migration()
        
        if success:
            logger.info("🎉 Migration completed successfully!")
            sys.exit(0)
        else:
            logger.error("❌ Migration failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.error("\n❌ Migration interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()