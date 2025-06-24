"""
Canonical Data Mapper - Centralized data transformation and mapping

This module provides:
- Centralized canonical data transformation logic
- Service-specific to canonical mapping functions
- Data validation and quality checks
- Schema evolution and versioning support
- Performance-optimized transformation pipelines
"""

import json
import os
import hashlib
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

try:
    from .aws_client_factory import AWSClientFactory
    from .utils import get_timestamp
except ImportError:
    # Fallback for direct imports
    from aws_client_factory import AWSClientFactory
    from utils import get_timestamp

logger = logging.getLogger(__name__)


class CanonicalMapper:
    """
    Centralized canonical data mapper for transforming service-specific data
    to canonical format.
    
    Consolidates duplicate code from:
    - src/canonical_transform/lambda_function.py:410-420
    - Multiple Lambda functions using canonical mapping
    """

    def __init__(self, s3_client=None):
        """
        Initialize the canonical mapper.
        
        Args:
            s3_client: Optional S3 client for loading mappings from S3
        """
        if s3_client is None:
            # Create S3 client using shared factory
            aws_factory = AWSClientFactory()
            clients = aws_factory.get_client_bundle(['s3'])
            self.s3_client = clients['s3']
        else:
            self.s3_client = s3_client
        
        # Cache for loaded mappings
        self._mapping_cache = {}

    def get_default_mapping(self, table_type: str) -> Dict[str, Any]:
        """
        Get default mapping configuration for a table type.
        
        EMERGENCY FIX: Updated to match actual mapping files to prevent data corruption.
        
        Args:
            table_type: Type of table (companies, contacts, tickets, time_entries)
            
        Returns:
            Default mapping configuration matching the actual mapping files
        """
        default_mappings = {
            'companies': {
                'id_field': 'id',
                'fields': {
                    'id': 'id',  # FIXED: Match actual mapping file
                    'company_id': 'id',  # Keep for backward compatibility
                    'company_name': 'name',  # FIXED: Match actual mapping file
                    'company_identifier': 'identifier',
                    'status': 'status__name',
                    'last_updated': '_info__lastUpdated'
                }
            },
            'contacts': {
                'id_field': 'id',
                'fields': {
                    'id': 'id',  # FIXED: Match actual mapping file
                    'contact_id': 'id',
                    'company_id': 'company__id',
                    'first_name': 'firstName',
                    'last_name': 'lastName',
                    'company_name': 'company__name',
                    'last_updated': '_info__lastUpdated'
                }
            },
            'tickets': {
                'id_field': 'id',
                'fields': {
                    'id': 'id',  # FIXED: Match actual mapping file
                    'ticket_id': 'id',
                    'company_id': 'company__id',
                    'contact_id': 'contact__id',
                    'summary': 'summary',
                    'status': 'status__name',
                    'last_updated': '_info__lastUpdated'
                }
            },
            'time_entries': {
                'id_field': 'id',
                'fields': {
                    'id': 'id',  # FIXED: Match actual mapping file
                    'entry_id': 'id',
                    'company_id': 'company__id',
                    'ticket_id': 'ticket__id',
                    'actual_hours': 'actualHours',  # FIXED: Match actual mapping file
                    'notes': 'notes',
                    'last_updated': '_info__lastUpdated'
                }
            }
        }
        
        return default_mappings.get(table_type, {})

    def load_mapping(self, table_type: str, bucket: Optional[str] = None) -> Dict[str, Any]:
        """
        Load mapping configuration for a table type.
        
        Args:
            table_type: Type of table (companies, contacts, tickets, time_entries)
            bucket: S3 bucket name for loading mappings
            
        Returns:
            Mapping configuration
        """
        # Check cache first
        cache_key = f"{table_type}_{bucket or 'default'}"
        if cache_key in self._mapping_cache:
            return self._mapping_cache[cache_key]
        
        mapping = None
        
        # Try to load from bundled file first
        bundled_path = os.path.join(os.path.dirname(__file__), '..', '..', 'mappings', 'canonical', f'{table_type}.json')
        if os.path.exists(bundled_path):
            try:
                with open(bundled_path, 'r') as f:
                    mapping = json.load(f)
                logger.debug(f"Loaded mapping from bundled file: {bundled_path}")
            except Exception as e:
                logger.warning(f"Failed to load bundled mapping: {e}")
        
        # Try local development file
        if mapping is None:
            local_path = os.path.join('mappings', 'canonical', f'{table_type}.json')
            if os.path.exists(local_path):
                try:
                    with open(local_path, 'r') as f:
                        mapping = json.load(f)
                    logger.debug(f"Loaded mapping from local file: {local_path}")
                except Exception as e:
                    logger.warning(f"Failed to load local mapping: {e}")
        
        # Try S3 as fallback
        if mapping is None and bucket and self.s3_client:
            try:
                s3_key = f"mappings/canonical/{table_type}.json"
                response = self.s3_client.get_object(Bucket=bucket, Key=s3_key)
                mapping = json.loads(response['Body'].read().decode('utf-8'))
                logger.debug(f"Loaded mapping from S3: {bucket}/{s3_key}")
            except Exception as e:
                logger.warning(f"Failed to load mapping from S3: {e}")
        
        # Use default mapping as final fallback
        if mapping is None:
            mapping = self.get_default_mapping(table_type)
            logger.info(f"Using default mapping for {table_type}")
        
        # Cache the result
        self._mapping_cache[cache_key] = mapping
        return mapping

    def _get_nested_value(self, data: Dict[str, Any], field_path: str) -> Any:
        """
        Get nested value from data using double underscore notation.
        
        Args:
            data: Source data dictionary
            field_path: Field path with double underscores (e.g., 'status__name')
            
        Returns:
            Value at the specified path or None if not found
        """
        try:
            keys = field_path.split('__')
            value = data
            
            for key in keys:
                if isinstance(value, dict):
                    # Try exact match first
                    if key in value:
                        value = value[key]
                    else:
                        # Try case-insensitive match for common field variations
                        key_lower = key.lower()
                        found = False
                        for data_key in value.keys():
                            if data_key.lower() == key_lower:
                                value = value[data_key]
                                found = True
                                break
                        if not found:
                            return None
                else:
                    return None
            
            return value
        except Exception:
            return None

    def _calculate_record_hash(self, record: Dict[str, Any]) -> str:
        """
        Calculate MD5 hash of record content for change detection.
        
        Args:
            record: Record dictionary
            
        Returns:
            MD5 hash string
        """
        # Exclude SCD fields from hash calculation
        scd_fields = {
            'effective_start_date', 'effective_end_date', 'is_current',
            'record_hash', 'ingestion_timestamp'
        }
        
        # Create a copy without SCD fields
        hash_data = {k: v for k, v in record.items() if k not in scd_fields}
        
        # Sort keys for consistent hashing
        sorted_data = json.dumps(hash_data, sort_keys=True, default=str)
        return hashlib.md5(sorted_data.encode()).hexdigest()

    def _get_source_table_for_canonical(self, canonical_table: str) -> str:
        """Map canonical table to source table name."""
        # For now, assume 1:1 mapping
        return canonical_table

    def get_source_mapping(self, canonical_table: str) -> Optional[Dict[str, str]]:
        """
        Get source service and table mapping for canonical table by reading from integration mapping files.
        
        Args:
            canonical_table: Name of canonical table
            
        Returns:
            Dictionary with 'service' and 'table' keys, or None if not found
        """
        # Load integration mappings dynamically from files
        integration_files = [
            'connectwise_endpoints.json',
            'salesforce_endpoints.json',
            'servicenow_endpoints.json'
        ]
        
        for integration_file in integration_files:
            try:
                # Try bundled file first
                bundled_path = os.path.join(os.path.dirname(__file__), '..', '..', 'mappings', 'integrations', integration_file)
                if os.path.exists(bundled_path):
                    with open(bundled_path, 'r') as f:
                        integration_config = json.load(f)
                else:
                    # Try local development file
                    local_path = os.path.join('mappings', 'integrations', integration_file)
                    if os.path.exists(local_path):
                        with open(local_path, 'r') as f:
                            integration_config = json.load(f)
                    else:
                        continue
                
                service_name = integration_config.get('service_name')
                endpoints = integration_config.get('endpoints', {})
                
                # Search for matching canonical table
                for endpoint_name, endpoint_config in endpoints.items():
                    # Check both 'table_name' (ConnectWise) and 'canonical_table' (Salesforce/ServiceNow)
                    endpoint_table = endpoint_config.get('table_name') or endpoint_config.get('canonical_table')
                    
                    if endpoint_table == canonical_table:
                        logger.debug(f"Found mapping for {canonical_table}: {service_name} -> {endpoint_name}")
                        return {
                            'service': service_name,
                            'table': endpoint_name  # Use the actual API endpoint name
                        }
                        
            except Exception as e:
                logger.warning(f"Failed to load integration mapping from {integration_file}: {e}")
                continue
        
        logger.warning(f"No source mapping found for canonical table: {canonical_table}")
        return None

    def transform_record(self, raw_record: Dict[str, Any], mapping: Dict[str, Any],
                        canonical_table: str, tenant_id: str = None) -> Optional[Dict[str, Any]]:
        """
        Transform a single record to canonical format.
        
        Args:
            raw_record: Raw record from source system
            mapping: Mapping configuration
            canonical_table: Target canonical table name
            tenant_id: Tenant ID for multi-tenant support
            
        Returns:
            Transformed canonical record or None if transformation fails
        """
        try:
            if not mapping or 'fields' not in mapping:
                logger.warning(f"Invalid mapping for {canonical_table}, using default mapping")
                mapping = self.get_default_mapping(canonical_table)
                if not mapping or 'fields' not in mapping:
                    logger.error(f"No valid mapping available for {canonical_table}")
                    return None
            
            canonical_record = {}
            
            # Apply field mappings
            for canonical_field, source_field in mapping['fields'].items():
                value = self._get_nested_value(raw_record, source_field)
                if value is not None:
                    # Convert ID fields to strings for consistency
                    if canonical_field in ['id', 'company_id', 'contact_id', 'ticket_id', 'entry_id']:
                        canonical_record[canonical_field] = str(value)
                    else:
                        canonical_record[canonical_field] = value
            
            # CRITICAL FIX: Add tenant_id to prevent NULL values
            if tenant_id:
                canonical_record['tenant_id'] = tenant_id
            
            # Add metadata fields
            source_mapping = self.get_source_mapping(canonical_table)
            if source_mapping:
                canonical_record['source_system'] = source_mapping['service']
                canonical_record['source_table'] = source_mapping['table']
            
            canonical_record['canonical_table'] = canonical_table
            
            # Add SCD Type 2 fields
            current_timestamp = get_timestamp()
            
            # Use updated_date if available, otherwise use current timestamp
            effective_date = canonical_record.get('updated_date', current_timestamp)
            canonical_record['effective_start_date'] = effective_date
            canonical_record['effective_end_date'] = None
            canonical_record['is_current'] = True
            canonical_record['ingestion_timestamp'] = current_timestamp
            
            # Calculate record hash for change detection
            canonical_record['record_hash'] = self._calculate_record_hash(canonical_record)
            
            return canonical_record
            
        except Exception as e:
            logger.error(f"Failed to transform record for {canonical_table}: {e}")
            return None

    # Legacy methods for backward compatibility
    def map_company_data(self, source_data: Dict[str, Any], service: str) -> Dict[str, Any]:
        """
        Map company data from service-specific format to canonical format.
        
        Args:
            source_data: Source data in service-specific format
            service: Source service name (connectwise, servicenow, salesforce)
            
        Returns:
            Data in canonical format with required fields for validation
        """
        mapping = self.load_mapping('companies')
        canonical_data = self.transform_record(source_data, mapping, 'companies')
        
        if not canonical_data:
            return {}
        
        # Ensure required fields for validation are present
        # Map common field variations to expected canonical fields
        if 'company_id' in canonical_data and 'id' not in canonical_data:
            canonical_data['id'] = canonical_data['company_id']
        
        # Ensure we have a name field
        if 'name' not in canonical_data and 'company_name' in canonical_data:
            canonical_data['name'] = canonical_data['company_name']
        
        return canonical_data

    def map_contact_data(self, source_data: Dict[str, Any], service: str) -> Dict[str, Any]:
        """
        Map contact data from service-specific format to canonical format.
        
        Args:
            source_data: Source data in service-specific format
            service: Source service name
            
        Returns:
            Data in canonical format
        """
        mapping = self.load_mapping('contacts')
        return self.transform_record(source_data, mapping, 'contacts') or {}

    def map_ticket_data(self, source_data: Dict[str, Any], service: str) -> Dict[str, Any]:
        """
        Map ticket data from service-specific format to canonical format.
        
        Args:
            source_data: Source data in service-specific format
            service: Source service name
            
        Returns:
            Data in canonical format
        """
        mapping = self.load_mapping('tickets')
        return self.transform_record(source_data, mapping, 'tickets') or {}

    def map_time_entry_data(self, source_data: Dict[str, Any], service: str) -> Dict[str, Any]:
        """
        Map time entry data from service-specific format to canonical format.
        
        Args:
            source_data: Source data in service-specific format
            service: Source service name
            
        Returns:
            Data in canonical format
        """
        mapping = self.load_mapping('time_entries')
        return self.transform_record(source_data, mapping, 'time_entries') or {}


def create_canonical_mapper() -> CanonicalMapper:
    """
    Create centralized canonical data mapper.
    
    Returns:
        CanonicalMapper instance
    """
    return CanonicalMapper()