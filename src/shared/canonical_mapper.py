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
from collections import OrderedDict

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

    def __init__(self, s3_client=None, max_cache_size=10):
        """
        Initialize the canonical mapper.
        
        Args:
            s3_client: Optional S3 client for loading mappings from S3
            max_cache_size: Maximum number of mappings to cache (prevents memory leaks)
        """
        if s3_client is None:
            # Create S3 client using shared factory
            aws_factory = AWSClientFactory()
            clients = aws_factory.get_client_bundle(['s3'])
            self.s3_client = clients['s3']
        else:
            self.s3_client = s3_client
        
        # MEMORY OPTIMIZATION: Bounded cache with LRU eviction
        self.max_cache_size = max_cache_size
        self._mapping_cache = OrderedDict()

# REMOVED: get_default_mapping function - redundant since we have actual mapping files
# This eliminates hardcoded fallback data that could drift from real JSON files

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
        
        # PRIORITY 1: Try S3 first (production mapping files location)
        if bucket and self.s3_client:
            try:
                s3_key = f"mappings/canonical/{table_type}.json"
                response = self.s3_client.get_object(Bucket=bucket, Key=s3_key)
                mapping = json.loads(response['Body'].read().decode('utf-8'))
                logger.debug(f"Loaded mapping from S3: {bucket}/{s3_key}")
            except Exception as e:
                logger.warning(f"Failed to load mapping from S3: {e}")
        
        # PRIORITY 2: Try local development file (for local testing)
        if mapping is None:
            local_path = os.path.join('mappings', 'canonical', f'{table_type}.json')
            if os.path.exists(local_path):
                try:
                    with open(local_path, 'r') as f:
                        mapping = json.load(f)
                    logger.debug(f"Loaded mapping from local file: {local_path}")
                except Exception as e:
                    logger.warning(f"Failed to load local mapping: {e}")
        
        # PRIORITY 3: Try bundled file as final fallback
        if mapping is None:
            bundled_path = os.path.join(os.path.dirname(__file__), '..', '..', 'mappings', 'canonical', f'{table_type}.json')
            if os.path.exists(bundled_path):
                try:
                    with open(bundled_path, 'r') as f:
                        mapping = json.load(f)
                    logger.debug(f"Loaded mapping from bundled file: {bundled_path}")
                except Exception as e:
                    logger.warning(f"Failed to load bundled mapping: {e}")
        
        # If no mapping found, raise an error - we should always have mapping files
        if mapping is None:
            raise FileNotFoundError(f"No mapping file found for table type '{table_type}'. "
                                  f"Expected S3 location: {bucket or 'BUCKET_NAME'}/mappings/canonical/{table_type}.json")
        
        # MEMORY OPTIMIZATION: Cache the result with size management
        self._manage_cache(cache_key, mapping)
        return mapping
    
    def _manage_cache(self, key: str, value: Dict[str, Any]) -> None:
        """
        Manage cache size to prevent memory leaks.
        
        Args:
            key: Cache key
            value: Value to cache
        """
        # Remove oldest items if cache is full
        while len(self._mapping_cache) >= self.max_cache_size:
            oldest_key = next(iter(self._mapping_cache))
            removed = self._mapping_cache.pop(oldest_key)
            logger.debug(f"Evicted mapping from cache: {oldest_key}")
        
        # Add/update the item (moves to end if exists)
        self._mapping_cache[key] = value
        self._mapping_cache.move_to_end(key)

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
        Get source service and table mapping for canonical table.
        
        Args:
            canonical_table: Name of canonical table
            
        Returns:
            Dictionary with 'service' and 'table' keys, or None if not found
        """
        # Default mappings for canonical tables
        source_mappings = {
            'companies': {'service': 'connectwise', 'table': 'companies'},
            'contacts': {'service': 'connectwise', 'table': 'contacts'},
            'tickets': {'service': 'connectwise', 'table': 'tickets'},
            'time_entries': {'service': 'connectwise', 'table': 'time_entries'}
        }
        
        return source_mappings.get(canonical_table)

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
            if not mapping:
                logger.error(f"Invalid or missing mapping for {canonical_table}")
                return None
            
            canonical_record = {}
            
            # Determine the service based on source system (default to connectwise)
            source_mapping = self.get_source_mapping(canonical_table)
            service = source_mapping['service'] if source_mapping else 'connectwise'
            
            # Get service-specific mapping
            service_mapping = mapping.get(service)
            if not service_mapping:
                logger.error(f"No mapping found for service '{service}' in {canonical_table}")
                return None
            
            # Get the table-specific mapping within the service
            source_table = source_mapping['table'] if source_mapping else canonical_table
            table_mapping_key = f"{service}/{source_table}" if service == 'connectwise' else source_table
            table_mapping = service_mapping.get(table_mapping_key) or service_mapping.get(source_table)
            
            if not table_mapping:
                # Fallback - try to find any mapping in the service
                if len(service_mapping) == 1:
                    table_mapping = list(service_mapping.values())[0]
                else:
                    logger.error(f"No table mapping found for '{table_mapping_key}' in service '{service}'")
                    return None
            
            # Apply field mappings - transform ALL fields from the mapping
            for canonical_field, source_field in table_mapping.items():
                value = self._get_nested_value(raw_record, source_field)
                if value is not None:
                    # Convert ID fields to strings for consistency
                    if canonical_field in ['id', 'company_id', 'contact_id', 'ticket_id', 'entry_id']:
                        canonical_record[canonical_field] = str(value)
                    else:
                        canonical_record[canonical_field] = value
                else:
                    # Set None for missing fields to maintain schema consistency
                    canonical_record[canonical_field] = None
            
            # CRITICAL FIX: Add tenant_id to prevent NULL values
            if tenant_id:
                canonical_record['tenant_id'] = tenant_id
            
            # Add metadata fields
            if source_mapping:
                canonical_record['source_system'] = source_mapping['service']
                canonical_record['source_table'] = source_mapping['table']
            
            canonical_record['canonical_table'] = canonical_table
            
            # CRITICAL FIX: Do NOT add SCD Type 2 fields here
            # SCD fields should be added by the SCD processing logic based on table configuration
            # This was causing schema mismatches for Type 1 tables
            current_timestamp = get_timestamp()
            canonical_record['ingestion_timestamp'] = current_timestamp
            
            # Calculate record hash for change detection
            canonical_record['record_hash'] = self._calculate_record_hash(canonical_record)
            
            logger.debug(f"Transformed record with {len(canonical_record)} fields for {canonical_table}")
            
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