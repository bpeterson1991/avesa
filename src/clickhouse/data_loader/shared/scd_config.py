"""
SCD Configuration Utilities

This module provides utilities for reading and handling SCD (Slowly Changing Dimension) 
configuration from canonical mapping files.
"""

import json
import os
import logging
from typing import Dict, Any, Optional, Literal
from enum import Enum

try:
    from .aws_client_factory import AWSClientFactory
except ImportError:
    from aws_client_factory import AWSClientFactory

logger = logging.getLogger(__name__)

# Type definitions for SCD types
SCDType = Literal["type_1", "type_2"]

class SCDTypeEnum(Enum):
    """Enumeration for SCD types."""
    TYPE_1 = "type_1"
    TYPE_2 = "type_2"


class SCDConfigManager:
    """
    Manager for SCD configuration from canonical mapping files.
    
    This class provides centralized access to SCD type configuration
    and validation for data processing components.
    """

    def __init__(self, s3_client=None):
        """
        Initialize the SCD configuration manager.
        
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
        
        # Cache for loaded SCD configurations
        self._scd_config_cache = {}

    def get_scd_type(self, table_name: str, bucket: Optional[str] = None) -> SCDType:
        """
        Get the SCD type for a specific table.
        
        Args:
            table_name: Name of the canonical table
            bucket: Optional S3 bucket name for loading mappings
            
        Returns:
            SCD type ("type_1" or "type_2")
        """
        try:
            mapping = self._load_canonical_mapping(table_name, bucket)
            scd_type = mapping.get('scd_type', 'type_1')  # Default to type_1
            
            # Validate SCD type
            if scd_type not in ['type_1', 'type_2']:
                logger.warning(f"Invalid SCD type '{scd_type}' for table {table_name}, defaulting to type_1")
                return 'type_1'
            
            logger.debug(f"Table {table_name} has SCD type: {scd_type}")
            return scd_type
            
        except Exception as e:
            logger.error(f"Failed to get SCD type for table {table_name}: {e}")
            return 'type_1'  # Safe default

    def is_scd_type_1(self, table_name: str, bucket: Optional[str] = None) -> bool:
        """
        Check if a table uses SCD Type 1 processing.
        
        Args:
            table_name: Name of the canonical table
            bucket: Optional S3 bucket name for loading mappings
            
        Returns:
            True if table uses SCD Type 1, False otherwise
        """
        return self.get_scd_type(table_name, bucket) == 'type_1'

    def is_scd_type_2(self, table_name: str, bucket: Optional[str] = None) -> bool:
        """
        Check if a table uses SCD Type 2 processing.
        
        Args:
            table_name: Name of the canonical table
            bucket: Optional S3 bucket name for loading mappings
            
        Returns:
            True if table uses SCD Type 2, False otherwise
        """
        return self.get_scd_type(table_name, bucket) == 'type_2'

    def get_scd_config_for_tables(self, table_names: list, bucket: Optional[str] = None) -> Dict[str, SCDType]:
        """
        Get SCD configuration for multiple tables.
        
        Args:
            table_names: List of canonical table names
            bucket: Optional S3 bucket name for loading mappings
            
        Returns:
            Dictionary mapping table names to their SCD types
        """
        scd_config = {}
        for table_name in table_names:
            scd_config[table_name] = self.get_scd_type(table_name, bucket)
        
        return scd_config

    def filter_tables_by_scd_type(self, table_names: list, scd_type: SCDType, 
                                  bucket: Optional[str] = None) -> list:
        """
        Filter tables by their SCD type.
        
        Args:
            table_names: List of canonical table names
            scd_type: SCD type to filter by ("type_1" or "type_2")
            bucket: Optional S3 bucket name for loading mappings
            
        Returns:
            List of table names that match the specified SCD type
        """
        filtered_tables = []
        for table_name in table_names:
            if self.get_scd_type(table_name, bucket) == scd_type:
                filtered_tables.append(table_name)
        
        logger.info(f"Filtered {len(filtered_tables)} tables with SCD {scd_type} from {len(table_names)} total tables")
        return filtered_tables

    def validate_scd_configuration(self, table_name: str, bucket: Optional[str] = None) -> Dict[str, Any]:
        """
        Validate SCD configuration for a table.
        
        Args:
            table_name: Name of the canonical table
            bucket: Optional S3 bucket name for loading mappings
            
        Returns:
            Validation result with status and details
        """
        try:
            mapping = self._load_canonical_mapping(table_name, bucket)
            
            validation_result = {
                'table_name': table_name,
                'valid': True,
                'issues': []
            }
            
            # Check if scd_type is present
            if 'scd_type' not in mapping:
                validation_result['issues'].append('Missing scd_type configuration')
                validation_result['valid'] = False
            else:
                scd_type = mapping['scd_type']
                
                # Validate SCD type value
                if scd_type not in ['type_1', 'type_2']:
                    validation_result['issues'].append(f'Invalid scd_type value: {scd_type}')
                    validation_result['valid'] = False
                else:
                    validation_result['scd_type'] = scd_type
            
            # Check for service mappings
            service_count = 0
            for key, value in mapping.items():
                if key != 'scd_type' and isinstance(value, dict):
                    service_count += 1
            
            if service_count == 0:
                validation_result['issues'].append('No service mappings found')
                validation_result['valid'] = False
            else:
                validation_result['service_count'] = service_count
            
            return validation_result
            
        except Exception as e:
            return {
                'table_name': table_name,
                'valid': False,
                'issues': [f'Failed to load mapping: {str(e)}']
            }

    def _load_canonical_mapping(self, table_name: str, bucket: Optional[str] = None) -> Dict[str, Any]:
        """
        Load canonical mapping configuration for a table.
        
        Args:
            table_name: Name of the canonical table
            bucket: Optional S3 bucket name for loading mappings
            
        Returns:
            Mapping configuration dictionary
        """
        # Check cache first
        cache_key = f"{table_name}_{bucket or 'default'}"
        if cache_key in self._scd_config_cache:
            return self._scd_config_cache[cache_key]
        
        mapping = None
        
        # Try to load from bundled file first
        bundled_path = os.path.join(
            os.path.dirname(__file__), '..', '..', 
            'mappings', 'canonical', f'{table_name}.json'
        )
        if os.path.exists(bundled_path):
            try:
                with open(bundled_path, 'r') as f:
                    mapping = json.load(f)
                logger.debug(f"Loaded SCD mapping from bundled file: {bundled_path}")
            except Exception as e:
                logger.warning(f"Failed to load bundled SCD mapping: {e}")
        
        # Try local development file
        if mapping is None:
            local_path = os.path.join('mappings', 'canonical', f'{table_name}.json')
            if os.path.exists(local_path):
                try:
                    with open(local_path, 'r') as f:
                        mapping = json.load(f)
                    logger.debug(f"Loaded SCD mapping from local file: {local_path}")
                except Exception as e:
                    logger.warning(f"Failed to load local SCD mapping: {e}")
        
        # Try S3 as fallback
        if mapping is None and bucket and self.s3_client:
            try:
                s3_key = f"mappings/canonical/{table_name}.json"
                response = self.s3_client.get_object(Bucket=bucket, Key=s3_key)
                mapping = json.loads(response['Body'].read().decode('utf-8'))
                logger.debug(f"Loaded SCD mapping from S3: {bucket}/{s3_key}")
            except Exception as e:
                logger.warning(f"Failed to load SCD mapping from S3: {e}")
        
        # Use default mapping as final fallback
        if mapping is None:
            mapping = self._get_default_scd_mapping(table_name)
            logger.info(f"Using default SCD mapping for {table_name}")
        
        # Cache the result
        self._scd_config_cache[cache_key] = mapping
        return mapping

    def _get_default_scd_mapping(self, table_name: str) -> Dict[str, Any]:
        """
        Get default SCD mapping for a table.
        
        Args:
            table_name: Name of the canonical table
            
        Returns:
            Default mapping with SCD configuration
        """
        # Default SCD types based on business requirements
        default_scd_types = {
            'companies': 'type_1',    # Simple upsert for companies
            'contacts': 'type_1',     # Simple upsert for contacts  
            'tickets': 'type_2',      # Full historical tracking for tickets
            'time_entries': 'type_1'  # Simple upsert for time entries
        }
        
        scd_type = default_scd_types.get(table_name, 'type_1')
        
        return {
            'scd_type': scd_type,
            'default_mapping': True
        }


# Convenience functions for backward compatibility
def get_scd_type(table_name: str, bucket: Optional[str] = None) -> SCDType:
    """
    Get the SCD type for a specific table.
    
    Args:
        table_name: Name of the canonical table
        bucket: Optional S3 bucket name for loading mappings
        
    Returns:
        SCD type ("type_1" or "type_2")
    """
    manager = SCDConfigManager()
    return manager.get_scd_type(table_name, bucket)


def is_scd_type_1(table_name: str, bucket: Optional[str] = None) -> bool:
    """
    Check if a table uses SCD Type 1 processing.
    
    Args:
        table_name: Name of the canonical table
        bucket: Optional S3 bucket name for loading mappings
        
    Returns:
        True if table uses SCD Type 1, False otherwise
    """
    manager = SCDConfigManager()
    return manager.is_scd_type_1(table_name, bucket)


def is_scd_type_2(table_name: str, bucket: Optional[str] = None) -> bool:
    """
    Check if a table uses SCD Type 2 processing.
    
    Args:
        table_name: Name of the canonical table
        bucket: Optional S3 bucket name for loading mappings
        
    Returns:
        True if table uses SCD Type 2, False otherwise
    """
    manager = SCDConfigManager()
    return manager.is_scd_type_2(table_name, bucket)


def filter_tables_by_scd_type(table_names: list, scd_type: SCDType, 
                              bucket: Optional[str] = None) -> list:
    """
    Filter tables by their SCD type.
    
    Args:
        table_names: List of canonical table names
        scd_type: SCD type to filter by ("type_1" or "type_2")
        bucket: Optional S3 bucket name for loading mappings
        
    Returns:
        List of table names that match the specified SCD type
    """
    manager = SCDConfigManager()
    return manager.filter_tables_by_scd_type(table_names, scd_type, bucket)


def validate_scd_configuration(table_name: str, bucket: Optional[str] = None) -> Dict[str, Any]:
    """
    Validate SCD configuration for a table.
    
    Args:
        table_name: Name of the canonical table
        bucket: Optional S3 bucket name for loading mappings
        
    Returns:
        Validation result with status and details
    """
    manager = SCDConfigManager()
    return manager.validate_scd_configuration(table_name, bucket)