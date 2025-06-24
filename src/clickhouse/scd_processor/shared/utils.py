"""
Utility functions for the dynamic data pipeline.
"""

import json
import os
import glob
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

try:
    from .config_simple import TenantConfig
except ImportError:
    from config_simple import TenantConfig


def flatten_json(data: Dict[str, Any], parent_key: str = '', sep: str = '__') -> Dict[str, Any]:
    """
    Flatten nested JSON object.
    
    Args:
        data: Dictionary to flatten
        parent_key: Parent key for nested objects
        sep: Separator for nested keys
        
    Returns:
        Flattened dictionary
    """
    items = []
    
    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        
        if isinstance(value, dict):
            items.extend(flatten_json(value, new_key, sep=sep).items())
        elif isinstance(value, list):
            # Handle arrays by converting to JSON string
            items.append((new_key, json.dumps(value) if value else None))
        else:
            items.append((new_key, value))
    
    return dict(items)


def get_timestamp() -> str:
    """Get current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def get_s3_key(tenant_id: str, data_type: str, service: str, table_name: str, timestamp: str) -> str:
    """
    Generate S3 key for data storage.
    
    Args:
        tenant_id: Tenant identifier
        data_type: Type of data (raw, canonical)
        service: Service name (e.g., connectwise, salesforce, servicenow)
        table_name: Table name (should be explicit table name, not derived from endpoint)
        timestamp: Timestamp string
        
    Returns:
        S3 key path
    """
    return f"{tenant_id}/{data_type}/{service}/{table_name}/{timestamp}.parquet"


def get_table_name_from_endpoint_config(endpoint_config: Dict[str, Any], endpoint_path: str) -> str:
    """
    Get explicit table name from endpoint configuration.
    
    Args:
        endpoint_config: Endpoint configuration dictionary
        endpoint_path: Endpoint path (e.g., 'time/entries')
        
    Returns:
        Explicit table name from configuration
        
    Raises:
        ValueError: If endpoint_config is None
    """
    if endpoint_config is None:
        raise ValueError(f"No endpoint configuration provided for {endpoint_path}")
    
    # Check for explicit table_name field
    table_name = endpoint_config.get('table_name')
    if table_name:
        return table_name
    
    # Fallback to canonical_table if table_name not present
    canonical_table = endpoint_config.get('canonical_table')
    if canonical_table:
        return canonical_table
    
    # Last resort: derive from endpoint path (but log warning)
    derived_name = endpoint_path.split('/')[-1]
    print(f"WARNING: No explicit table_name found for {endpoint_path}, using derived name: {derived_name}")
    return derived_name


def load_endpoint_configuration(service_name: str) -> Dict[str, Any]:
    """
    Load endpoint configuration for a service.
    
    This function tries multiple sources in order of preference:
    1. Bundled mapping files (Lambda package)
    2. Local development mappings (relative path)
    3. S3 bucket (if BUCKET_NAME environment variable is set)
    
    Args:
        service_name: Service name (e.g., 'connectwise')
        
    Returns:
        Endpoint configuration dictionary
    """
    import json
    import os
    
    try:
        # Try bundled mapping files first (Lambda package deployment)
        bundled_config_path = os.path.join(
            os.path.dirname(__file__),
            'mappings', 'integrations',
            f'{service_name}_endpoints.json'
        )
        
        if os.path.exists(bundled_config_path):
            with open(bundled_config_path, 'r') as f:
                return json.load(f)
        
        # Try local development mappings (relative path)
        local_config_path = os.path.join(
            os.path.dirname(__file__),
            '..', '..',
            'mappings', 'integrations',
            f'{service_name}_endpoints.json'
        )
        
        if os.path.exists(local_config_path):
            with open(local_config_path, 'r') as f:
                return json.load(f)
        
        # Try S3 as fallback (if in Lambda environment with S3 access)
        bucket_name = os.environ.get('BUCKET_NAME')
        if bucket_name:
            try:
                import boto3
                s3 = boto3.client('s3')
                config_key = f"mappings/integrations/{service_name}_endpoints.json"
                response = s3.get_object(Bucket=bucket_name, Key=config_key)
                return json.loads(response['Body'].read().decode('utf-8'))
            except Exception as s3_error:
                print(f"Failed to load endpoint config from S3: {s3_error}")
        
        print(f"No endpoint configuration found for {service_name}")
        return {}
        
    except Exception as e:
        print(f"Failed to load endpoint configuration for {service_name}: {e}")
        return {}


def discover_available_services() -> List[str]:
    """
    Discover all available services by scanning the mappings directory.
    
    This function tries multiple sources in order of preference:
    1. Bundled mapping files (Lambda package)
    2. Local development mappings (relative path)
    
    Returns:
        List of available service names
    """
    try:
        services = []
        
        # Try bundled mappings first (Lambda package deployment)
        bundled_mappings_dir = os.path.join(os.path.dirname(__file__), 'mappings')
        if os.path.exists(bundled_mappings_dir):
            services.extend(_discover_services_in_directory(bundled_mappings_dir))
        
        # Try local development mappings if no bundled mappings found
        if not services:
            local_mappings_dir = os.path.join(
                os.path.dirname(__file__),
                '..', '..',
                'mappings'
            )
            if os.path.exists(local_mappings_dir):
                services.extend(_discover_services_in_directory(local_mappings_dir))
        
        return sorted(list(set(services)))  # Remove duplicates and sort
        
    except Exception as e:
        print(f"Failed to discover available services: {e}")
        return []


def _discover_services_in_directory(mappings_dir: str) -> List[str]:
    """Helper function to discover services in a specific directory."""
    services = []
    
    # Check services directory
    services_dir = os.path.join(mappings_dir, 'services')
    if os.path.exists(services_dir):
        for file_path in glob.glob(os.path.join(services_dir, '*.json')):
            service_name = os.path.basename(file_path).replace('.json', '')
            services.append(service_name)
    
    # Also check integrations directory for backward compatibility
    integrations_dir = os.path.join(mappings_dir, 'integrations')
    if os.path.exists(integrations_dir):
        for file_path in glob.glob(os.path.join(integrations_dir, '*_endpoints.json')):
            service_name = os.path.basename(file_path).replace('_endpoints.json', '')
            if service_name not in services:
                services.append(service_name)
    
    return services


def load_service_configuration(service_name: str) -> Dict[str, Any]:
    """
    Load service configuration from the services directory.
    
    This function tries multiple sources in order of preference:
    1. Bundled mapping files (Lambda package)
    2. Local development mappings (relative path)
    3. S3 bucket (if BUCKET_NAME environment variable is set)
    
    Args:
        service_name: Service name (e.g., 'connectwise')
        
    Returns:
        Service configuration dictionary
    """
    try:
        # Try bundled mapping files first (Lambda package deployment)
        bundled_config_path = os.path.join(
            os.path.dirname(__file__),
            'mappings', 'services',
            f'{service_name}.json'
        )
        
        if os.path.exists(bundled_config_path):
            with open(bundled_config_path, 'r') as f:
                return json.load(f)
        
        # Try local development mappings (relative path)
        local_config_path = os.path.join(
            os.path.dirname(__file__),
            '..', '..',
            'mappings', 'services',
            f'{service_name}.json'
        )
        
        if os.path.exists(local_config_path):
            with open(local_config_path, 'r') as f:
                return json.load(f)
        
        # Try S3 as fallback (if in Lambda environment with S3 access)
        bucket_name = os.environ.get('BUCKET_NAME')
        if bucket_name:
            try:
                import boto3
                s3 = boto3.client('s3')
                config_key = f"mappings/services/{service_name}.json"
                response = s3.get_object(Bucket=bucket_name, Key=config_key)
                return json.loads(response['Body'].read().decode('utf-8'))
            except Exception as s3_error:
                print(f"Failed to load service config from S3: {s3_error}")
        
        print(f"No service configuration found for {service_name}")
        return {}
        
    except Exception as e:
        print(f"Failed to load service configuration for {service_name}: {e}")
        return {}


def discover_canonical_tables() -> List[str]:
    """
    Discover all canonical tables by scanning the canonical mappings directory.
    
    This function tries multiple sources in order of preference:
    1. Bundled mapping files (Lambda package)
    2. Local development mappings (relative path)
    
    Returns:
        List of canonical table names
    """
    try:
        canonical_tables = []
        
        # Try bundled mappings first (Lambda package deployment)
        bundled_canonical_dir = os.path.join(
            os.path.dirname(__file__),
            'mappings', 'canonical'
        )
        
        if os.path.exists(bundled_canonical_dir):
            for file_path in glob.glob(os.path.join(bundled_canonical_dir, '*.json')):
                table_name = os.path.basename(file_path).replace('.json', '')
                canonical_tables.append(table_name)
        
        # Try local development mappings if no bundled mappings found
        if not canonical_tables:
            local_canonical_dir = os.path.join(
                os.path.dirname(__file__),
                '..', '..',
                'mappings', 'canonical'
            )
            
            if os.path.exists(local_canonical_dir):
                for file_path in glob.glob(os.path.join(local_canonical_dir, '*.json')):
                    table_name = os.path.basename(file_path).replace('.json', '')
                    canonical_tables.append(table_name)
        
        return sorted(canonical_tables)
        
    except Exception as e:
        print(f"Failed to discover canonical tables: {e}")
        return []


def load_canonical_mapping(canonical_table: str) -> Dict[str, Any]:
    """
    Load canonical mapping for a specific table.
    
    This function tries multiple sources in order of preference:
    1. Bundled mapping files (Lambda package)
    2. Local development mappings (relative path)
    3. S3 bucket (if BUCKET_NAME environment variable is set)
    4. Default fallback mappings
    
    Args:
        canonical_table: Canonical table name (e.g., 'companies')
        
    Returns:
        Canonical mapping dictionary
    """
    try:
        # Try bundled mapping files first (Lambda package deployment)
        bundled_mapping_path = os.path.join(
            os.path.dirname(__file__),
            'mappings', 'canonical',
            f'{canonical_table}.json'
        )
        
        if os.path.exists(bundled_mapping_path):
            with open(bundled_mapping_path, 'r') as f:
                return json.load(f)
        
        # Try local development mappings (relative path)
        local_mapping_path = os.path.join(
            os.path.dirname(__file__),
            '..', '..',
            'mappings', 'canonical',
            f'{canonical_table}.json'
        )
        
        if os.path.exists(local_mapping_path):
            with open(local_mapping_path, 'r') as f:
                return json.load(f)
        
        # Try S3 as fallback (if in Lambda environment with S3 access)
        bucket_name = os.environ.get('BUCKET_NAME')
        if bucket_name:
            try:
                import boto3
                s3 = boto3.client('s3')
                mapping_key = f"mappings/canonical/{canonical_table}.json"
                response = s3.get_object(Bucket=bucket_name, Key=mapping_key)
                return json.loads(response['Body'].read().decode('utf-8'))
            except Exception as s3_error:
                print(f"Failed to load from S3: {s3_error}")
        
        # Return empty dict if no mapping found
        print(f"No canonical mapping found for {canonical_table}")
        return {}
        
    except Exception as e:
        print(f"Failed to load canonical mapping for {canonical_table}: {e}")
        return {}


def get_canonical_table_for_endpoint(service_name: str, endpoint_path: str) -> Optional[str]:
    """
    Determine which canonical table an endpoint belongs to by scanning canonical mappings.
    
    Args:
        service_name: Service name (e.g., 'connectwise')
        endpoint_path: Endpoint path (e.g., 'service/tickets')
        
    Returns:
        Canonical table name or None if not found
    """
    try:
        # Get all canonical tables
        canonical_tables = discover_canonical_tables()
        
        # Check each canonical table to see if it contains this service/endpoint
        for canonical_table in canonical_tables:
            mapping = load_canonical_mapping(canonical_table)
            
            # Check if this service exists in the mapping
            if service_name in mapping:
                service_mapping = mapping[service_name]
                
                # Check if this endpoint exists in the service mapping
                if endpoint_path in service_mapping:
                    return canonical_table
        
        return None
        
    except Exception as e:
        print(f"Failed to get canonical table for {service_name}/{endpoint_path}: {e}")
        return None


def get_service_tables_for_canonical(canonical_table: str) -> Dict[str, List[str]]:
    """
    Get all service tables that contribute to a canonical table.
    
    Args:
        canonical_table: Canonical table name (e.g., 'companies')
        
    Returns:
        Dictionary mapping service names to lists of endpoint paths
    """
    try:
        mapping = load_canonical_mapping(canonical_table)
        
        service_tables = {}
        for service_name, service_mapping in mapping.items():
            if isinstance(service_mapping, dict):
                service_tables[service_name] = list(service_mapping.keys())
        
        return service_tables
        
    except Exception as e:
        print(f"Failed to get service tables for canonical table {canonical_table}: {e}")
        return {}


def build_service_table_configurations(service_name: str) -> List[Dict[str, Any]]:
    """
    Build table configurations for a service by combining endpoint and canonical mappings.
    
    Args:
        service_name: Service name (e.g., 'connectwise')
        
    Returns:
        List of table configurations
    """
    try:
        # Load endpoint configuration
        endpoint_config = load_endpoint_configuration(service_name)
        endpoints = endpoint_config.get('endpoints', {})
        
        # Load service configuration
        service_config = load_service_configuration(service_name)
        
        table_configs = []
        
        for endpoint_path, endpoint_data in endpoints.items():
            if not endpoint_data.get('enabled', True):
                continue
            
            # Get canonical table for this endpoint
            canonical_table = get_canonical_table_for_endpoint(service_name, endpoint_path)
            
            # Get table name from endpoint config or derive from canonical
            table_name = endpoint_data.get('table_name')
            if not table_name and canonical_table:
                table_name = canonical_table
            if not table_name:
                table_name = endpoint_path.split('/')[-1]
            
            table_config = {
                'table_name': table_name,
                'endpoint': endpoint_path,
                'canonical_table': canonical_table,
                'enabled': endpoint_data.get('enabled', True),
                'api_config': {
                    'page_size': endpoint_data.get('page_size', 1000),
                    'order_by': endpoint_data.get('order_by'),
                    'incremental_field': endpoint_data.get('incremental_field'),
                    'sync_frequency': endpoint_data.get('sync_frequency', '30min')
                },
                'processing_config': {
                    'chunk_size': endpoint_data.get('chunk_size', 5000)
                },
                'service_name': service_name
            }
            
            table_configs.append(table_config)
        
        return table_configs
        
    except Exception as e:
        print(f"Failed to build service table configurations for {service_name}: {e}")
        return []


def validate_tenant_config(config: Dict[str, Any]) -> TenantConfig:
    """
    Validate and parse tenant configuration.
    
    Args:
        config: Raw configuration dictionary
        
    Returns:
        Validated TenantConfig object
        
    Raises:
        ValueError: If configuration is invalid
    """
    try:
        return TenantConfig(**config)
    except Exception as e:
        raise ValueError(f"Invalid tenant configuration: {e}")


def chunk_list(data: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split list into chunks of specified size.
    
    Args:
        data: List to chunk
        chunk_size: Size of each chunk
        
    Returns:
        List of chunks
    """
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]


def safe_get(data: Dict[str, Any], key: str, default: Any = None) -> Any:
    """
    Safely get value from dictionary with dot notation support.
    
    Args:
        data: Dictionary to search
        key: Key to find (supports dot notation)
        default: Default value if key not found
        
    Returns:
        Value or default
    """
    try:
        keys = key.split('.')
        value = data
        for k in keys:
            value = value[k]
        return value
    except (KeyError, TypeError):
        return default


def normalize_datetime(dt_str: Optional[str]) -> Optional[str]:
    """
    Normalize datetime string to ISO format.
    
    Args:
        dt_str: Datetime string in various formats
        
    Returns:
        Normalized ISO datetime string or None
    """
    if not dt_str:
        return None
    
    try:
        # Try parsing common formats
        formats = [
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%m/%d/%Y %H:%M:%S',
            '%Y-%m-%d'
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(dt_str, fmt)
                # Ensure timezone awareness
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat().replace('+00:00', 'Z')
            except ValueError:
                continue
        
        # If no format matches, return original
        return dt_str
    except Exception:
        return dt_str


def calculate_data_freshness(last_updated: Optional[str]) -> Optional[int]:
    """
    Calculate data freshness in seconds.
    
    Args:
        last_updated: Last updated timestamp string
        
    Returns:
        Seconds since last update or None
    """
    if not last_updated:
        return None
    
    try:
        last_dt = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
        now = datetime.now(timezone.utc)
        return int((now - last_dt).total_seconds())
    except Exception:
        return None


def detect_schema_changes(old_df, new_df) -> Dict[str, List[str]]:
    """
    Detect schema changes between DataFrames.
    
    Args:
        old_df: Previous DataFrame
        new_df: New DataFrame
        
    Returns:
        Dictionary with added and removed columns
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas is required for schema change detection. Ensure AWS pandas layer is available.")
    
    old_columns = set(old_df.columns)
    new_columns = set(new_df.columns)
    
    return {
        'added_columns': list(new_columns - old_columns),
        'removed_columns': list(old_columns - new_columns)
    }


def validate_data_quality(df, table_name: str) -> Dict[str, Any]:
    """
    Perform basic data quality checks.
    
    Args:
        df: DataFrame to validate
        table_name: Name of the table
        
    Returns:
        Data quality report
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas is required for data quality validation. Ensure AWS pandas layer is available.")
    
    report = {
        'table_name': table_name,
        'record_count': len(df),
        'column_count': len(df.columns),
        'null_counts': df.isnull().sum().to_dict(),
        'duplicate_count': df.duplicated().sum(),
        'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
    }
    
    # Check for required fields
    if 'id' in df.columns:
        report['unique_ids'] = df['id'].nunique()
        report['null_ids'] = df['id'].isnull().sum()
    
    return report