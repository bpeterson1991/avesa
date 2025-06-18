"""
Utility functions for the ConnectWise data pipeline.
"""

import json
import pandas as pd
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
from .config import TenantConfig


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
        service: Service name (connectwise)
        table_name: Table name
        timestamp: Timestamp string
        
    Returns:
        S3 key path
    """
    return f"{tenant_id}/{data_type}/{service}/{table_name}/{timestamp}.parquet"


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


def detect_schema_changes(old_df: pd.DataFrame, new_df: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Detect schema changes between DataFrames.
    
    Args:
        old_df: Previous DataFrame
        new_df: New DataFrame
        
    Returns:
        Dictionary with added and removed columns
    """
    old_columns = set(old_df.columns)
    new_columns = set(new_df.columns)
    
    return {
        'added_columns': list(new_columns - old_columns),
        'removed_columns': list(old_columns - new_columns)
    }


def validate_data_quality(df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
    """
    Perform basic data quality checks.
    
    Args:
        df: DataFrame to validate
        table_name: Name of the table
        
    Returns:
        Data quality report
    """
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