"""
Simple configuration management without pydantic dependencies.
"""

import os
import json
from typing import Optional, Dict, List, Any


class Config:
    """Configuration settings for the data pipeline."""
    
    def __init__(self, **kwargs):
        # AWS Configuration
        self.bucket_name = kwargs.get('bucket_name')
        self.tenant_services_table = kwargs.get('tenant_services_table')
        self.last_updated_table = kwargs.get('last_updated_table')
        self.environment = kwargs.get('environment', 'dev')
        
        # Lambda Configuration
        self.max_records_per_batch = kwargs.get('max_records_per_batch', 1000)
        self.max_execution_time = kwargs.get('max_execution_time', 840)
        
        # ConnectWise API Configuration
        self.api_timeout = kwargs.get('api_timeout', 30)
        self.max_retries = kwargs.get('max_retries', 3)
        self.page_size = kwargs.get('page_size', 1000)
        
        # Data Quality Configuration
        self.max_record_age_hours = kwargs.get('max_record_age_hours', 24)
        self.enable_data_validation = kwargs.get('enable_data_validation', True)
    
    @classmethod
    def from_environment(cls) -> "Config":
        """Create configuration from environment variables."""
        return cls(
            bucket_name=os.environ["BUCKET_NAME"],
            tenant_services_table=os.environ["TENANT_SERVICES_TABLE"],
            last_updated_table=os.environ["LAST_UPDATED_TABLE"],
            environment=os.environ.get("ENVIRONMENT", "dev"),
            max_records_per_batch=int(os.environ.get("MAX_RECORDS_PER_BATCH", "1000")),
            max_execution_time=int(os.environ.get("MAX_EXECUTION_TIME", "840")),
            api_timeout=int(os.environ.get("API_TIMEOUT", "30")),
            max_retries=int(os.environ.get("MAX_RETRIES", "3")),
            page_size=int(os.environ.get("PAGE_SIZE", "1000")),
            max_record_age_hours=int(os.environ.get("MAX_RECORD_AGE_HOURS", "24")),
            enable_data_validation=os.environ.get("ENABLE_DATA_VALIDATION", "true").lower() == "true"
        )


class TenantConfig:
    """Configuration for a specific tenant and service."""
    
    def __init__(self, **kwargs):
        self.tenant_id = kwargs.get('tenant_id')
        self.connectwise_url = kwargs.get('connectwise_url')
        self.secret_name = kwargs.get('secret_name')
        self.enabled = kwargs.get('enabled', True)
        self.tables = kwargs.get('tables', [])
        self.custom_config = kwargs.get('custom_config')
    
    def get_api_url(self, endpoint: str) -> str:
        """Get full API URL for an endpoint."""
        base_url = self.connectwise_url.rstrip('/')
        endpoint = endpoint.lstrip('/')
        return f"{base_url}/{endpoint}"


class ServiceConfig:
    """Configuration for a specific service within a tenant."""
    
    def __init__(self, **kwargs):
        self.service_name = kwargs.get('service_name')
        self.enabled = kwargs.get('enabled', True)
        self.api_url = kwargs.get('api_url')
        self.tables = kwargs.get('tables', [])
        self.custom_config = kwargs.get('custom_config')
    
    def get_api_url(self, endpoint: str) -> str:
        """Get full API URL for an endpoint."""
        base_url = self.api_url.rstrip('/')
        endpoint = endpoint.lstrip('/')
        return f"{base_url}/{endpoint}"


class ConnectWiseCredentials:
    """ConnectWise API credentials."""
    
    def __init__(self, **kwargs):
        self.company_id = kwargs.get('company_id')
        self.public_key = kwargs.get('public_key')
        self.private_key = kwargs.get('private_key')
        self.client_id = kwargs.get('client_id')
        self.api_base_url = kwargs.get('api_base_url')
    
    def get_auth_header(self) -> str:
        """Get authorization header value."""
        import base64
        auth_string = f"{self.company_id}+{self.public_key}:{self.private_key}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        return f"Basic {encoded_auth}"
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConnectWiseCredentials":
        """Create credentials from dictionary."""
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> "ConnectWiseCredentials":
        """Create credentials from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)