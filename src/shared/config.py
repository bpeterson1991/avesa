"""
Configuration management for the ConnectWise data pipeline.
"""

import os
from typing import Optional
from pydantic import BaseModel, Field


class Config(BaseModel):
    """Configuration settings for the data pipeline."""
    
    # AWS Configuration
    bucket_name: str = Field(..., description="S3 bucket name for data storage")
    tenant_services_table: str = Field(..., description="DynamoDB table for tenant services")
    last_updated_table: str = Field(..., description="DynamoDB table for last updated tracking")
    environment: str = Field(default="dev", description="Environment (dev/staging/prod)")
    
    # Lambda Configuration
    max_records_per_batch: int = Field(default=1000, description="Maximum records per batch")
    max_execution_time: int = Field(default=840, description="Maximum execution time in seconds")
    
    # ConnectWise API Configuration
    api_timeout: int = Field(default=30, description="API request timeout in seconds")
    max_retries: int = Field(default=3, description="Maximum API retry attempts")
    page_size: int = Field(default=1000, description="API pagination page size")
    
    # Data Quality Configuration
    max_record_age_hours: int = Field(default=24, description="Maximum age for records in hours")
    enable_data_validation: bool = Field(default=True, description="Enable data validation")
    
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


class TenantConfig(BaseModel):
    """Configuration for a specific tenant."""
    
    tenant_id: str = Field(..., description="Unique tenant identifier")
    connectwise_url: str = Field(..., description="ConnectWise API base URL")
    secret_name: str = Field(..., description="AWS Secrets Manager secret name")
    enabled: bool = Field(default=True, description="Whether tenant is enabled")
    tables: list[str] = Field(default_factory=list, description="List of tables to sync")
    custom_config: Optional[dict] = Field(default=None, description="Custom tenant configuration")
    
    def get_api_url(self, endpoint: str) -> str:
        """Get full API URL for an endpoint."""
        base_url = self.connectwise_url.rstrip('/')
        endpoint = endpoint.lstrip('/')
        return f"{base_url}/{endpoint}"


class ConnectWiseCredentials(BaseModel):
    """ConnectWise API credentials."""
    
    company_id: str = Field(..., description="ConnectWise company ID")
    public_key: str = Field(..., description="ConnectWise public key")
    private_key: str = Field(..., description="ConnectWise private key")
    client_id: str = Field(..., description="ConnectWise client ID")
    
    def get_auth_header(self) -> str:
        """Get authorization header value."""
        import base64
        auth_string = f"{self.company_id}+{self.public_key}:{self.private_key}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        return f"Basic {encoded_auth}"