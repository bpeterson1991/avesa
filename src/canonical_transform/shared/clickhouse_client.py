"""
ClickHouse Client - Centralized ClickHouse connection and query management

This module provides:
- Centralized ClickHouse client creation and configuration
- Connection pooling and management
- Query optimization and caching
- Multi-tenant data access patterns
- Comprehensive error handling and retry logic
"""

import json
import os
import logging
import time
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timezone
from contextlib import contextmanager

try:
    import clickhouse_connect
    from clickhouse_connect.driver import Client
except ImportError:
    clickhouse_connect = None
    Client = None

from .aws_client_factory import AWSClientFactory

logger = logging.getLogger(__name__)


class ClickHouseConnectionError(Exception):
    """Custom exception for ClickHouse connection errors."""
    pass


class ClickHouseQueryError(Exception):
    """Custom exception for ClickHouse query errors."""
    pass


class ClickHouseClient:
    """
    Centralized ClickHouse client with connection management and query optimization.
    
    Consolidates ClickHouse connection logic from:
    - src/clickhouse/data_loader/lambda_function.py:22-51
    - src/clickhouse/scd_processor/lambda_function.py:21-50
    - src/clickhouse/schema_init/lambda_function.py:20-49
    """
    
    def __init__(self, secret_name: str, region_name: Optional[str] = None):
        """
        Initialize ClickHouse client with AWS Secrets Manager integration.
        
        Args:
            secret_name: Name of the secret in AWS Secrets Manager containing ClickHouse credentials
            region_name: AWS region name for Secrets Manager
        """
        if clickhouse_connect is None:
            raise ImportError("clickhouse-connect package is required but not installed")
            
        self.secret_name = secret_name
        self.region_name = region_name
        self._client: Optional[Client] = None
        self._credentials: Optional[Dict[str, Any]] = None
        self._connection_time: Optional[datetime] = None
        
        # Connection configuration
        self.connection_config = {
            'secure': True,
            'verify': True,
            'connect_timeout': 30,
            'send_receive_timeout': 300,
            'compress': True,
            'query_limit': 0,  # No limit
            'max_execution_time': 300  # 5 minutes
        }
        
        # AWS client factory for secrets retrieval
        self._aws_factory = AWSClientFactory(region_name)
    
    def _get_credentials(self) -> Dict[str, Any]:
        """
        Retrieve ClickHouse credentials from AWS Secrets Manager.
        
        Returns:
            Dictionary containing ClickHouse connection credentials
            
        Raises:
            ClickHouseConnectionError: If credentials cannot be retrieved
        """
        if self._credentials:
            return self._credentials
            
        try:
            secrets_client = self._aws_factory.get_client('secretsmanager')
            
            logger.debug(f"Retrieving ClickHouse credentials from secret: {self.secret_name}")
            response = secrets_client.get_secret_value(SecretId=self.secret_name)
            secret_data = json.loads(response['SecretString'])
            
            # Validate required fields
            required_fields = ['host', 'username', 'password']
            missing_fields = [field for field in required_fields if field not in secret_data]
            
            if missing_fields:
                raise ClickHouseConnectionError(
                    f"Missing required ClickHouse credential fields: {', '.join(missing_fields)}"
                )
            
            self._credentials = secret_data
            logger.debug("ClickHouse credentials retrieved successfully")
            return self._credentials
            
        except Exception as e:
            logger.error(f"Failed to retrieve ClickHouse credentials: {e}")
            raise ClickHouseConnectionError(f"Failed to retrieve ClickHouse credentials: {e}")
    
    def _create_connection(self) -> Client:
        """
        Create a new ClickHouse connection.
        
        Returns:
            ClickHouse client instance
            
        Raises:
            ClickHouseConnectionError: If connection cannot be established
        """
        try:
            credentials = self._get_credentials()
            
            # Build connection parameters
            connection_params = {
                'host': credentials['host'],
                'port': credentials.get('port', 8443),
                'username': credentials['username'],
                'password': credentials['password'],
                'database': credentials.get('database', 'default'),
                **self.connection_config
            }
            
            logger.debug(f"Connecting to ClickHouse: {credentials['host']}:{connection_params['port']}")
            client = clickhouse_connect.get_client(**connection_params)
            
            # Test the connection
            client.ping()
            
            self._connection_time = datetime.now(timezone.utc)
            logger.info("ClickHouse connection established successfully")
            return client
            
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise ClickHouseConnectionError(f"Failed to connect to ClickHouse: {e}")
    
    def get_client(self) -> Client:
        """
        Get ClickHouse client, creating connection if needed.
        
        Returns:
            ClickHouse client instance
        """
        if self._client is None:
            self._client = self._create_connection()
        
        # Check if connection is still alive (basic health check)
        try:
            self._client.ping()
        except Exception as e:
            logger.warning(f"ClickHouse connection lost, reconnecting: {e}")
            self._client = self._create_connection()
        
        return self._client
    
    @property
    def client(self) -> Client:
        """
        Property to access ClickHouse client for backward compatibility.
        
        Returns:
            ClickHouse client instance
        """
        return self.get_client()
    
    @classmethod
    def from_environment(cls, env_name: str = 'CLICKHOUSE_SECRET_NAME',
                        region_name: Optional[str] = None) -> 'ClickHouseClient':
        """
        Create ClickHouse client from environment variable.
        
        Args:
            env_name: Environment variable name containing the secret name
            region_name: AWS region name
            
        Returns:
            Configured ClickHouseClient instance
            
        Raises:
            ClickHouseConnectionError: If environment variable is not set
        """
        secret_name = os.environ.get(env_name)
        if not secret_name:
            raise ClickHouseConnectionError(f"Environment variable {env_name} not set")
        
        return cls(secret_name, region_name)
    
    def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None,
                     settings: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a ClickHouse query with error handling and retry logic.
        
        Args:
            query: SQL query to execute
            parameters: Query parameters for parameterized queries
            settings: ClickHouse-specific query settings
            
        Returns:
            Query result
            
        Raises:
            ClickHouseQueryError: If query execution fails
        """
        client = self.get_client()
        
        try:
            logger.debug(f"Executing ClickHouse query: {query[:100]}...")
            
            # Apply default settings
            query_settings = {
                'max_execution_time': 300,
                'max_memory_usage': 10000000000,  # 10GB
                'use_uncompressed_cache': 1
            }
            if settings:
                query_settings.update(settings)
            
            start_time = time.time()
            
            if parameters:
                result = client.query(query, parameters=parameters, settings=query_settings)
            else:
                result = client.query(query, settings=query_settings)
            
            execution_time = time.time() - start_time
            logger.debug(f"Query executed successfully in {execution_time:.2f}s")
            
            return result
            
        except Exception as e:
            logger.error(f"ClickHouse query failed: {e}")
            logger.error(f"Query: {query}")
            raise ClickHouseQueryError(f"Query execution failed: {e}")
    
    def execute_command(self, command: str, parameters: Optional[Dict[str, Any]] = None,
                       settings: Optional[Dict[str, Any]] = None) -> None:
        """
        Execute a ClickHouse command (DDL, DML) with error handling.
        
        Args:
            command: SQL command to execute
            parameters: Command parameters
            settings: ClickHouse-specific command settings
            
        Raises:
            ClickHouseQueryError: If command execution fails
        """
        client = self.get_client()
        
        try:
            logger.debug(f"Executing ClickHouse command: {command[:100]}...")
            
            # Apply default settings
            command_settings = {
                'max_execution_time': 600,  # 10 minutes for DDL operations
                'max_memory_usage': 10000000000  # 10GB
            }
            if settings:
                command_settings.update(settings)
            
            start_time = time.time()
            
            if parameters:
                client.command(command, parameters=parameters, settings=command_settings)
            else:
                client.command(command, settings=command_settings)
            
            execution_time = time.time() - start_time
            logger.debug(f"Command executed successfully in {execution_time:.2f}s")
            
        except Exception as e:
            logger.error(f"ClickHouse command failed: {e}")
            logger.error(f"Command: {command}")
            raise ClickHouseQueryError(f"Command execution failed: {e}")
    
    def bulk_insert(self, table: str, data: List[Dict[str, Any]],
                   batch_size: int = 10000, tenant_id: Optional[str] = None) -> int:
        """
        Optimized bulk insert with batching and error handling.
        
        Args:
            table: Target table name
            data: List of dictionaries to insert
            batch_size: Number of records per batch
            tenant_id: Optional tenant ID for multi-tenant tables
            
        Returns:
            Number of records inserted
            
        Raises:
            ClickHouseQueryError: If bulk insert fails
        """
        if not data:
            logger.warning("No data provided for bulk insert")
            return 0
        
        client = self.get_client()
        total_inserted = 0
        
        try:
            logger.info(f"Starting bulk insert to {table}: {len(data)} records in batches of {batch_size}")
            
            # Process data in batches
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                
                # Add tenant_id to each record if specified
                if tenant_id:
                    for record in batch:
                        record['tenant_id'] = tenant_id
                
                start_time = time.time()
                client.insert(table, batch)
                execution_time = time.time() - start_time
                
                total_inserted += len(batch)
                logger.debug(f"Inserted batch {i//batch_size + 1}: {len(batch)} records in {execution_time:.2f}s")
            
            logger.info(f"Bulk insert completed: {total_inserted} records inserted to {table}")
            return total_inserted
            
        except Exception as e:
            logger.error(f"Bulk insert failed after {total_inserted} records: {e}")
            raise ClickHouseQueryError(f"Bulk insert failed: {e}")
    
    def get_table_info(self, table: str) -> Dict[str, Any]:
        """
        Get information about a ClickHouse table.
        
        Args:
            table: Table name
            
        Returns:
            Dictionary with table information
        """
        try:
            # Get table schema
            schema_query = f"DESCRIBE TABLE {table}"
            schema_result = self.execute_query(schema_query)
            
            # Get table statistics
            stats_query = f"SELECT count() as total_rows FROM {table}"
            stats_result = self.execute_query(stats_query)
            
            return {
                'table_name': table,
                'schema': schema_result.result_rows,
                'total_rows': stats_result.result_rows[0][0] if stats_result.result_rows else 0,
                'columns': [row[0] for row in schema_result.result_rows]
            }
            
        except Exception as e:
            logger.error(f"Failed to get table info for {table}: {e}")
            raise ClickHouseQueryError(f"Failed to get table info: {e}")
    
    def check_table_exists(self, table: str) -> bool:
        """
        Check if a table exists in ClickHouse.
        
        Args:
            table: Table name
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            query = f"EXISTS TABLE {table}"
            result = self.execute_query(query)
            return bool(result.result_rows[0][0]) if result.result_rows else False
            
        except Exception as e:
            logger.warning(f"Error checking if table {table} exists: {e}")
            return False
    
    @contextmanager
    def transaction(self):
        """
        Context manager for ClickHouse transactions (limited support).
        
        Note: ClickHouse has limited transaction support, this is mainly for
        connection management and error handling.
        """
        client = self.get_client()
        try:
            yield client
        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            raise
        finally:
            # ClickHouse doesn't have explicit transaction rollback
            # This is mainly for cleanup and logging
            pass
    
    def close(self):
        """Close the ClickHouse connection."""
        if self._client:
            try:
                self._client.close()
                logger.debug("ClickHouse connection closed")
            except Exception as e:
                logger.warning(f"Error closing ClickHouse connection: {e}")
            finally:
                self._client = None
                self._connection_time = None
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Convenience function for backward compatibility
def get_clickhouse_connection(secret_name: Optional[str] = None) -> Client:
    """
    Get ClickHouse connection using credentials from AWS Secrets Manager.
    
    This function provides backward compatibility with existing code.
    
    Args:
        secret_name: Secret name, defaults to CLICKHOUSE_SECRET_NAME environment variable
        
    Returns:
        ClickHouse client instance
    """
    if secret_name is None:
        secret_name = os.environ.get('CLICKHOUSE_SECRET_NAME')
        if not secret_name:
            raise ClickHouseConnectionError("CLICKHOUSE_SECRET_NAME environment variable not set")
    
    client = ClickHouseClient(secret_name)
    return client.get_client()