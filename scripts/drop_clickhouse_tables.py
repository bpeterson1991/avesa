#!/usr/bin/env python3
"""
Drop ClickHouse tables for schema rebuild

This script drops the canonical tables in ClickHouse to allow them to be
rebuilt with updated schema based on the new mapping files.
"""

import json
import logging
import sys
import boto3
from clickhouse_connect import get_client

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_clickhouse_credentials():
    """Get ClickHouse credentials from AWS Secrets Manager"""
    try:
        secrets_manager = boto3.client('secretsmanager', region_name='us-east-2')
        secret_name = 'clickhouse-connection-dev'
        
        logger.info(f"Fetching ClickHouse credentials from secret: {secret_name}")
        
        response = secrets_manager.get_secret_value(SecretId=secret_name)
        credentials = json.loads(response['SecretString'])
        
        return credentials
    except Exception as e:
        logger.error(f"Failed to get ClickHouse credentials: {e}")
        raise

def drop_tables(client, tables):
    """Drop specified tables from ClickHouse"""
    dropped_tables = []
    failed_tables = []
    
    for table in tables:
        try:
            logger.info(f"Checking if table '{table}' exists...")
            
            # Check if table exists
            result = client.query(f"EXISTS TABLE {table}")
            exists = result.result_rows[0][0] == 1
            
            if exists:
                logger.info(f"Dropping table '{table}'...")
                client.command(f"DROP TABLE IF EXISTS {table}")
                logger.info(f"Successfully dropped table '{table}'")
                dropped_tables.append(table)
            else:
                logger.info(f"Table '{table}' does not exist, skipping...")
                
        except Exception as e:
            logger.error(f"Failed to drop table '{table}': {e}")
            failed_tables.append((table, str(e)))
    
    return dropped_tables, failed_tables

def main():
    """Main function to drop ClickHouse tables"""
    # Tables to drop based on canonical mappings
    tables_to_drop = [
        'companies',
        'contacts', 
        'tickets',
        'time_entries'
    ]
    
    try:
        # Get ClickHouse credentials
        credentials = get_clickhouse_credentials()
        
        # Connect to ClickHouse
        logger.info("Connecting to ClickHouse...")
        # ClickHouse Cloud typically uses port 8443 for HTTPS
        port = credentials.get('port', 8443)
        # Don't include protocol in host for clickhouse_connect
        host = credentials['host']
        
        client = get_client(
            host=host,
            port=port,
            username=credentials['username'],
            password=credentials['password'],
            database=credentials.get('database', 'default'),
            secure=True,  # Use HTTPS for ClickHouse Cloud
            verify=False,  # Skip SSL verification for development
            settings={
                'readonly': 0  # Allow write operations
            }
        )
        
        # Test connection
        logger.info("Testing ClickHouse connection...")
        version = client.query("SELECT version()").result_rows[0][0]
        logger.info(f"Connected to ClickHouse version: {version}")
        
        # Drop tables
        logger.info(f"Dropping {len(tables_to_drop)} tables...")
        dropped_tables, failed_tables = drop_tables(client, tables_to_drop)
        
        # Summary
        logger.info("\n" + "="*50)
        logger.info("SUMMARY")
        logger.info("="*50)
        
        if dropped_tables:
            logger.info(f"Successfully dropped {len(dropped_tables)} tables:")
            for table in dropped_tables:
                logger.info(f"  ✓ {table}")
        
        if failed_tables:
            logger.error(f"\nFailed to drop {len(failed_tables)} tables:")
            for table, error in failed_tables:
                logger.error(f"  ✗ {table}: {error}")
        
        if not dropped_tables and not failed_tables:
            logger.info("No tables were dropped (all tables already absent)")
        
        # Close connection
        client.close()
        
        # Exit with appropriate code
        if failed_tables:
            sys.exit(1)
        else:
            logger.info("\nAll operations completed successfully!")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Script failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()