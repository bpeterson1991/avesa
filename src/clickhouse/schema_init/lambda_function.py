"""
ClickHouse Schema Initialization Lambda Function

This function initializes the ClickHouse database schema for multi-tenant SaaS.
It creates the shared tables with tenant_id partitioning and sets up indexes.
"""

import json
import os
import logging
from typing import Dict, Any

# Import shared components
from shared import ClickHouseClient

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def read_schema_file() -> str:
    """Read the schema SQL file."""
    schema_path = os.path.join(os.path.dirname(__file__), 'shared_tables.sql')
    try:
        with open(schema_path, 'r') as f:
            return f.read()
    except FileNotFoundError:
        # If running in Lambda, the schema might be in a different location
        # Try to read from the deployment package
        import pkgutil
        schema_content = pkgutil.get_data(__package__, 'shared_tables.sql')
        if schema_content:
            return schema_content.decode('utf-8')
        else:
            raise Exception("Schema file not found in package")

def execute_schema_statements(client, schema_sql: str) -> Dict[str, Any]:
    """Execute schema creation statements."""
    results = {
        'tables_created': [],
        'indexes_created': [],
        'views_created': [],
        'errors': []
    }
    
    # Split SQL into individual statements
    statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
    
    for statement in statements:
        if not statement or statement.startswith('--'):
            continue
            
        try:
            logger.info(f"Executing statement: {statement[:100]}...")
            client.command(statement)
            
            # Categorize the statement
            if 'CREATE TABLE' in statement.upper():
                table_name = extract_table_name(statement)
                results['tables_created'].append(table_name)
                logger.info(f"Created table: {table_name}")
                
            elif 'ADD INDEX' in statement.upper():
                index_info = extract_index_info(statement)
                results['indexes_created'].append(index_info)
                logger.info(f"Created index: {index_info}")
                
            elif 'CREATE MATERIALIZED VIEW' in statement.upper():
                view_name = extract_view_name(statement)
                results['views_created'].append(view_name)
                logger.info(f"Created materialized view: {view_name}")
                
        except Exception as e:
            error_msg = f"Failed to execute statement: {str(e)}"
            logger.error(error_msg)
            results['errors'].append({
                'statement': statement[:200],
                'error': str(e)
            })
    
    return results

def extract_table_name(statement: str) -> str:
    """Extract table name from CREATE TABLE statement."""
    try:
        parts = statement.upper().split()
        if_not_exists_idx = -1
        for i, part in enumerate(parts):
            if part == 'EXISTS':
                if_not_exists_idx = i
                break
        
        if if_not_exists_idx > 0:
            return parts[if_not_exists_idx + 1]
        else:
            table_idx = parts.index('TABLE') + 1
            return parts[table_idx]
    except:
        return "unknown"

def extract_index_info(statement: str) -> str:
    """Extract index information from ALTER TABLE ADD INDEX statement."""
    try:
        parts = statement.split()
        table_name = parts[2]  # ALTER TABLE table_name
        index_name = parts[5]  # ADD INDEX index_name
        return f"{index_name} on {table_name}"
    except:
        return "unknown"

def extract_view_name(statement: str) -> str:
    """Extract view name from CREATE MATERIALIZED VIEW statement."""
    try:
        parts = statement.upper().split()
        if_not_exists_idx = -1
        for i, part in enumerate(parts):
            if part == 'EXISTS':
                if_not_exists_idx = i
                break
        
        if if_not_exists_idx > 0:
            return parts[if_not_exists_idx + 1]
        else:
            view_idx = parts.index('VIEW') + 1
            return parts[view_idx]
    except:
        return "unknown"

def verify_schema(client) -> Dict[str, Any]:
    """Verify that the schema was created successfully."""
    verification_results = {
        'tables': {},
        'total_tables': 0,
        'total_indexes': 0,
        'total_views': 0
    }
    
    try:
        # Check tables
        tables_query = "SHOW TABLES"
        tables = client.query(tables_query).result_rows
        
        expected_tables = ['companies', 'contacts', 'tickets', 'time_entries']
        for table_name in expected_tables:
            table_exists = any(table[0] == table_name for table in tables)
            verification_results['tables'][table_name] = table_exists
            if table_exists:
                verification_results['total_tables'] += 1
        
        # Check materialized views
        views_query = "SHOW TABLES WHERE engine LIKE '%MaterializedView%'"
        views = client.query(views_query).result_rows
        verification_results['total_views'] = len(views)
        
        logger.info(f"Schema verification: {verification_results}")
        
    except Exception as e:
        logger.error(f"Schema verification failed: {e}")
        verification_results['error'] = str(e)
    
    return verification_results

def lambda_handler(event, context):
    """
    Lambda handler for ClickHouse schema initialization.
    
    Args:
        event: Lambda event (can contain schema configuration)
        context: Lambda context
        
    Returns:
        Dict with initialization results
    """
    logger.info(f"Starting ClickHouse schema initialization")
    logger.info(f"Event: {json.dumps(event, default=str)}")
    
    try:
        # Get environment configuration using proper pattern
        from shared.environment import Environment
        env_name = os.environ.get('ENVIRONMENT', 'dev')
        config = Environment.get_config(env_name)
        
        # Get ClickHouse connection using shared client
        client = ClickHouseClient.from_environment(os.environ.get('ENVIRONMENT', 'dev'))
        logger.info("Successfully connected to ClickHouse")
        
        # Read schema file
        schema_sql = read_schema_file()
        logger.info(f"Loaded schema file ({len(schema_sql)} characters)")
        
        # Execute schema statements
        execution_results = execute_schema_statements(client, schema_sql)
        logger.info(f"Schema execution results: {execution_results}")
        
        # Verify schema
        verification_results = verify_schema(client)
        logger.info(f"Schema verification results: {verification_results}")
        
        # Prepare response
        response = {
            'statusCode': 200,
            'body': {
                'message': 'ClickHouse schema initialization completed',
                'execution_results': execution_results,
                'verification_results': verification_results,
                'environment': os.environ.get('ENVIRONMENT', 'unknown')
            }
        }
        
        # Check for errors
        if execution_results['errors']:
            response['statusCode'] = 207  # Partial success
            response['body']['message'] = 'Schema initialization completed with some errors'
        
        logger.info(f"Schema initialization completed successfully")
        return response
        
    except Exception as e:
        logger.error(f"Schema initialization failed: {e}")
        return {
            'statusCode': 500,
            'body': {
                'message': 'ClickHouse schema initialization failed',
                'error': str(e),
                'environment': os.environ.get('ENVIRONMENT', 'unknown')
            }
        }
    
    finally:
        # Close connection if it exists
        try:
            if 'client' in locals():
                client.close()
        except:
            pass