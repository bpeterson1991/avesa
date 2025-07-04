"""
ClickHouse SCD Type 2 Processor Lambda Function

This function handles SCD Type 2 processing for ClickHouse tables,
managing historical data versioning and ensuring data integrity.
Only processes tables configured with SCD Type 2.
"""

import json
import os
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

# Import shared components
from shared import ClickHouseClient
from shared.scd_config import SCDConfigManager, get_scd_type, is_scd_type_2, filter_tables_by_scd_type

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def cleanup_expired_records(client, table_name: str, retention_days: int = 2555) -> Dict[str, Any]:
    """Clean up very old expired records to manage storage."""
    try:
        cutoff_date = datetime.now(timezone.utc).replace(day=1)  # Keep at least current month
        cutoff_date = cutoff_date.replace(year=cutoff_date.year - 7)  # 7 years retention
        
        # Count records to be deleted
        count_query = f"""
        SELECT count() as expired_count
        FROM {table_name}
        WHERE is_current = false
        AND expiration_date < '{cutoff_date.isoformat()}'
        """
        
        expired_count = client.execute_query(count_query).result_rows[0][0]
        
        if expired_count > 0:
            # Delete expired records
            delete_query = f"""
            ALTER TABLE {table_name}
            DELETE WHERE is_current = false
            AND expiration_date < '{cutoff_date.isoformat()}'
            """
            
            client.execute_command(delete_query)
            logger.info(f"Deleted {expired_count} expired records from {table_name}")
        
        return {
            'table': table_name,
            'expired_records_deleted': expired_count,
            'cutoff_date': cutoff_date.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to cleanup expired records: {e}")
        return {
            'table': table_name,
            'status': 'error',
            'error': str(e)
        }

def validate_scd_integrity(client, table_name: str) -> Dict[str, Any]:
    """Validate SCD Type 2 data integrity."""
    try:
        validation_results = {}
        
        # Check for overlapping current records
        overlap_query = f"""
        SELECT tenant_id, id, count() as duplicate_current
        FROM {table_name}
        WHERE is_current = true
        GROUP BY tenant_id, id
        HAVING count() > 1
        """
        
        overlaps = client.execute_query(overlap_query).result_rows
        validation_results['overlapping_current_records'] = len(overlaps)
        
        # Check for records without proper expiration
        orphan_query = f"""
        SELECT count() as orphan_count
        FROM {table_name}
        WHERE is_current = false
        AND expiration_date IS NULL
        """
        
        orphan_count = client.execute_query(orphan_query).result_rows[0][0]
        validation_results['orphaned_records'] = orphan_count
        
        # Check for future effective dates
        future_query = f"""
        SELECT count() as future_count
        FROM {table_name}
        WHERE effective_date > now()
        """
        
        future_count = client.execute_query(future_query).result_rows[0][0]
        validation_results['future_effective_dates'] = future_count
        
        # Check for invalid date ranges
        invalid_range_query = f"""
        SELECT count() as invalid_range_count
        FROM {table_name}
        WHERE expiration_date IS NOT NULL
        AND expiration_date <= effective_date
        """
        
        invalid_range_count = client.execute_query(invalid_range_query).result_rows[0][0]
        validation_results['invalid_date_ranges'] = invalid_range_count
        
        # Calculate overall health score
        total_issues = (validation_results['overlapping_current_records'] + 
                       validation_results['orphaned_records'] + 
                       validation_results['future_effective_dates'] + 
                       validation_results['invalid_date_ranges'])
        
        validation_results['total_issues'] = total_issues
        validation_results['health_status'] = 'healthy' if total_issues == 0 else 'issues_found'
        
        logger.info(f"SCD integrity validation for {table_name}: {validation_results}")
        return {
            'table': table_name,
            'validation_results': validation_results
        }
        
    except Exception as e:
        logger.error(f"Failed to validate SCD integrity: {e}")
        return {
            'table': table_name,
            'status': 'error',
            'error': str(e)
        }

def fix_scd_issues(client, table_name: str) -> Dict[str, Any]:
    """Fix common SCD Type 2 issues."""
    try:
        fixes_applied = []
        
        # Fix orphaned records (set expiration date)
        current_time = datetime.now(timezone.utc)
        orphan_fix_query = f"""
        ALTER TABLE {table_name}
        UPDATE expiration_date = '{current_time.isoformat()}'
        WHERE is_current = false
        AND expiration_date IS NULL
        """
        
        client.execute_command(orphan_fix_query)
        fixes_applied.append('fixed_orphaned_records')
        
        # Fix overlapping current records (keep the latest one)
        overlap_fix_query = f"""
        ALTER TABLE {table_name}
        UPDATE 
            is_current = false,
            expiration_date = '{current_time.isoformat()}'
        WHERE (tenant_id, id, effective_date) IN (
            SELECT tenant_id, id, min(effective_date)
            FROM {table_name}
            WHERE is_current = true
            GROUP BY tenant_id, id
            HAVING count() > 1
        )
        """
        
        client.execute_command(overlap_fix_query)
        fixes_applied.append('fixed_overlapping_current_records')
        
        logger.info(f"Applied SCD fixes for {table_name}: {fixes_applied}")
        return {
            'table': table_name,
            'fixes_applied': fixes_applied,
            'status': 'success'
        }
        
    except Exception as e:
        logger.error(f"Failed to fix SCD issues: {e}")
        return {
            'table': table_name,
            'status': 'error',
            'error': str(e)
        }

def generate_scd_statistics(client, table_name: str) -> Dict[str, Any]:
    """Generate statistics about SCD Type 2 data."""
    try:
        stats = {}
        
        # Total records
        total_query = f"SELECT count() as total FROM {table_name}"
        stats['total_records'] = client.execute_query(total_query).result_rows[0][0]
        
        # Current records
        current_query = f"SELECT count() as current FROM {table_name} WHERE is_current = true"
        stats['current_records'] = client.execute_query(current_query).result_rows[0][0]
        
        # Historical records
        stats['historical_records'] = stats['total_records'] - stats['current_records']
        
        # Records by tenant
        tenant_query = f"""
        SELECT tenant_id, count() as records, countIf(is_current = true) as current_records
        FROM {table_name}
        GROUP BY tenant_id
        ORDER BY records DESC
        """
        
        tenant_stats = client.execute_query(tenant_query).result_rows
        stats['tenant_breakdown'] = [
            {
                'tenant_id': row[0],
                'total_records': row[1],
                'current_records': row[2],
                'historical_records': row[1] - row[2]
            }
            for row in tenant_stats
        ]
        
        # Version distribution
        version_query = f"""
        SELECT record_version, count() as count
        FROM {table_name}
        WHERE is_current = true
        GROUP BY record_version
        ORDER BY record_version
        """
        
        version_stats = client.execute_query(version_query).result_rows
        stats['version_distribution'] = [
            {'version': row[0], 'count': row[1]}
            for row in version_stats
        ]
        
        logger.info(f"Generated SCD statistics for {table_name}")
        return {
            'table': table_name,
            'statistics': stats
        }
        
    except Exception as e:
        logger.error(f"Failed to generate SCD statistics: {e}")
        return {
            'table': table_name,
            'status': 'error',
            'error': str(e)
        }

def process_table_scd(client, table_name: str) -> Dict[str, Any]:
    """Process SCD Type 2 operations for a specific table."""
    logger.info(f"Processing SCD operations for {table_name}")
    
    # Check if this table uses SCD Type 2
    bucket_name = os.environ.get('S3_BUCKET_NAME')
    scd_type = get_scd_type(table_name, bucket_name)
    
    results = {
        'table': table_name,
        'scd_type': scd_type,
        'operations': {}
    }
    
    if scd_type != 'type_2':
        logger.info(f"Table {table_name} uses SCD {scd_type}, skipping SCD Type 2 processing")
        results['status'] = 'skipped'
        results['reason'] = f'Table uses SCD {scd_type}, not type_2'
        return results
    
    try:
        logger.info(f"Table {table_name} uses SCD Type 2, proceeding with processing")
        
        # 1. Validate SCD integrity
        validation_result = validate_scd_integrity(client, table_name)
        results['operations']['validation'] = validation_result
        
        # 2. Fix issues if found
        if validation_result.get('validation_results', {}).get('total_issues', 0) > 0:
            fix_result = fix_scd_issues(client, table_name)
            results['operations']['fixes'] = fix_result
        
        # 3. Cleanup old records
        cleanup_result = cleanup_expired_records(client, table_name)
        results['operations']['cleanup'] = cleanup_result
        
        # 4. Generate statistics
        stats_result = generate_scd_statistics(client, table_name)
        results['operations']['statistics'] = stats_result
        
        results['status'] = 'success'
        logger.info(f"Completed SCD Type 2 processing for {table_name}")
        
    except Exception as e:
        logger.error(f"Failed SCD processing for {table_name}: {e}")
        results['status'] = 'error'
        results['error'] = str(e)
    
    return results

def lambda_handler(event, context):
    """
    Lambda handler for SCD Type 2 processing.
    
    Args:
        event: Lambda event (can specify table_name)
        context: Lambda context
        
    Returns:
        Dict with processing results
    """
    logger.info(f"Starting SCD Type 2 processing")
    logger.info(f"Event: {json.dumps(event, default=str)}")
    
    try:
        # Get environment configuration using proper pattern
        from shared.environment import Environment
        env_name = os.environ.get('ENVIRONMENT', 'dev')
        config = Environment.get_config(env_name)
        
        # Get ClickHouse connection using shared client
        clickhouse_secret = os.environ.get('CLICKHOUSE_SECRET_NAME', 'clickhouse-credentials-dev')
        client = ClickHouseClient(clickhouse_secret, region_name='us-east-2')
        logger.info("Successfully connected to ClickHouse")
        
        # Determine tables to process
        if 'table_name' in event:
            tables = [event['table_name']]
        else:
            # Get all main tables and filter by SCD Type 2
            all_tables = ['companies', 'contacts', 'tickets', 'time_entries']
            bucket_name = os.environ.get('S3_BUCKET_NAME')
            tables = filter_tables_by_scd_type(all_tables, 'type_2', bucket_name)
            logger.info(f"Filtered to {len(tables)} SCD Type 2 tables: {tables}")
        
        # Process each table
        results = []
        for table_name in tables:
            result = process_table_scd(client, table_name)
            results.append(result)
        
        # Summarize results
        successful_tables = [r for r in results if r.get('status') == 'success']
        failed_tables = [r for r in results if r.get('status') == 'error']
        skipped_tables = [r for r in results if r.get('status') == 'skipped']
        
        response = {
            'statusCode': 200 if not failed_tables else 207,
            'body': {
                'message': 'SCD Type 2 processing completed',
                'summary': {
                    'tables_processed': len(results),
                    'successful_tables': len(successful_tables),
                    'failed_tables': len(failed_tables),
                    'skipped_tables': len(skipped_tables)
                },
                'results': results,
                'environment': os.environ.get('ENVIRONMENT', 'unknown')
            }
        }
        
        logger.info(f"SCD processing completed: {response['body']['summary']}")
        return response
        
    except Exception as e:
        logger.error(f"SCD processing failed: {e}")
        return {
            'statusCode': 500,
            'body': {
                'message': 'SCD Type 2 processing failed',
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