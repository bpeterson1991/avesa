"""
Universal Data Quality Pipeline Monitoring Lambda Function
=========================================================

This Lambda function provides comprehensive monitoring across the entire
canonical data transformation pipeline:

1. Raw Data Ingestion Quality
2. Canonical Transformation Quality  
3. ClickHouse Loading Quality
4. Final Data State Quality

Monitors ALL canonical tables (companies, contacts, tickets, time_entries) and
publishes detailed metrics to CloudWatch for each pipeline stage.
"""

import json
import os
import boto3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import clickhouse_connect
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
cloudwatch = boto3.client('cloudwatch')
sns = boto3.client('sns')
secrets_manager = boto3.client('secretsmanager')
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
stepfunctions = boto3.client('stepfunctions')

# Configuration from environment variables
ALERT_THRESHOLD = int(os.environ.get('ALERT_THRESHOLD', '50'))
CLICKHOUSE_SECRET_NAME = os.environ.get('CLICKHOUSE_SECRET_NAME', 'clickhouse-connection-dev')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')
MONITOR_FULL_PIPELINE = os.environ.get('MONITOR_FULL_PIPELINE', 'true').lower() == 'true'
CANONICAL_TABLES = os.environ.get('CANONICAL_TABLES', 'companies,contacts,tickets,time_entries').split(',')

# Table display names for alerts
TABLE_DISPLAY_NAMES = {
    'companies': 'Companies',
    'contacts': 'Contacts', 
    'tickets': 'Tickets',
    'time_entries': 'Time Entries'
}

# Pipeline stages
PIPELINE_STAGES = {
    'ingestion': 'Raw Data Ingestion',
    'transformation': 'Canonical Transformation',
    'loading': 'ClickHouse Loading',
    'validation': 'Final Data Validation'
}


def get_clickhouse_connection():
    """Get ClickHouse connection from AWS Secrets Manager"""
    try:
        response = secrets_manager.get_secret_value(SecretId=CLICKHOUSE_SECRET_NAME)
        secret = json.loads(response['SecretString'])
        
        client = clickhouse_connect.get_client(
            host=secret['host'],
            port=secret.get('port', 8123),
            username=secret['username'],
            password=secret['password'],
            database=secret.get('database', 'default'),
            secure=secret.get('secure', True)
        )
        
        logger.info("Successfully connected to ClickHouse")
        return client
        
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {str(e)}")
        raise


def analyze_ingestion_quality(table_name: str) -> Dict[str, Any]:
    """Analyze raw data ingestion quality for a table"""
    
    logger.info(f"Analyzing ingestion quality for table: {table_name}")
    
    try:
        # Get recent processing jobs from DynamoDB
        processing_jobs_table = dynamodb.Table('ProcessingJobsDev')  # TODO: Make environment-aware
        
        # Query recent jobs for this table
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=24)
        
        response = processing_jobs_table.scan(
            FilterExpression='#table_name = :table_name AND #created_at BETWEEN :start_time AND :end_time',
            ExpressionAttributeNames={
                '#table_name': 'table_name',
                '#created_at': 'created_at'
            },
            ExpressionAttributeValues={
                ':table_name': table_name,
                ':start_time': start_time.isoformat(),
                ':end_time': end_time.isoformat()
            }
        )
        
        jobs = response.get('Items', [])
        
        # Calculate ingestion metrics
        total_jobs = len(jobs)
        successful_jobs = len([j for j in jobs if j.get('status') == 'completed'])
        failed_jobs = len([j for j in jobs if j.get('status') == 'failed'])
        
        records_ingested = sum(int(j.get('records_processed', 0)) for j in jobs if j.get('status') == 'completed')
        ingestion_errors = sum(int(j.get('error_count', 0)) for j in jobs)
        
        # Calculate rates
        success_rate = (successful_jobs / total_jobs * 100) if total_jobs > 0 else 100
        error_rate = (ingestion_errors / max(records_ingested, 1) * 100) if records_ingested > 0 else 0
        
        # Calculate health score (weighted: 70% success rate, 30% low error rate)
        health_score = (success_rate * 0.7) + ((100 - min(error_rate, 100)) * 0.3)
        
        return {
            'table_name': table_name,
            'stage': 'ingestion',
            'total_jobs': total_jobs,
            'successful_jobs': successful_jobs,
            'failed_jobs': failed_jobs,
            'records_ingested': records_ingested,
            'ingestion_errors': ingestion_errors,
            'success_rate': round(success_rate, 2),
            'error_rate': round(error_rate, 2),
            'health_score': round(health_score, 2),
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error analyzing ingestion quality for {table_name}: {str(e)}")
        return {
            'table_name': table_name,
            'stage': 'ingestion',
            'error': str(e),
            'health_score': 0,
            'timestamp': datetime.utcnow().isoformat()
        }


def analyze_transformation_quality(table_name: str) -> Dict[str, Any]:
    """Analyze canonical transformation quality for a table"""
    
    logger.info(f"Analyzing transformation quality for table: {table_name}")
    
    try:
        # Get transformation metrics from CloudWatch (if available)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=24)
        
        # Get transformation success metrics
        try:
            transformation_metrics = cloudwatch.get_metric_statistics(
                Namespace='AWS/Lambda',
                MetricName='Invocations',
                Dimensions=[
                    {'Name': 'FunctionName', 'Value': 'canonical-transform'}
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Sum']
            )
            
            error_metrics = cloudwatch.get_metric_statistics(
                Namespace='AWS/Lambda',
                MetricName='Errors',
                Dimensions=[
                    {'Name': 'FunctionName', 'Value': 'canonical-transform'}
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Sum']
            )
            
            total_invocations = sum(point['Sum'] for point in transformation_metrics.get('Datapoints', []))
            total_errors = sum(point['Sum'] for point in error_metrics.get('Datapoints', []))
            
        except Exception as e:
            logger.warning(f"Could not get Lambda metrics: {e}")
            total_invocations = 0
            total_errors = 0
        
        # Analyze S3 transformation outputs (if available)
        try:
            # Check for recent transformation outputs in S3
            bucket_name = f"avesa-data-dev"  # TODO: Make environment-aware
            prefix = f"canonical/{table_name}/"
            
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                MaxKeys=100
            )
            
            recent_files = []
            for obj in response.get('Contents', []):
                if obj['LastModified'] >= start_time.replace(tzinfo=obj['LastModified'].tzinfo):
                    recent_files.append(obj)
            
            files_processed = len(recent_files)
            
        except Exception as e:
            logger.warning(f"Could not analyze S3 transformation outputs: {e}")
            files_processed = 0
        
        # Calculate transformation quality metrics
        success_rate = ((total_invocations - total_errors) / max(total_invocations, 1) * 100) if total_invocations > 0 else 100
        error_rate = (total_errors / max(total_invocations, 1) * 100) if total_invocations > 0 else 0
        
        # Estimate field mapping quality (placeholder - would need actual mapping analysis)
        field_mapping_errors = 0  # TODO: Implement actual field mapping error detection
        mapping_success_rate = 100  # TODO: Calculate based on actual mapping validation
        
        # Calculate health score
        health_score = (success_rate * 0.6) + (mapping_success_rate * 0.4)
        
        return {
            'table_name': table_name,
            'stage': 'transformation',
            'total_invocations': total_invocations,
            'total_errors': total_errors,
            'files_processed': files_processed,
            'success_rate': round(success_rate, 2),
            'error_rate': round(error_rate, 2),
            'field_mapping_errors': field_mapping_errors,
            'mapping_success_rate': round(mapping_success_rate, 2),
            'health_score': round(health_score, 2),
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error analyzing transformation quality for {table_name}: {str(e)}")
        return {
            'table_name': table_name,
            'stage': 'transformation',
            'error': str(e),
            'health_score': 0,
            'timestamp': datetime.utcnow().isoformat()
        }


def analyze_clickhouse_loading_quality(client, table_name: str) -> Dict[str, Any]:
    """Analyze ClickHouse loading quality for a table"""
    
    logger.info(f"Analyzing ClickHouse loading quality for table: {table_name}")
    
    try:
        # Get total record count
        total_count_query = f"""
        SELECT COUNT(*) as total_count
        FROM {table_name}
        WHERE is_current = 1
        """
        
        total_result = client.query(total_count_query)
        total_count = total_result.result_rows[0][0] if total_result.result_rows else 0
        
        # Get duplicate count (SCD Type 2 violations)
        duplicate_query = f"""
        SELECT 
            COUNT(*) as duplicate_count,
            COUNT(DISTINCT id) as unique_ids_with_duplicates
        FROM (
            SELECT id, COUNT(*) as current_count
            FROM {table_name}
            WHERE is_current = 1
            GROUP BY id
            HAVING current_count > 1
        ) duplicates
        """
        
        duplicate_result = client.query(duplicate_query)
        if duplicate_result.result_rows:
            duplicate_count = duplicate_result.result_rows[0][0]
            scd_violations = duplicate_result.result_rows[0][1]
        else:
            duplicate_count = 0
            scd_violations = 0
        
        # Get loading errors from recent inserts (if tracked)
        loading_errors = 0  # TODO: Implement actual loading error tracking
        
        # Calculate loading quality metrics
        duplicate_percentage = (duplicate_count / total_count * 100) if total_count > 0 else 0
        loading_success_rate = 100 - duplicate_percentage  # Simplified metric
        
        # Calculate health score
        health_score = max(0, 100 - duplicate_percentage)
        
        return {
            'table_name': table_name,
            'stage': 'loading',
            'total_count': total_count,
            'duplicate_count': duplicate_count,
            'scd_violations': scd_violations,
            'loading_errors': loading_errors,
            'duplicate_percentage': round(duplicate_percentage, 2),
            'loading_success_rate': round(loading_success_rate, 2),
            'health_score': round(health_score, 2),
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error analyzing ClickHouse loading quality for {table_name}: {str(e)}")
        return {
            'table_name': table_name,
            'stage': 'loading',
            'error': str(e),
            'health_score': 0,
            'timestamp': datetime.utcnow().isoformat()
        }


def analyze_final_data_validation(client, table_name: str) -> Dict[str, Any]:
    """Analyze final data state validation for a table"""
    
    logger.info(f"Analyzing final data validation for table: {table_name}")
    
    try:
        # Get data freshness
        freshness_query = f"""
        SELECT 
            MAX(effective_date) as latest_effective_date,
            COUNT(*) as total_records,
            COUNT(DISTINCT tenant_id) as tenant_count
        FROM {table_name}
        WHERE is_current = 1
        """
        
        freshness_result = client.query(freshness_query)
        if freshness_result.result_rows:
            latest_effective_date = freshness_result.result_rows[0][0]
            total_records = freshness_result.result_rows[0][1]
            tenant_count = freshness_result.result_rows[0][2]
        else:
            latest_effective_date = None
            total_records = 0
            tenant_count = 0
        
        # Calculate data freshness in hours
        if latest_effective_date:
            if isinstance(latest_effective_date, str):
                latest_date = datetime.fromisoformat(latest_effective_date.replace('Z', '+00:00'))
            else:
                latest_date = latest_effective_date
            
            data_freshness_hours = (datetime.utcnow().replace(tzinfo=latest_date.tzinfo) - latest_date).total_seconds() / 3600
        else:
            data_freshness_hours = 999  # Very stale
        
        # Get completeness metrics (simplified)
        completeness_query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN id IS NOT NULL AND id != '' THEN 1 END) as records_with_id,
            COUNT(CASE WHEN tenant_id IS NOT NULL AND tenant_id != '' THEN 1 END) as records_with_tenant
        FROM {table_name}
        WHERE is_current = 1
        """
        
        completeness_result = client.query(completeness_query)
        if completeness_result.result_rows:
            total_for_completeness = completeness_result.result_rows[0][0]
            records_with_id = completeness_result.result_rows[0][1]
            records_with_tenant = completeness_result.result_rows[0][2]
        else:
            total_for_completeness = 0
            records_with_id = 0
            records_with_tenant = 0
        
        # Calculate completeness score
        id_completeness = (records_with_id / max(total_for_completeness, 1) * 100)
        tenant_completeness = (records_with_tenant / max(total_for_completeness, 1) * 100)
        overall_completeness = (id_completeness + tenant_completeness) / 2
        
        # Calculate freshness score (100 if < 6 hours, decreasing linearly)
        freshness_score = max(0, 100 - (data_freshness_hours / 24 * 100))
        
        # Calculate overall validation health score
        health_score = (overall_completeness * 0.6) + (freshness_score * 0.4)
        
        return {
            'table_name': table_name,
            'stage': 'validation',
            'total_records': total_records,
            'tenant_count': tenant_count,
            'data_freshness_hours': round(data_freshness_hours, 2),
            'overall_completeness': round(overall_completeness, 2),
            'id_completeness': round(id_completeness, 2),
            'tenant_completeness': round(tenant_completeness, 2),
            'freshness_score': round(freshness_score, 2),
            'health_score': round(health_score, 2),
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error analyzing final data validation for {table_name}: {str(e)}")
        return {
            'table_name': table_name,
            'stage': 'validation',
            'error': str(e),
            'health_score': 0,
            'timestamp': datetime.utcnow().isoformat()
        }


def publish_pipeline_metrics(pipeline_analysis: Dict[str, List[Dict[str, Any]]]):
    """Publish comprehensive pipeline metrics to CloudWatch"""
    
    try:
        metric_data = []
        
        # Calculate overall pipeline health
        all_health_scores = []
        stage_health_scores = {stage: [] for stage in PIPELINE_STAGES.keys()}
        table_health_scores = {table: [] for table in CANONICAL_TABLES}
        
        # Process each stage's results
        for stage, results in pipeline_analysis.items():
            for result in results:
                if 'error' in result:
                    continue
                    
                table_name = result['table_name']
                health_score = result['health_score']
                
                all_health_scores.append(health_score)
                stage_health_scores[stage].append(health_score)
                table_health_scores[table_name].append(health_score)
                
                # Stage-specific metrics
                if stage == 'ingestion':
                    metric_data.extend([
                        {
                            'MetricName': 'RecordsIngested',
                            'Dimensions': [{'Name': 'TableName', 'Value': table_name}],
                            'Value': result.get('records_ingested', 0),
                            'Unit': 'Count'
                        },
                        {
                            'MetricName': 'IngestionErrors',
                            'Dimensions': [{'Name': 'TableName', 'Value': table_name}],
                            'Value': result.get('ingestion_errors', 0),
                            'Unit': 'Count'
                        },
                        {
                            'MetricName': 'IngestionErrorRate',
                            'Dimensions': [{'Name': 'TableName', 'Value': table_name}],
                            'Value': result.get('error_rate', 0),
                            'Unit': 'Percent'
                        }
                    ])
                
                elif stage == 'transformation':
                    metric_data.extend([
                        {
                            'MetricName': 'TransformationSuccessRate',
                            'Dimensions': [{'Name': 'TableName', 'Value': table_name}],
                            'Value': result.get('success_rate', 0),
                            'Unit': 'Percent'
                        },
                        {
                            'MetricName': 'FieldMappingErrors',
                            'Dimensions': [{'Name': 'TableName', 'Value': table_name}],
                            'Value': result.get('field_mapping_errors', 0),
                            'Unit': 'Count'
                        }
                    ])
                
                elif stage == 'loading':
                    metric_data.extend([
                        {
                            'MetricName': 'DuplicateCount',
                            'Dimensions': [{'Name': 'TableName', 'Value': table_name}],
                            'Value': result.get('duplicate_count', 0),
                            'Unit': 'Count'
                        },
                        {
                            'MetricName': 'SCDViolations',
                            'Dimensions': [{'Name': 'TableName', 'Value': table_name}],
                            'Value': result.get('scd_violations', 0),
                            'Unit': 'Count'
                        }
                    ])
                
                elif stage == 'validation':
                    metric_data.extend([
                        {
                            'MetricName': 'DataFreshness',
                            'Dimensions': [{'Name': 'TableName', 'Value': table_name}],
                            'Value': result.get('data_freshness_hours', 0),
                            'Unit': 'None'
                        },
                        {
                            'MetricName': 'CompletenessScore',
                            'Dimensions': [{'Name': 'TableName', 'Value': table_name}],
                            'Value': result.get('overall_completeness', 0),
                            'Unit': 'Percent'
                        }
                    ])
        
        # Overall pipeline metrics
        overall_health = sum(all_health_scores) / len(all_health_scores) if all_health_scores else 0
        metric_data.append({
            'MetricName': 'OverallHealthScore',
            'Value': overall_health,
            'Unit': 'Percent'
        })
        
        # Stage health metrics
        for stage, scores in stage_health_scores.items():
            if scores:
                stage_health = sum(scores) / len(scores)
                metric_data.append({
                    'MetricName': 'StageHealthScore',
                    'Dimensions': [{'Name': 'Stage', 'Value': stage}],
                    'Value': stage_health,
                    'Unit': 'Percent'
                })
        
        # Table health metrics
        for table, scores in table_health_scores.items():
            if scores:
                table_health = sum(scores) / len(scores)
                metric_data.append({
                    'MetricName': 'DataQualityScore',
                    'Dimensions': [{'Name': 'TableName', 'Value': table}],
                    'Value': table_health,
                    'Unit': 'Percent'
                })
        
        # Calculate throughput
        total_records = sum(
            result.get('records_ingested', 0) 
            for result in pipeline_analysis.get('ingestion', [])
            if 'error' not in result
        )
        metric_data.append({
            'MetricName': 'RecordsProcessedPerHour',
            'Value': total_records,  # This is actually per monitoring period, not per hour
            'Unit': 'Count'
        })
        
        # Publish metrics in batches
        batch_size = 20
        for i in range(0, len(metric_data), batch_size):
            batch = metric_data[i:i + batch_size]
            
            # Publish to different namespaces based on metric type
            ingestion_metrics = [m for m in batch if any(name in m['MetricName'] for name in ['RecordsIngested', 'IngestionErrors', 'IngestionErrorRate'])]
            transformation_metrics = [m for m in batch if any(name in m['MetricName'] for name in ['TransformationSuccessRate', 'FieldMappingErrors'])]
            clickhouse_metrics = [m for m in batch if any(name in m['MetricName'] for name in ['DuplicateCount', 'SCDViolations'])]
            completeness_metrics = [m for m in batch if any(name in m['MetricName'] for name in ['DataFreshness', 'CompletenessScore'])]
            pipeline_metrics = [m for m in batch if any(name in m['MetricName'] for name in ['OverallHealthScore', 'StageHealthScore', 'DataQualityScore', 'RecordsProcessedPerHour'])]
            
            if ingestion_metrics:
                cloudwatch.put_metric_data(Namespace='DataQuality/Ingestion', MetricData=ingestion_metrics)
            if transformation_metrics:
                cloudwatch.put_metric_data(Namespace='DataQuality/Transformation', MetricData=transformation_metrics)
            if clickhouse_metrics:
                cloudwatch.put_metric_data(Namespace='DataQuality/ClickHouse', MetricData=clickhouse_metrics)
            if completeness_metrics:
                cloudwatch.put_metric_data(Namespace='DataQuality/Completeness', MetricData=completeness_metrics)
            if pipeline_metrics:
                cloudwatch.put_metric_data(Namespace='DataQuality/Pipeline', MetricData=pipeline_metrics)
        
        logger.info(f"Published {len(metric_data)} pipeline metrics to CloudWatch")
        
    except Exception as e:
        logger.error(f"Error publishing pipeline metrics: {str(e)}")
        raise


def send_pipeline_alerts(pipeline_analysis: Dict[str, List[Dict[str, Any]]]):
    """Send comprehensive pipeline alerts if issues detected"""
    
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_TOPIC_ARN not configured, skipping alerts")
        return
    
    alerts = []
    critical_issues = []
    
    # Analyze each stage for issues
    for stage, results in pipeline_analysis.items():
        stage_name = PIPELINE_STAGES[stage]
        
        for result in results:
            if 'error' in result:
                critical_issues.append(f"❌ {stage_name} ({result['table_name']}): Analysis failed - {result['error']}")
                continue
                
            table_name = result['table_name']
            table_display_name = TABLE_DISPLAY_NAMES.get(table_name, table_name)
            health_score = result['health_score']
            
            # Stage-specific thresholds and alerts
            if stage == 'ingestion':
                error_rate = result.get('error_rate', 0)
                success_rate = result.get('success_rate', 100)
                
                if error_rate > 5.0:
                    alerts.append(f"🚨 {table_display_name} Ingestion: High error rate ({error_rate:.1f}%)")
                if success_rate < 95.0:
                    alerts.append(f"⚠️ {table_display_name} Ingestion: Low success rate ({success_rate:.1f}%)")
            
            elif stage == 'transformation':
                success_rate = result.get('success_rate', 100)
                mapping_errors = result.get('field_mapping_errors', 0)
                
                if success_rate < 95.0:
                    alerts.append(f"🚨 {table_display_name} Transformation: Low success rate ({success_rate:.1f}%)")
                if mapping_errors > 0:
                    alerts.append(f"⚠️ {table_display_name} Transformation: {mapping_errors} field mapping errors")
            
            elif stage == 'loading':
                duplicate_count = result.get('duplicate_count', 0)
                scd_violations = result.get('scd_violations', 0)
                
                if duplicate_count > ALERT_THRESHOLD:
                    alerts.append(f"🚨 {table_display_name} Loading: {duplicate_count} duplicates detected")
                if scd_violations > 0:
                    alerts.append(f"🔄 {table_display_name} Loading: {scd_violations} SCD violations")
            
            elif stage == 'validation':
                freshness_hours = result.get('data_freshness_hours', 0)
                completeness = result.get('overall_completeness', 100)
                
                if freshness_hours > 24:
                    alerts.append(f"⏰ {table_display_name} Validation: Data is {freshness_hours:.1f} hours old")
                if completeness < 90:
                    alerts.append(f"📊 {table_display_name} Validation: Low completeness ({completeness:.1f}%)")
            
            # General health score alerts
            if health_score < 80:
                alerts.append(f"📉 {table_display_name} {stage_name}: Low health score ({health_score:.1f}%)")
    
    # Send alerts if any issues found
    all_issues = critical_issues + alerts
    if all_issues:
        # Calculate overall pipeline health
        all_health_scores = []
        for results in pipeline_analysis.values():
            for result in results:
                if 'error' not in result:
                    all_health_scores.append(result['health_score'])
        
        overall_health = sum(all_health_scores) / len(all_health_scores) if all_health_scores else 0
        
        alert_message = f"""
🔍 Data Quality Pipeline Alert

Overall Pipeline Health: {overall_health:.1f}%

Critical Issues:
{chr(10).join(critical_issues) if critical_issues else "None"}

Warnings:
{chr(10).join(alerts) if alerts else "None"}

Pipeline Stages Monitored:
• Raw Data Ingestion
• Canonical Transformation  
• ClickHouse Loading
• Final Data Validation

Tables Monitored: {', '.join(TABLE_DISPLAY_NAMES.values())}

Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}

Please investigate pipeline issues and run appropriate remediation procedures.
        """.strip()
        
        try:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject="Data Quality Pipeline Alert",
                Message=alert_message
            )
            logger.info(f"Sent SNS alert for {len(all_issues)} pipeline issues")
            
        except Exception as e:
            logger.error(f"Error sending SNS alert: {str(e)}")
    else:
        logger.info("No pipeline alerts needed - all stages within thresholds")


def lambda_handler(event, context):
    """Main Lambda handler for comprehensive pipeline monitoring"""
    
    logger.info("Starting comprehensive data quality pipeline monitoring")
    
    try:
        # Connect to ClickHouse for loading and validation analysis
        clickhouse_client = get_clickhouse_connection()
        
        # Determine which tables to monitor
        tables_to_monitor = CANONICAL_TABLES
        logger.info(f"Monitoring tables: {tables_to_monitor}")
        
        # Analyze each pipeline stage for each table
        pipeline_analysis = {
            'ingestion': [],
            'transformation': [],
            'loading': [],
            'validation': []
        }
        
        for table_name in tables_to_monitor:
            logger.info(f"Analyzing pipeline for table: {table_name}")
            
            # Analyze each stage
            pipeline_analysis['ingestion'].append(analyze_ingestion_quality(table_name))
            pipeline_analysis['transformation'].append(analyze_transformation_quality(table_name))
            pipeline_analysis['loading'].append(analyze_clickhouse_loading_quality(clickhouse_client, table_name))
            pipeline_analysis['validation'].append(analyze_final_data_validation(clickhouse_client, table_name))
        
        # Publish comprehensive metrics to CloudWatch
        publish_pipeline_metrics(pipeline_analysis)
        
        # Send alerts if needed
        send_pipeline_alerts(pipeline_analysis)
        
        # Calculate summary statistics
        total_issues = 0
        successful_analyses = 0
        failed_analyses = 0
        
        for stage_results in pipeline_analysis.values():
            for result in stage_results:
                if 'error' in result:
                    failed_analyses += 1
                else:
                    successful_analyses += 1
        
        # Calculate overall health scores
        all_health_scores = []
        for stage_results in pipeline_analysis.values():
            for result in stage_results:
                if 'error' not in result:
                    all_health_scores.append(result['health_score'])
        
        overall_health = sum(all_health_scores) / len(all_health_scores) if all_health_scores else 0
        
        # Prepare response
        response = {
            'statusCode': 200,
            'body': {
                'message': 'Comprehensive pipeline monitoring completed successfully',
                'tables_monitored': len(tables_to_monitor),
                'stages_monitored': len(PIPELINE_STAGES),
                'successful_analyses': successful_analyses,
                'failed_analyses': failed_analyses,
                'overall_pipeline_health': round(overall_health, 2),
                'pipeline_analysis': pipeline_analysis,
                'timestamp': datetime.utcnow().isoformat()
            }
        }
        
        logger.info(f"Pipeline monitoring completed: Overall health {overall_health:.1f}%")
        return response
        
    except Exception as e:
        logger.error(f"Pipeline monitoring failed: {str(e)}")
        
        # Try to send error alert
        if SNS_TOPIC_ARN:
            try:
                sns.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Subject="Data Quality Pipeline Monitoring Error",
                    Message=f"Comprehensive pipeline monitoring failed: {str(e)}\nTimestamp: {datetime.utcnow().isoformat()}"
                )
            except:
                pass  # Don't fail on alert failure
        
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
        }
    
    finally:
        # Close ClickHouse connection
        try:
            if 'clickhouse_client' in locals():
                clickhouse_client.close()
        except:
            pass