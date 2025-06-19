#!/usr/bin/env python3
"""
Backfill Trigger Script

This script helps trigger backfill operations for tenants when they connect
new services. It can be used for manual backfills or testing.
"""

import argparse
import boto3
import json
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, Any


def main():
    parser = argparse.ArgumentParser(description='Trigger backfill for tenant services')
    parser.add_argument('--tenant-id', required=True, help='Tenant identifier')
    parser.add_argument('--service', required=True, choices=['connectwise', 'servicenow', 'salesforce'],
                       help='Service to backfill')
    parser.add_argument('--table-name', help='Specific table/endpoint to backfill (optional)')
    parser.add_argument('--start-date', help='Start date for backfill (ISO format, e.g., 2023-01-01T00:00:00Z)')
    parser.add_argument('--end-date', help='End date for backfill (ISO format, defaults to now)')
    parser.add_argument('--chunk-size-days', type=int, default=30, help='Chunk size in days (default: 30)')
    parser.add_argument('--environment', default='dev', choices=['dev', 'staging', 'prod'],
                       help='Environment (default: dev)')
    parser.add_argument('--region', default='us-east-1', help='AWS region (default: us-east-1)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be triggered without actually doing it')
    parser.add_argument('--auto-detect', action='store_true', help='Auto-detect services needing backfill')
    
    args = parser.parse_args()
    
    # Initialize AWS client
    lambda_client = boto3.client('lambda', region_name=args.region)
    
    print(f"Backfill Configuration:")
    print(f"  Tenant ID: {args.tenant_id}")
    print(f"  Service: {args.service}")
    print(f"  Environment: {args.environment}")
    print(f"  Region: {args.region}")
    if args.table_name:
        print(f"  Table: {args.table_name}")
    if args.start_date:
        print(f"  Start Date: {args.start_date}")
    if args.end_date:
        print(f"  End Date: {args.end_date}")
    print(f"  Chunk Size: {args.chunk_size_days} days")
    print()
    
    if args.dry_run:
        print("DRY RUN MODE - No backfill will be triggered")
        print()
    
    try:
        if args.auto_detect:
            # Trigger auto-detection
            result = trigger_auto_detection(lambda_client, args.environment, args.dry_run)
        else:
            # Trigger manual backfill
            result = trigger_manual_backfill(
                lambda_client=lambda_client,
                environment=args.environment,
                tenant_id=args.tenant_id,
                service=args.service,
                table_name=args.table_name,
                start_date=args.start_date,
                end_date=args.end_date,
                chunk_size_days=args.chunk_size_days,
                dry_run=args.dry_run
            )
        
        print("✓ Backfill triggered successfully")
        print(f"Response: {json.dumps(result, indent=2)}")
        
        if not args.dry_run:
            print()
            print("Monitor the backfill progress:")
            print(f"  aws logs tail /aws/lambda/avesa-backfill-initiator-{args.environment} --follow --region {args.region}")
            print(f"  aws logs tail /aws/lambda/avesa-backfill-{args.environment} --follow --region {args.region}")
        
    except Exception as e:
        print(f"Error triggering backfill: {str(e)}", file=sys.stderr)
        sys.exit(1)


def trigger_auto_detection(lambda_client, environment: str, dry_run: bool) -> Dict[str, Any]:
    """Trigger auto-detection of services needing backfill."""
    
    function_name = f"avesa-backfill-initiator-{environment}"
    
    payload = {
        'action': 'auto_detect'
    }
    
    if dry_run:
        print(f"Would invoke Lambda function: {function_name}")
        print(f"With payload: {json.dumps(payload, indent=2)}")
        return {'dry_run': True}
    
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )
    
    result = json.loads(response['Payload'].read().decode('utf-8'))
    return result


def trigger_manual_backfill(
    lambda_client,
    environment: str,
    tenant_id: str,
    service: str,
    table_name: str = None,
    start_date: str = None,
    end_date: str = None,
    chunk_size_days: int = 30,
    dry_run: bool = False
) -> Dict[str, Any]:
    """Trigger manual backfill for a specific tenant/service."""
    
    function_name = f"avesa-backfill-initiator-{environment}"
    
    payload = {
        'action': 'manual_trigger',
        'tenant_id': tenant_id,
        'service': service,
        'chunk_size_days': chunk_size_days
    }
    
    if table_name:
        payload['table_name'] = table_name
    if start_date:
        payload['start_date'] = start_date
    if end_date:
        payload['end_date'] = end_date
    
    if dry_run:
        print(f"Would invoke Lambda function: {function_name}")
        print(f"With payload: {json.dumps(payload, indent=2)}")
        return {'dry_run': True}
    
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )
    
    result = json.loads(response['Payload'].read().decode('utf-8'))
    return result


def validate_date_format(date_str: str) -> bool:
    """Validate ISO date format."""
    try:
        datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return True
    except ValueError:
        return False


if __name__ == '__main__':
    main()