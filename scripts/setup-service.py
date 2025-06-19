#!/usr/bin/env python3
"""
Service Setup Script

This script adds or updates a service configuration for an existing tenant.
It stores service-specific data and credentials in AWS Secrets Manager.
Multiple services can be configured for each tenant.
"""

import argparse
import boto3
import json
import sys
from datetime import datetime, timezone
from typing import Dict, Any, Optional


def main():
    parser = argparse.ArgumentParser(description='Setup a service for an existing tenant')
    parser.add_argument('--tenant-id', required=True, help='Existing tenant identifier')
    parser.add_argument('--service', required=True, 
                       choices=['connectwise', 'servicenow', 'salesforce'],
                       help='Service to configure')
    parser.add_argument('--environment', default='dev', choices=['dev', 'staging', 'prod'],
                       help='Environment (default: dev)')
    parser.add_argument('--region', default='us-east-1', help='AWS region (default: us-east-1)')
    
    # ConnectWise specific arguments
    parser.add_argument('--connectwise-url', help='ConnectWise API base URL')
    parser.add_argument('--company-id', help='ConnectWise company ID')
    parser.add_argument('--public-key', help='ConnectWise public key')
    parser.add_argument('--private-key', help='ConnectWise private key')
    parser.add_argument('--client-id', help='ConnectWise client ID')
    
    # ServiceNow specific arguments
    parser.add_argument('--servicenow-instance', help='ServiceNow instance URL')
    parser.add_argument('--servicenow-username', help='ServiceNow username')
    parser.add_argument('--servicenow-password', help='ServiceNow password')
    
    # Salesforce specific arguments
    parser.add_argument('--salesforce-instance', help='Salesforce instance URL')
    parser.add_argument('--salesforce-client-id', help='Salesforce client ID')
    parser.add_argument('--salesforce-client-secret', help='Salesforce client secret')
    parser.add_argument('--salesforce-username', help='Salesforce username')
    parser.add_argument('--salesforce-password', help='Salesforce password')
    parser.add_argument('--salesforce-security-token', help='Salesforce security token')
    
    # Common arguments
    parser.add_argument('--tables', nargs='+', help='Tables/endpoints to sync for this service')
    parser.add_argument('--enabled', action='store_true', default=True, 
                       help='Enable service (default: True)')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Show what would be created without actually creating')
    
    args = parser.parse_args()
    
    # Initialize AWS clients
    dynamodb = boto3.client('dynamodb', region_name=args.region)
    secrets = boto3.client('secretsmanager', region_name=args.region)
    
    # Table names and secret pattern
    tenant_services_table = f"TenantServices-{args.environment}"
    secret_name = f"tenant/{args.tenant_id}/{args.environment}"
    
    print(f"Setting up service: {args.service}")
    print(f"Tenant: {args.tenant_id}")
    print(f"Environment: {args.environment}")
    print(f"Region: {args.region}")
    print()
    
    if args.dry_run:
        print("DRY RUN MODE - No changes will be made")
        print()
    
    try:
        # Verify tenant exists
        tenant = get_tenant(dynamodb, tenant_services_table, args.tenant_id)
        if not tenant:
            print(f"❌ Tenant {args.tenant_id} not found. Create it first using setup-tenant-only.py")
            sys.exit(1)
        
        print(f"✓ Found tenant: {tenant.get('company_name', {}).get('S', 'Unknown')}")
        
        # Validate service-specific arguments
        service_config = validate_and_build_service_config(args)
        
        # Get or create the tenant's secret
        existing_secret = get_existing_secret(secrets, secret_name)
        
        # Update the secret with new service configuration
        updated_secret = update_secret_with_service(existing_secret, args.service, service_config)
        
        # Store/update secret in AWS Secrets Manager
        print(f"Updating secret: {secret_name}")
        if not args.dry_run:
            store_secret(secrets, secret_name, updated_secret, tenant.get('company_name', {}).get('S', 'Unknown'))
            print("✓ Secret updated successfully")
        else:
            print("✓ Would update secret with:")
            print(f"  Service: {args.service}")
            print(f"  Config keys: {list(service_config.keys())}")
        
        # Update tenant configuration with service information
        service_info = build_service_info(args)
        print(f"Updating tenant configuration with {args.service} service info")
        if not args.dry_run:
            update_tenant_with_service(dynamodb, tenant_services_table, args.tenant_id, args.service, service_info)
            print("✓ Tenant configuration updated successfully")
        else:
            print("✓ Would update tenant configuration with:")
            for key, value in service_info.items():
                print(f"  {key}: {value}")
        
        print()
        print("Setup completed successfully!")
        print()
        print("Next steps:")
        print("1. Verify the service configuration:")
        print(f"   aws secretsmanager get-secret-value --secret-id {secret_name} --region {args.region}")
        print()
        print("2. Test the service Lambda function:")
        payload_json = json.dumps({"tenant_id": args.tenant_id})
        lambda_name = f"avesa-{args.service}-ingestion-{args.environment}"
        print(f"   aws lambda invoke --function-name {lambda_name} --payload '{payload_json}' response.json --region {args.region}")
        print()
        print("3. Monitor the logs:")
        print(f"   aws logs tail /aws/lambda/{lambda_name} --follow --region {args.region}")
        
    except Exception as e:
        print(f"Error setting up service: {str(e)}", file=sys.stderr)
        sys.exit(1)


def get_tenant(dynamodb, table_name: str, tenant_id: str) -> Optional[Dict[str, Any]]:
    """Get tenant configuration from DynamoDB."""
    try:
        response = dynamodb.get_item(
            TableName=table_name,
            Key={'tenant_id': {'S': tenant_id}}
        )
        return response.get('Item')
    except Exception:
        return None


def validate_and_build_service_config(args) -> Dict[str, Any]:
    """Validate service-specific arguments and build configuration."""
    if args.service == 'connectwise':
        required_args = ['connectwise_url', 'company_id', 'public_key', 'private_key', 'client_id']
        missing = [arg for arg in required_args if not getattr(args, arg.replace('-', '_'))]
        if missing:
            raise ValueError(f"Missing required ConnectWise arguments: {', '.join(missing)}")
        
        return {
            "api_url": args.connectwise_url,
            "company_id": args.company_id,
            "public_key": args.public_key,
            "private_key": args.private_key,
            "client_id": args.client_id
        }
    
    elif args.service == 'servicenow':
        required_args = ['servicenow_instance', 'servicenow_username', 'servicenow_password']
        missing = [arg for arg in required_args if not getattr(args, arg)]
        if missing:
            raise ValueError(f"Missing required ServiceNow arguments: {', '.join(missing)}")
        
        return {
            "instance_url": args.servicenow_instance,
            "username": args.servicenow_username,
            "password": args.servicenow_password
        }
    
    elif args.service == 'salesforce':
        required_args = ['salesforce_instance', 'salesforce_client_id', 'salesforce_client_secret', 
                        'salesforce_username', 'salesforce_password']
        missing = [arg for arg in required_args if not getattr(args, arg)]
        if missing:
            raise ValueError(f"Missing required Salesforce arguments: {', '.join(missing)}")
        
        config = {
            "instance_url": args.salesforce_instance,
            "client_id": args.salesforce_client_id,
            "client_secret": args.salesforce_client_secret,
            "username": args.salesforce_username,
            "password": args.salesforce_password
        }
        
        if args.salesforce_security_token:
            config["security_token"] = args.salesforce_security_token
        
        return config
    
    else:
        raise ValueError(f"Unsupported service: {args.service}")


def build_service_info(args) -> Dict[str, Any]:
    """Build service information for tenant configuration."""
    service_info = {
        "enabled": args.enabled
    }
    
    if args.service == 'connectwise':
        service_info["api_url"] = args.connectwise_url
        service_info["tables"] = args.tables or ['service/tickets', 'time/entries', 'company/companies', 'company/contacts']
    elif args.service == 'servicenow':
        service_info["instance_url"] = args.servicenow_instance
        service_info["tables"] = args.tables or ['incident', 'change_request', 'problem', 'user']
    elif args.service == 'salesforce':
        service_info["instance_url"] = args.salesforce_instance
        service_info["tables"] = args.tables or ['Account', 'Contact', 'Opportunity', 'Case', 'Lead']
    
    return service_info


def get_existing_secret(secrets, secret_name: str) -> Dict[str, Any]:
    """Get existing secret or return empty dict."""
    try:
        response = secrets.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except secrets.exceptions.ResourceNotFoundException:
        return {}
    except Exception:
        return {}


def update_secret_with_service(existing_secret: Dict[str, Any], service: str, service_config: Dict[str, Any]) -> Dict[str, Any]:
    """Update existing secret with new service configuration."""
    updated_secret = existing_secret.copy()
    updated_secret[service] = service_config
    return updated_secret


def store_secret(secrets, secret_name: str, secret_value: Dict[str, Any], company_name: str):
    """Store or update secret in AWS Secrets Manager."""
    try:
        secrets.create_secret(
            Name=secret_name,
            Description=f"Multi-service credentials for {company_name}",
            SecretString=json.dumps(secret_value)
        )
    except secrets.exceptions.ResourceExistsException:
        secrets.update_secret(
            SecretId=secret_name,
            SecretString=json.dumps(secret_value)
        )


def update_tenant_with_service(dynamodb, table_name: str, tenant_id: str, service: str, service_info: Dict[str, Any]):
    """Update tenant configuration with service information."""
    current_time = datetime.now(timezone.utc).isoformat()
    
    # Build update expression
    update_expression = f"SET services.{service} = :service_info, updated_at = :updated_at"
    expression_attribute_values = {
        ':service_info': {'M': {k: {'S': str(v)} if isinstance(v, (str, bool)) else {'L': [{'S': item} for item in v]} for k, v in service_info.items()}},
        ':updated_at': {'S': current_time}
    }
    
    dynamodb.update_item(
        TableName=table_name,
        Key={'tenant_id': {'S': tenant_id}},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values
    )


if __name__ == '__main__':
    main()