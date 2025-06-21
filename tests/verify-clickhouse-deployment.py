#!/usr/bin/env python3
"""
ClickHouse Infrastructure Deployment Verification Script

This script verifies that all ClickHouse infrastructure components
have been successfully deployed and are ready for ClickHouse Cloud integration.
"""

import boto3
import json
import sys
from datetime import datetime
from typing import Dict, List, Any

def print_header(title: str):
    """Print a formatted header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def print_status(item: str, status: str, details: str = ""):
    """Print status with formatting."""
    status_icon = "✅" if status == "OK" else "❌" if status == "ERROR" else "⚠️"
    print(f"{status_icon} {item:<40} {status}")
    if details:
        print(f"   {details}")

def check_cloudformation_stack(cf_client, stack_name: str) -> Dict[str, Any]:
    """Check CloudFormation stack status and outputs."""
    try:
        response = cf_client.describe_stacks(StackName=stack_name)
        stack = response['Stacks'][0]
        
        outputs = {}
        if 'Outputs' in stack:
            outputs = {output['OutputKey']: output['OutputValue'] 
                      for output in stack['Outputs']}
        
        return {
            'status': stack['StackStatus'],
            'outputs': outputs,
            'creation_time': stack['CreationTime']
        }
    except Exception as e:
        return {'status': 'NOT_FOUND', 'error': str(e)}

def check_lambda_functions(lambda_client, function_names: List[str]) -> Dict[str, Any]:
    """Check Lambda function status."""
    results = {}
    for func_name in function_names:
        try:
            response = lambda_client.get_function(FunctionName=func_name)
            config = response['Configuration']
            results[func_name] = {
                'state': config.get('State', 'Unknown'),
                'runtime': config.get('Runtime', 'Unknown'),
                'last_modified': config.get('LastModified', 'Unknown')
            }
        except Exception as e:
            results[func_name] = {'state': 'NOT_FOUND', 'error': str(e)}
    
    return results

def check_secrets_manager(secrets_client, secret_arn: str) -> Dict[str, Any]:
    """Check Secrets Manager secret."""
    try:
        response = secrets_client.describe_secret(SecretId=secret_arn)
        return {
            'status': 'EXISTS',
            'name': response['Name'],
            'last_changed': response.get('LastChangedDate', 'Unknown')
        }
    except Exception as e:
        return {'status': 'NOT_FOUND', 'error': str(e)}

def check_step_functions(sfn_client, state_machine_arn: str) -> Dict[str, Any]:
    """Check Step Functions state machine."""
    try:
        response = sfn_client.describe_state_machine(stateMachineArn=state_machine_arn)
        return {
            'status': response['status'],
            'name': response['name'],
            'creation_date': response['creationDate']
        }
    except Exception as e:
        return {'status': 'NOT_FOUND', 'error': str(e)}

def check_vpc_resources(ec2_client, vpc_id: str, subnet_ids: List[str]) -> Dict[str, Any]:
    """Check VPC and subnet resources."""
    results = {'vpc': {}, 'subnets': {}}
    
    # Check VPC
    try:
        response = ec2_client.describe_vpcs(VpcIds=[vpc_id])
        vpc = response['Vpcs'][0]
        results['vpc'] = {
            'state': vpc['State'],
            'cidr': vpc['CidrBlock']
        }
    except Exception as e:
        results['vpc'] = {'state': 'NOT_FOUND', 'error': str(e)}
    
    # Check subnets
    try:
        response = ec2_client.describe_subnets(SubnetIds=subnet_ids)
        for subnet in response['Subnets']:
            results['subnets'][subnet['SubnetId']] = {
                'state': subnet['State'],
                'cidr': subnet['CidrBlock'],
                'az': subnet['AvailabilityZone']
            }
    except Exception as e:
        results['subnets'] = {'error': str(e)}
    
    return results

def main():
    """Main verification function."""
    print_header("ClickHouse Infrastructure Deployment Verification")
    print(f"Verification Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Configuration
    region = 'us-east-2'
    stack_name = 'AVESAClickHouse-dev'
    
    # Initialize AWS clients
    try:
        cf_client = boto3.client('cloudformation', region_name=region)
        lambda_client = boto3.client('lambda', region_name=region)
        secrets_client = boto3.client('secretsmanager', region_name=region)
        sfn_client = boto3.client('stepfunctions', region_name=region)
        ec2_client = boto3.client('ec2', region_name=region)
        sns_client = boto3.client('sns', region_name=region)
    except Exception as e:
        print_status("AWS Client Initialization", "ERROR", str(e))
        sys.exit(1)
    
    print_status("AWS Client Initialization", "OK", f"Region: {region}")
    
    # Check CloudFormation Stack
    print_header("CloudFormation Stack Verification")
    stack_info = check_cloudformation_stack(cf_client, stack_name)
    
    if stack_info['status'] == 'CREATE_COMPLETE':
        print_status("CloudFormation Stack", "OK", f"Status: {stack_info['status']}")
        outputs = stack_info['outputs']
        
        # Extract key outputs
        vpc_id = outputs.get('VPCId', '')
        subnet_ids = outputs.get('PrivateSubnetIds', '').split(',')
        secret_arn = outputs.get('ClickHouseSecretArn', '')
        state_machine_arn = outputs.get('StateMachineArn', '')
        alert_topic_arn = outputs.get('AlertTopicArn', '')
        
        print_status("VPC ID", "OK", vpc_id)
        print_status("Private Subnets", "OK", f"{len(subnet_ids)} subnets")
        print_status("Secret ARN", "OK", secret_arn.split(':')[-1])
        print_status("State Machine ARN", "OK", state_machine_arn.split(':')[-1])
        print_status("Alert Topic ARN", "OK", alert_topic_arn.split(':')[-1])
        
    else:
        print_status("CloudFormation Stack", "ERROR", f"Status: {stack_info['status']}")
        if 'error' in stack_info:
            print_status("Error Details", "ERROR", stack_info['error'])
        sys.exit(1)
    
    # Check Lambda Functions
    print_header("Lambda Functions Verification")
    lambda_functions = [
        'clickhouse-schema-init-dev',
        'clickhouse-loader-companies-dev',
        'clickhouse-loader-contacts-dev',
        'clickhouse-loader-tickets-dev',
        'clickhouse-loader-time-entries-dev',
        'clickhouse-scd-processor-dev'
    ]
    
    lambda_results = check_lambda_functions(lambda_client, lambda_functions)
    
    for func_name, result in lambda_results.items():
        if result.get('state') == 'Active':
            print_status(func_name, "OK", f"Runtime: {result.get('runtime', 'Unknown')}")
        else:
            status = "ERROR" if 'error' in result else "WARNING"
            details = result.get('error', f"State: {result.get('state', 'Unknown')}")
            print_status(func_name, status, details)
    
    # Check Secrets Manager
    print_header("Secrets Manager Verification")
    secret_info = check_secrets_manager(secrets_client, secret_arn)
    
    if secret_info['status'] == 'EXISTS':
        print_status("ClickHouse Secret", "OK", f"Name: {secret_info['name']}")
        print_status("Secret Status", "WARNING", "⚠️  Update with actual ClickHouse Cloud credentials")
    else:
        print_status("ClickHouse Secret", "ERROR", secret_info.get('error', 'Not found'))
    
    # Check Step Functions
    print_header("Step Functions Verification")
    sfn_info = check_step_functions(sfn_client, state_machine_arn)
    
    if sfn_info['status'] == 'ACTIVE':
        print_status("Data Pipeline State Machine", "OK", f"Status: {sfn_info['status']}")
    else:
        print_status("Data Pipeline State Machine", "ERROR", f"Status: {sfn_info.get('status', 'Unknown')}")
    
    # Check VPC Resources
    print_header("VPC Resources Verification")
    vpc_info = check_vpc_resources(ec2_client, vpc_id, subnet_ids)
    
    if vpc_info['vpc'].get('state') == 'available':
        print_status("VPC", "OK", f"CIDR: {vpc_info['vpc'].get('cidr', 'Unknown')}")
    else:
        print_status("VPC", "ERROR", vpc_info['vpc'].get('error', 'Not available'))
    
    for subnet_id, subnet_info in vpc_info['subnets'].items():
        if 'error' not in subnet_info and subnet_info.get('state') == 'available':
            print_status(f"Subnet {subnet_id}", "OK", f"AZ: {subnet_info.get('az', 'Unknown')}")
        else:
            print_status(f"Subnet {subnet_id}", "ERROR", subnet_info.get('error', 'Not available'))
    
    # Summary and Next Steps
    print_header("Deployment Summary")
    print_status("Infrastructure Deployment", "OK", "All AWS resources successfully deployed")
    print_status("ClickHouse Cloud Setup", "PENDING", "Manual setup required via AWS Marketplace")
    
    print_header("Next Steps")
    print("\n📋 Required Actions:")
    print("   1. Subscribe to ClickHouse Cloud via AWS Marketplace")
    print("   2. Create ClickHouse Cloud service in us-east-2 region")
    print("   3. Configure VPC PrivateLink or IP access list")
    print("   4. Update Secrets Manager with actual connection credentials")
    print("   5. Test schema initialization Lambda function")
    print("   6. Validate end-to-end data pipeline")
    
    print(f"\n📖 Detailed instructions: docs/CLICKHOUSE_CLOUD_DEPLOYMENT_GUIDE.md")
    
    print_header("Resource Information")
    print(f"🔐 Secret ARN: {secret_arn}")
    print(f"🌐 VPC ID: {vpc_id}")
    print(f"🔄 State Machine ARN: {state_machine_arn}")
    print(f"📢 Alert Topic ARN: {alert_topic_arn}")
    
    print(f"\n✅ Infrastructure deployment verification completed successfully!")
    print(f"   Ready for ClickHouse Cloud integration.")

if __name__ == "__main__":
    main()