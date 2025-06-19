#!/usr/bin/env python3
"""
Lambda Function Testing Script

This script tests the Lambda functions in the dev environment to ensure they're working properly.
It performs end-to-end testing of the data pipeline.
"""

import argparse
import boto3
import json
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any, List


def main():
    parser = argparse.ArgumentParser(description='Test Lambda functions in dev environment')
    parser.add_argument('--environment', default='dev', choices=['dev', 'staging', 'prod'],
                       help='Environment to test (default: dev)')
    parser.add_argument('--region', default='us-east-2', help='AWS region (default: us-east-2)')
    parser.add_argument('--tenant-id', default='test-tenant', help='Tenant ID to test with')
    parser.add_argument('--skip-ingestion', action='store_true', help='Skip ingestion testing')
    parser.add_argument('--skip-canonical', action='store_true', help='Skip canonical transform testing')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    # Initialize AWS clients
    lambda_client = boto3.client('lambda', region_name=args.region)
    dynamodb = boto3.client('dynamodb', region_name=args.region)
    s3 = boto3.client('s3', region_name=args.region)
    
    print(f"🧪 Testing Lambda functions in {args.environment} environment")
    print(f"Region: {args.region}")
    print(f"Tenant ID: {args.tenant_id}")
    print()
    
    test_results = []
    
    try:
        # 1. Test environment setup
        print("🔍 Testing environment setup...")
        setup_results = test_environment_setup(dynamodb, s3, args.environment, args.region)
        test_results.extend(setup_results)
        
        # 2. Test ConnectWise ingestion function
        if not args.skip_ingestion:
            print("📥 Testing ConnectWise ingestion function...")
            ingestion_results = test_connectwise_ingestion(lambda_client, args.environment, args.tenant_id, args.verbose)
            test_results.extend(ingestion_results)
        
        # 3. Test canonical transform functions
        if not args.skip_canonical:
            print("🔄 Testing canonical transform functions...")
            canonical_results = test_canonical_transforms(lambda_client, args.environment, args.tenant_id, args.verbose)
            test_results.extend(canonical_results)
        
        # 4. Generate test report
        print_test_report(test_results)
        
        # 5. Provide troubleshooting guidance
        failed_tests = [t for t in test_results if not t['passed']]
        if failed_tests:
            print_troubleshooting_guide(failed_tests, args.environment, args.region)
        
    except Exception as e:
        print(f"❌ Error during testing: {str(e)}", file=sys.stderr)
        sys.exit(1)


def test_environment_setup(dynamodb, s3, environment: str, region: str) -> List[Dict[str, Any]]:
    """Test that the environment is properly set up."""
    results = []
    
    # Test DynamoDB tables
    required_tables = [f'TenantServices-{environment}', f'LastUpdated-{environment}']
    for table_name in required_tables:
        try:
            response = dynamodb.describe_table(TableName=table_name)
            status = response['Table']['TableStatus']
            results.append({
                'test': f'DynamoDB Table {table_name}',
                'passed': status == 'ACTIVE',
                'message': f'Status: {status}',
                'details': response['Table']
            })
            print(f"  ✅ DynamoDB table {table_name}: {status}")
        except Exception as e:
            results.append({
                'test': f'DynamoDB Table {table_name}',
                'passed': False,
                'message': str(e),
                'details': None
            })
            print(f"  ❌ DynamoDB table {table_name}: {e}")
    
    # Test S3 bucket
    bucket_name = f'data-storage-msp-{environment}' if environment != 'prod' else 'data-storage-msp'
    try:
        s3.head_bucket(Bucket=bucket_name)
        results.append({
            'test': f'S3 Bucket {bucket_name}',
            'passed': True,
            'message': 'Bucket accessible',
            'details': None
        })
        print(f"  ✅ S3 bucket {bucket_name}: Accessible")
    except Exception as e:
        results.append({
            'test': f'S3 Bucket {bucket_name}',
            'passed': False,
            'message': str(e),
            'details': None
        })
        print(f"  ❌ S3 bucket {bucket_name}: {e}")
    
    return results


def test_connectwise_ingestion(lambda_client, environment: str, tenant_id: str, verbose: bool) -> List[Dict[str, Any]]:
    """Test ConnectWise ingestion function."""
    results = []
    function_name = f"avesa-connectwise-ingestion-{environment}"
    
    try:
        # Test function exists and is accessible
        try:
            response = lambda_client.get_function(FunctionName=function_name)
            results.append({
                'test': f'Lambda Function {function_name} Exists',
                'passed': True,
                'message': 'Function found',
                'details': response['Configuration']
            })
            print(f"  ✅ Function {function_name}: Found")
        except Exception as e:
            results.append({
                'test': f'Lambda Function {function_name} Exists',
                'passed': False,
                'message': str(e),
                'details': None
            })
            print(f"  ❌ Function {function_name}: {e}")
            return results
        
        # Test function invocation
        payload = json.dumps({"tenant_id": tenant_id})
        
        print(f"  🔄 Invoking {function_name} with tenant {tenant_id}...")
        
        response = lambda_client.invoke(
            FunctionName=function_name,
            Payload=payload
        )
        
        # Parse response
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))
        
        if response.get('FunctionError'):
            results.append({
                'test': f'Lambda Function {function_name} Invocation',
                'passed': False,
                'message': f"Function error: {response.get('FunctionError')}",
                'details': response_payload
            })
            print(f"  ❌ Function {function_name}: {response_payload.get('errorMessage', 'Unknown error')}")
        else:
            status_code = response_payload.get('statusCode', 500)
            passed = status_code == 200
            
            results.append({
                'test': f'Lambda Function {function_name} Invocation',
                'passed': passed,
                'message': f"Status code: {status_code}",
                'details': response_payload
            })
            
            if passed:
                print(f"  ✅ Function {function_name}: Executed successfully")
                if verbose:
                    body = response_payload.get('body', {})
                    print(f"    Records processed: {body.get('total_records', 0)}")
                    print(f"    Successful jobs: {body.get('successful_jobs', 0)}")
                    print(f"    Failed jobs: {body.get('failed_jobs', 0)}")
            else:
                print(f"  ❌ Function {function_name}: Status {status_code}")
                if verbose:
                    print(f"    Error: {response_payload}")
        
    except Exception as e:
        results.append({
            'test': f'Lambda Function {function_name} Invocation',
            'passed': False,
            'message': str(e),
            'details': None
        })
        print(f"  ❌ Function {function_name}: {e}")
    
    return results


def test_canonical_transforms(lambda_client, environment: str, tenant_id: str, verbose: bool) -> List[Dict[str, Any]]:
    """Test canonical transform functions."""
    results = []
    canonical_tables = ['tickets', 'time-entries', 'companies', 'contacts']
    
    for table in canonical_tables:
        function_name = f"avesa-canonical-transform-{table}-{environment}"
        
        try:
            # Test function exists
            try:
                response = lambda_client.get_function(FunctionName=function_name)
                results.append({
                    'test': f'Lambda Function {function_name} Exists',
                    'passed': True,
                    'message': 'Function found',
                    'details': response['Configuration']
                })
                print(f"  ✅ Function {function_name}: Found")
            except Exception as e:
                results.append({
                    'test': f'Lambda Function {function_name} Exists',
                    'passed': False,
                    'message': str(e),
                    'details': None
                })
                print(f"  ❌ Function {function_name}: {e}")
                continue
            
            # Test function invocation
            payload = json.dumps({"tenant_id": tenant_id})
            
            print(f"  🔄 Invoking {function_name}...")
            
            response = lambda_client.invoke(
                FunctionName=function_name,
                Payload=payload
            )
            
            # Parse response
            response_payload = json.loads(response['Payload'].read().decode('utf-8'))
            
            if response.get('FunctionError'):
                results.append({
                    'test': f'Lambda Function {function_name} Invocation',
                    'passed': False,
                    'message': f"Function error: {response.get('FunctionError')}",
                    'details': response_payload
                })
                print(f"  ❌ Function {function_name}: {response_payload.get('errorMessage', 'Unknown error')}")
            else:
                status_code = response_payload.get('statusCode', 500)
                passed = status_code == 200
                
                results.append({
                    'test': f'Lambda Function {function_name} Invocation',
                    'passed': passed,
                    'message': f"Status code: {status_code}",
                    'details': response_payload
                })
                
                if passed:
                    print(f"  ✅ Function {function_name}: Executed successfully")
                    if verbose:
                        body = response_payload.get('body', {})
                        print(f"    Records processed: {body.get('total_records', 0)}")
                        print(f"    Successful jobs: {body.get('successful_jobs', 0)}")
                else:
                    print(f"  ❌ Function {function_name}: Status {status_code}")
            
        except Exception as e:
            results.append({
                'test': f'Lambda Function {function_name} Invocation',
                'passed': False,
                'message': str(e),
                'details': None
            })
            print(f"  ❌ Function {function_name}: {e}")
    
    return results


def print_test_report(results: List[Dict[str, Any]]):
    """Print comprehensive test report."""
    print()
    print("📊 Test Report")
    print("=" * 50)
    
    total_tests = len(results)
    passed_tests = len([r for r in results if r['passed']])
    failed_tests = total_tests - passed_tests
    
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {failed_tests}")
    print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
    print()
    
    if failed_tests > 0:
        print("❌ Failed Tests:")
        for result in results:
            if not result['passed']:
                print(f"  - {result['test']}: {result['message']}")
        print()
    
    print("✅ Passed Tests:")
    for result in results:
        if result['passed']:
            print(f"  - {result['test']}: {result['message']}")


def print_troubleshooting_guide(failed_tests: List[Dict[str, Any]], environment: str, region: str):
    """Print troubleshooting guide for failed tests."""
    print()
    print("🔧 Troubleshooting Guide")
    print("=" * 50)
    
    for test in failed_tests:
        test_name = test['test']
        message = test['message']
        
        print(f"\n❌ {test_name}")
        print(f"   Error: {message}")
        
        if 'DynamoDB Table' in test_name:
            print("   Solutions:")
            print(f"   1. Create missing table: python scripts/setup-dev-environment.py --region {region}")
            print(f"   2. Check table status: aws dynamodb describe-table --table-name [TABLE_NAME] --region {region}")
        
        elif 'S3 Bucket' in test_name:
            print("   Solutions:")
            print(f"   1. Create missing bucket: python scripts/setup-dev-environment.py --region {region}")
            print(f"   2. Check bucket permissions: aws s3 ls s3://[BUCKET_NAME] --region {region}")
        
        elif 'Lambda Function' in test_name and 'Exists' in test_name:
            print("   Solutions:")
            print(f"   1. Deploy missing function: python scripts/deploy-lambda-functions.py --environment {environment} --region {region}")
            print(f"   2. Check function exists: aws lambda get-function --function-name [FUNCTION_NAME] --region {region}")
        
        elif 'Lambda Function' in test_name and 'Invocation' in test_name:
            print("   Solutions:")
            print(f"   1. Check function logs: aws logs tail /aws/lambda/[FUNCTION_NAME] --follow --region {region}")
            print(f"   2. Redeploy with fixed dependencies: python scripts/deploy-lambda-functions.py --environment {environment} --region {region}")
            print("   3. Check environment variables and IAM permissions")
            
            if 'pydantic' in message or 'import' in message.lower():
                print("   4. This looks like a dependency issue - redeploy the function")
        
        print()


if __name__ == '__main__':
    main()