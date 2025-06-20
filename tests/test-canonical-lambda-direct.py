#!/usr/bin/env python3
"""
Direct Canonical Transform Lambda Function Test

This script directly tests canonical transform Lambda functions with minimal setup.
"""

import boto3
import json
import sys
from datetime import datetime

def test_canonical_lambda_direct(region='us-east-2', environment='dev', table='companies'):
    """Test a canonical transform Lambda function directly."""
    
    print(f"🧪 Direct Canonical Transform Lambda Test")
    print(f"Region: {region}, Environment: {environment}, Table: {table}")
    print(f"Time: {datetime.now().isoformat()}")
    print()
    
    # Initialize Lambda client
    lambda_client = boto3.client('lambda', region_name=region)
    
    # Function name
    function_name = f"avesa-canonical-transform-{table.replace('_', '-')}-{environment}"
    
    try:
        # Test 1: Check if function exists
        print(f"🔍 Checking function: {function_name}")
        try:
            response = lambda_client.get_function(FunctionName=function_name)
            print(f"✅ Function exists and is accessible")
            print(f"   Runtime: {response['Configuration']['Runtime']}")
            print(f"   Memory: {response['Configuration']['MemorySize']} MB")
            print(f"   Timeout: {response['Configuration']['Timeout']} seconds")
        except Exception as e:
            print(f"❌ Function not found or not accessible: {str(e)}")
            return False
        
        # Test 2: Invoke function
        print(f"\n🚀 Invoking function with test payload...")
        
        payload = {
            'tenant_id': 'sitetechnology',
            'test_mode': True
        }
        
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        # Parse response
        if response['StatusCode'] == 200:
            response_payload = json.loads(response['Payload'].read())
            
            if response.get('FunctionError'):
                print(f"❌ Function execution error:")
                print(f"   Error: {response_payload.get('errorMessage', 'Unknown error')}")
                print(f"   Type: {response_payload.get('errorType', 'Unknown')}")
                if 'stackTrace' in response_payload:
                    print(f"   Stack trace: {response_payload['stackTrace'][:3]}")
                return False
            else:
                status_code = response_payload.get('statusCode', 500)
                body = response_payload.get('body', {})
                
                if status_code == 200:
                    print(f"✅ Function executed successfully")
                    print(f"   Status: {status_code}")
                    print(f"   Message: {body.get('message', 'No message')}")
                    print(f"   Records processed: {body.get('total_records', 0)}")
                    print(f"   Successful jobs: {body.get('successful_jobs', 0)}")
                    print(f"   Failed jobs: {body.get('failed_jobs', 0)}")
                    return True
                else:
                    print(f"❌ Function returned error status: {status_code}")
                    print(f"   Body: {body}")
                    return False
        else:
            print(f"❌ Lambda invocation failed: HTTP {response['StatusCode']}")
            return False
            
    except Exception as e:
        print(f"❌ Test failed with exception: {str(e)}")
        return False

def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Direct Canonical Lambda Test')
    parser.add_argument('--region', default='us-east-2', help='AWS region')
    parser.add_argument('--environment', default='dev', help='Environment')
    parser.add_argument('--table', default='companies', 
                       choices=['companies', 'contacts', 'tickets', 'time_entries'],
                       help='Table to test')
    
    args = parser.parse_args()
    
    success = test_canonical_lambda_direct(args.region, args.environment, args.table)
    
    print(f"\n{'='*50}")
    if success:
        print("🎉 DIRECT LAMBDA TEST: PASSED")
    else:
        print("❌ DIRECT LAMBDA TEST: FAILED")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())