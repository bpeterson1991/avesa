#!/usr/bin/env python3
"""
Test script to validate ConnectWise data endpoints for sitetechnology tenant.
This script tests the specific endpoints that were returning 403 errors.
"""

import json
import os
import sys
import requests
from typing import Dict, Any

# Add shared module to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'shared'))

try:
    from aws_clients import get_secrets_client
    from config_simple import ConnectWiseCredentials
    from logger import PipelineLogger
except ImportError as e:
    print(f"Error importing shared modules: {e}")
    sys.exit(1)


def test_connectwise_endpoints():
    """Test the specific ConnectWise endpoints that were failing."""
    logger = PipelineLogger("connectwise_endpoint_test")
    
    # Test configuration
    tenant_id = "sitetechnology"
    environment = os.environ.get("ENVIRONMENT", "dev")
    secret_name = f"{tenant_id}/{environment}"
    
    # Endpoints to test (the ones that were returning 403)
    test_endpoints = [
        "service/tickets",
        "time/entries", 
        "company/companies",
        "company/contacts"
    ]
    
    print(f"Testing ConnectWise data endpoints for tenant: {tenant_id}")
    print(f"Endpoints to test: {test_endpoints}")
    print("-" * 60)
    
    try:
        # Get credentials
        secrets = get_secrets_client()
        response = secrets.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response['SecretString'])
        connectwise_data = secret_data['connectwise']
        credentials = ConnectWiseCredentials(**connectwise_data)
        
        # Test each endpoint
        results = {}
        
        for endpoint in test_endpoints:
            print(f"\nTesting endpoint: {endpoint}")
            
            # Build URL with corrected logic
            api_base_url = connectwise_data['api_base_url'].rstrip('/')
            if 'v4_6_release/apis/3.0' in api_base_url:
                url = f"{api_base_url}/{endpoint}"
            else:
                url = f"{api_base_url}/v4_6_release/apis/3.0/{endpoint}"
            
            print(f"URL: {url}")
            
            # Build headers
            headers = {
                'Authorization': credentials.get_auth_header(),
                'ClientId': connectwise_data['client_id'],
                'Content-Type': 'application/json',
                'Accept': 'application/vnd.connectwise.com+json;version=2022.1'
            }
            
            # Add pagination parameters
            params = {
                'pageSize': 5,  # Small page size for testing
                'page': 1,
                'orderBy': 'id asc'
            }
            
            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)
                
                print(f"Status Code: {response.status_code}")
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        record_count = len(data) if isinstance(data, list) else 1
                        print(f"✓ SUCCESS - Retrieved {record_count} records")
                        
                        # Show sample record structure if available
                        if isinstance(data, list) and data:
                            sample_keys = list(data[0].keys()) if isinstance(data[0], dict) else []
                            print(f"Sample record keys: {sample_keys[:10]}...")  # First 10 keys
                        
                        results[endpoint] = {
                            'status': 'success',
                            'record_count': record_count,
                            'status_code': response.status_code
                        }
                    except Exception as e:
                        print(f"✓ SUCCESS - Response received but JSON parsing failed: {e}")
                        results[endpoint] = {
                            'status': 'success',
                            'status_code': response.status_code,
                            'note': 'Non-JSON response'
                        }
                        
                elif response.status_code == 403:
                    print(f"✗ FAILED - HTTP 403 Forbidden")
                    print(f"Response: {response.text[:200]}...")
                    results[endpoint] = {
                        'status': 'failed',
                        'status_code': response.status_code,
                        'error': 'HTTP 403 Forbidden'
                    }
                    
                else:
                    print(f"✗ FAILED - HTTP {response.status_code}")
                    print(f"Response: {response.text[:200]}...")
                    results[endpoint] = {
                        'status': 'failed',
                        'status_code': response.status_code,
                        'error': f'HTTP {response.status_code}'
                    }
                    
            except Exception as e:
                print(f"✗ FAILED - Request error: {e}")
                results[endpoint] = {
                    'status': 'failed',
                    'error': str(e)
                }
        
        # Summary
        print("\n" + "=" * 60)
        print("ENDPOINT TEST SUMMARY")
        print("=" * 60)
        
        successful = 0
        failed = 0
        
        for endpoint, result in results.items():
            status = result['status']
            if status == 'success':
                print(f"✓ {endpoint}: SUCCESS")
                successful += 1
            else:
                print(f"✗ {endpoint}: FAILED - {result.get('error', 'Unknown error')}")
                failed += 1
        
        print(f"\nResults: {successful} successful, {failed} failed")
        
        return failed == 0
        
    except Exception as e:
        print(f"✗ Test failed with error: {e}")
        return False


if __name__ == "__main__":
    print("ConnectWise Data Endpoints Test")
    print("=" * 60)
    
    # Set environment
    os.environ['AWS_REGION'] = os.environ.get('AWS_REGION', 'us-east-1')
    
    success = test_connectwise_endpoints()
    
    if success:
        print("\n✓ All ConnectWise data endpoints are working correctly!")
        print("The HTTP 403 errors have been resolved.")
    else:
        print("\n✗ Some ConnectWise data endpoints are still failing.")
        print("Additional investigation may be required.")
    
    sys.exit(0 if success else 1)