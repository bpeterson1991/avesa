#!/usr/bin/env python3
"""
Test script to verify that processors work with the dynamic service discovery system.
"""

import sys
import os
import json
from unittest.mock import Mock, patch

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'shared'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'optimized', 'processors'))

def test_tenant_processor_dynamic_service_config():
    """Test that tenant processor can dynamically load service configurations."""
    print("=== Testing Tenant Processor Dynamic Service Config ===")
    
    try:
        # Mock environment variables
        with patch.dict(os.environ, {
            'BUCKET_NAME': 'test-bucket',
            'TENANT_SERVICES_TABLE': 'test-tenant-services',
            'LAST_UPDATED_TABLE': 'test-last-updated',
            'ENVIRONMENT': 'test'
        }):
            from tenant_processor import TenantProcessor
            
            processor = TenantProcessor()
            
            # Test ConnectWise service config
            connectwise_config = processor._get_service_config('connectwise')
            print(f"ConnectWise config loaded: {connectwise_config is not None}")
            
            if connectwise_config:
                tables = connectwise_config.get('tables', [])
                print(f"ConnectWise tables found: {len(tables)}")
                for table in tables:
                    print(f"  - {table['table_name']} ({table['endpoint']}) -> {table.get('canonical_table', 'N/A')}")
            
            # Test Salesforce service config
            salesforce_config = processor._get_service_config('salesforce')
            print(f"Salesforce config loaded: {salesforce_config is not None}")
            
            if salesforce_config:
                tables = salesforce_config.get('tables', [])
                print(f"Salesforce tables found: {len(tables)}")
                for table in tables:
                    print(f"  - {table['table_name']} ({table['endpoint']}) -> {table.get('canonical_table', 'N/A')}")
            
            # Test ServiceNow service config
            servicenow_config = processor._get_service_config('servicenow')
            print(f"ServiceNow config loaded: {servicenow_config is not None}")
            
            if servicenow_config:
                tables = servicenow_config.get('tables', [])
                print(f"ServiceNow tables found: {len(tables)}")
                for table in tables:
                    print(f"  - {table['table_name']} ({table['endpoint']}) -> {table.get('canonical_table', 'N/A')}")
            
            return all([connectwise_config, salesforce_config, servicenow_config])
            
    except Exception as e:
        print(f"ERROR: {e}")
        return False


def test_table_processor_dynamic_estimation():
    """Test that table processor can dynamically estimate records based on canonical tables."""
    print("\n=== Testing Table Processor Dynamic Estimation ===")
    
    try:
        # Mock environment variables
        with patch.dict(os.environ, {
            'BUCKET_NAME': 'test-bucket',
            'TENANT_SERVICES_TABLE': 'test-tenant-services',
            'LAST_UPDATED_TABLE': 'test-last-updated',
            'ENVIRONMENT': 'test',
            'AWS_REGION': 'us-east-2'
        }):
            from table_processor import TableProcessor
            
            processor = TableProcessor()
            
            # Test different table configurations
            test_configs = [
                {
                    'table_name': 'tickets',
                    'endpoint': 'service/tickets',
                    'service_name': 'connectwise'
                },
                {
                    'table_name': 'companies',
                    'endpoint': 'Account',
                    'service_name': 'salesforce'
                },
                {
                    'table_name': 'contacts',
                    'endpoint': 'sys_user',
                    'service_name': 'servicenow'
                },
                {
                    'table_name': 'time_entries',
                    'endpoint': 'time/entries',
                    'service_name': 'connectwise'
                }
            ]
            
            success_count = 0
            for config in test_configs:
                try:
                    # Test full sync estimation
                    full_sync_estimate = processor._estimate_total_records(config, True)
                    incremental_estimate = processor._estimate_total_records(config, False)
                    
                    print(f"{config['service_name']}/{config['endpoint']}:")
                    print(f"  Full sync estimate: {full_sync_estimate}")
                    print(f"  Incremental estimate: {incremental_estimate}")
                    
                    if full_sync_estimate > 0 and incremental_estimate > 0:
                        success_count += 1
                        
                except Exception as e:
                    print(f"  ERROR: {e}")
            
            return success_count == len(test_configs)
            
    except Exception as e:
        print(f"ERROR: {e}")
        return False


def test_table_processor_dynamic_chunking():
    """Test that table processor can dynamically calculate chunk sizes."""
    print("\n=== Testing Table Processor Dynamic Chunking ===")
    
    try:
        # Mock environment variables
        with patch.dict(os.environ, {
            'BUCKET_NAME': 'test-bucket',
            'TENANT_SERVICES_TABLE': 'test-tenant-services',
            'LAST_UPDATED_TABLE': 'test-last-updated',
            'ENVIRONMENT': 'test',
            'AWS_REGION': 'us-east-2'
        }):
            from table_processor import TableProcessor
            
            processor = TableProcessor()
            
            # Test different table configurations
            test_configs = [
                {
                    'table_name': 'tickets',
                    'endpoint': 'service/tickets',
                    'service_name': 'connectwise'
                },
                {
                    'table_name': 'companies',
                    'endpoint': 'Account',
                    'service_name': 'salesforce'
                },
                {
                    'table_name': 'time_entries',
                    'endpoint': 'time_card',
                    'service_name': 'servicenow'
                }
            ]
            
            success_count = 0
            for config in test_configs:
                try:
                    # Store config for dynamic functions
                    processor._current_table_config = config
                    
                    # Test chunk size calculation
                    chunk_size = processor._calculate_optimal_chunk_size(config['table_name'], 10000)
                    
                    print(f"{config['service_name']}/{config['endpoint']}:")
                    print(f"  Optimal chunk size: {chunk_size}")
                    
                    if chunk_size > 0:
                        success_count += 1
                        
                except Exception as e:
                    print(f"  ERROR: {e}")
            
            return success_count == len(test_configs)
            
    except Exception as e:
        print(f"ERROR: {e}")
        return False


def test_dynamic_processors():
    """Run all dynamic processor tests."""
    print("Testing Dynamic Processor System")
    print("=" * 50)
    
    tests = [
        ("Tenant Processor Dynamic Service Config", test_tenant_processor_dynamic_service_config),
        ("Table Processor Dynamic Estimation", test_table_processor_dynamic_estimation),
        ("Table Processor Dynamic Chunking", test_table_processor_dynamic_chunking)
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            result = test_func()
            results[test_name] = "PASS" if result else "FAIL"
        except Exception as e:
            print(f"ERROR in {test_name}: {e}")
            results[test_name] = "ERROR"
    
    print("\n" + "=" * 50)
    print("Test Results:")
    for test_name, result in results.items():
        status_symbol = "✓" if result == "PASS" else "✗"
        print(f"{status_symbol} {test_name}: {result}")
    
    passed = sum(1 for r in results.values() if r == "PASS")
    total = len(results)
    print(f"\nPassed: {passed}/{total}")
    
    return passed == total


if __name__ == "__main__":
    success = test_dynamic_processors()
    sys.exit(0 if success else 1)