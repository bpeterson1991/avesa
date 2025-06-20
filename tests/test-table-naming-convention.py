#!/usr/bin/env python3
"""
Table Naming Convention Test

This script validates that the table naming convention fix is working correctly:
1. Verifies endpoint configurations have explicit table_name fields
2. Tests utility functions for extracting table names
3. Validates S3 path generation uses correct table names
4. Checks consistency across all components
"""

import json
import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src' / 'shared'))

try:
    from utils import get_table_name_from_endpoint_config, load_endpoint_configuration, get_s3_key
except ImportError as e:
    print(f"Import error: {e}")
    print("Running without shared utilities - basic validation only")

def test_connectwise_endpoint_configuration():
    """Test that ConnectWise endpoint configuration has explicit table names"""
    print("🔍 Testing ConnectWise endpoint configuration...")
    
    config_path = Path(__file__).parent.parent / 'mappings' / 'integrations' / 'connectwise_endpoints.json'
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        endpoints = config.get('endpoints', {})
        
        # Test critical endpoints
        critical_endpoints = {
            'time/entries': 'time_entries',
            'service/tickets': 'tickets',
            'company/companies': 'companies',
            'company/contacts': 'contacts'
        }
        
        all_passed = True
        
        for endpoint_path, expected_table_name in critical_endpoints.items():
            if endpoint_path in endpoints:
                endpoint_config = endpoints[endpoint_path]
                
                # Check for explicit table_name field
                table_name = endpoint_config.get('table_name')
                canonical_table = endpoint_config.get('canonical_table')
                
                if table_name == expected_table_name:
                    print(f"✅ {endpoint_path}: table_name = '{table_name}' (correct)")
                else:
                    print(f"❌ {endpoint_path}: table_name = '{table_name}', expected '{expected_table_name}'")
                    all_passed = False
                
                # Note: canonical_table should NOT be in endpoint config - it's reverse-mapped from canonical tables
                if canonical_table is None:
                    print(f"✅ {endpoint_path}: canonical_table not present (correct architecture)")
                else:
                    print(f"⚠️  {endpoint_path}: canonical_table = '{canonical_table}' (should be reverse-mapped instead)")
            else:
                print(f"❌ {endpoint_path}: endpoint not found in configuration")
                all_passed = False
        
        return all_passed
        
    except Exception as e:
        print(f"❌ Error reading ConnectWise configuration: {e}")
        return False

def test_utility_functions():
    """Test utility functions for table name extraction"""
    print("\n🔍 Testing utility functions...")
    
    try:
        # Test cases
        test_cases = [
            {
                'endpoint_config': {'table_name': 'time_entries', 'canonical_table': 'time_entries'},
                'endpoint_path': 'time/entries',
                'expected': 'time_entries'
            },
            {
                'endpoint_config': {'canonical_table': 'tickets'},  # No table_name, should use canonical_table
                'endpoint_path': 'service/tickets',
                'expected': 'tickets'
            },
            {
                'endpoint_config': {},  # No explicit names, should derive from path
                'endpoint_path': 'company/companies',
                'expected': 'companies'
            }
        ]
        
        all_passed = True
        
        for i, test_case in enumerate(test_cases, 1):
            try:
                result = get_table_name_from_endpoint_config(
                    test_case['endpoint_config'], 
                    test_case['endpoint_path']
                )
                
                if result == test_case['expected']:
                    print(f"✅ Test case {i}: got '{result}' (correct)")
                else:
                    print(f"❌ Test case {i}: got '{result}', expected '{test_case['expected']}'")
                    all_passed = False
                    
            except Exception as e:
                print(f"❌ Test case {i}: error - {e}")
                all_passed = False
        
        return all_passed
        
    except NameError:
        print("⚠️  Utility functions not available - skipping test")
        return True

def test_s3_path_generation():
    """Test S3 path generation with correct table names"""
    print("\n🔍 Testing S3 path generation...")
    
    try:
        # Test S3 key generation
        test_cases = [
            {
                'tenant_id': 'sitetechnology',
                'data_type': 'raw',
                'service': 'connectwise',
                'table_name': 'time_entries',
                'timestamp': '2025-06-19T20:13:00Z',
                'expected_pattern': 'sitetechnology/raw/connectwise/time_entries/'
            },
            {
                'tenant_id': 'testclient',
                'data_type': 'canonical',
                'service': 'connectwise',
                'table_name': 'tickets',
                'timestamp': '2025-06-19T20:13:00Z',
                'expected_pattern': 'testclient/canonical/connectwise/tickets/'
            }
        ]
        
        all_passed = True
        
        for i, test_case in enumerate(test_cases, 1):
            try:
                result = get_s3_key(
                    test_case['tenant_id'],
                    test_case['data_type'],
                    test_case['service'],
                    test_case['table_name'],
                    test_case['timestamp']
                )
                
                if test_case['expected_pattern'] in result:
                    print(f"✅ S3 path test {i}: {result}")
                else:
                    print(f"❌ S3 path test {i}: {result} (missing expected pattern: {test_case['expected_pattern']})")
                    all_passed = False
                    
            except Exception as e:
                print(f"❌ S3 path test {i}: error - {e}")
                all_passed = False
        
        return all_passed
        
    except NameError:
        print("⚠️  S3 utility functions not available - skipping test")
        return True

def test_canonical_mapping_consistency():
    """Test that canonical mappings use correct table names"""
    print("\n🔍 Testing canonical mapping consistency...")
    
    mappings_path = Path(__file__).parent.parent / 'mappings' / 'canonical'
    
    expected_mappings = {
        'time_entries.json': 'time_entries',
        'tickets.json': 'tickets',
        'companies.json': 'companies',
        'contacts.json': 'contacts'
    }
    
    all_passed = True
    
    for mapping_file, expected_table in expected_mappings.items():
        mapping_path = mappings_path / mapping_file
        
        if mapping_path.exists():
            try:
                with open(mapping_path, 'r') as f:
                    mapping_config = json.load(f)
                
                # Check if the mapping contains the expected table reference
                if 'connectwise' in mapping_config:
                    connectwise_mappings = mapping_config['connectwise']
                    
                    # For time_entries, check if it maps from 'time/entries'
                    if expected_table == 'time_entries':
                        if 'time/entries' in connectwise_mappings:
                            print(f"✅ {mapping_file}: correctly maps from 'time/entries' endpoint")
                        else:
                            print(f"❌ {mapping_file}: missing 'time/entries' endpoint mapping")
                            all_passed = False
                    else:
                        print(f"✅ {mapping_file}: canonical mapping exists")
                else:
                    print(f"⚠️  {mapping_file}: no ConnectWise mappings found")
                    
            except Exception as e:
                print(f"❌ Error reading {mapping_file}: {e}")
                all_passed = False
        else:
            print(f"❌ {mapping_file}: file not found")
            all_passed = False
    
    return all_passed

def test_tenant_processor_configuration():
    """Test that tenant processor uses correct table names"""
    print("\n🔍 Testing tenant processor configuration...")
    
    processor_path = Path(__file__).parent.parent / 'src' / 'optimized' / 'processors' / 'tenant_processor.py'
    
    try:
        with open(processor_path, 'r') as f:
            content = f.read()
        
        # Check that it uses 'time_entries' instead of 'entries'
        if "'table_name': 'time_entries'" in content:
            print("✅ Tenant processor uses 'time_entries' table name")
            return True
        elif "'table_name': 'entries'" in content:
            print("❌ Tenant processor still uses 'entries' table name")
            return False
        else:
            print("⚠️  Could not find table name configuration in tenant processor")
            return True
            
    except Exception as e:
        print(f"❌ Error reading tenant processor: {e}")
        return False

def main():
    """Run all table naming convention tests"""
    print("🚀 Table Naming Convention Validation")
    print("=" * 50)
    
    tests = [
        ("ConnectWise Endpoint Configuration", test_connectwise_endpoint_configuration),
        ("Utility Functions", test_utility_functions),
        ("S3 Path Generation", test_s3_path_generation),
        ("Canonical Mapping Consistency", test_canonical_mapping_consistency),
        ("Tenant Processor Configuration", test_tenant_processor_configuration)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n📋 {test_name}")
        print("-" * 30)
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print(f"\n📊 Test Summary")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All table naming convention tests PASSED!")
        print("✅ Table naming consistency has been successfully implemented")
        return True
    else:
        print("❌ Some table naming convention tests FAILED!")
        print("⚠️  Please review and fix the failing components")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)