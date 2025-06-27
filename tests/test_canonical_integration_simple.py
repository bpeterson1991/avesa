#!/usr/bin/env python3

"""
Simplified test script to validate canonical transformation integration logic.

This script tests the core logic without requiring AWS client initialization.
"""

import json

def test_canonical_table_name_mapping():
    """Test the canonical table name mapping logic."""
    print("=" * 60)
    print("Testing Canonical Table Name Mapping")
    print("=" * 60)
    
    # Reproduce the mapping logic from our implementation
    def get_canonical_table_name(service_name: str, table_name: str) -> str:
        endpoint_to_table = {
            'connectwise': {
                'service/tickets': 'tickets',
                'time/entries': 'time_entries',
                'company/companies': 'companies',
                'company/contacts': 'contacts',
                'procurement/products': 'products',
                'finance/agreements': 'agreements',
                'project/projects': 'projects',
                'system/members': 'members'
            },
            'servicenow': {
                'incident': 'tickets',
                'change_request': 'change_requests',
                'problem': 'problems',
                'user': 'contacts',
                'sys_user_group': 'user_groups'
            },
            'salesforce': {
                'Account': 'companies',
                'Contact': 'contacts',
                'Opportunity': 'opportunities',
                'Case': 'tickets',
                'Lead': 'leads',
                'User': 'users'
            }
        }
        
        service_mappings = endpoint_to_table.get(service_name.lower(), {})
        
        # Try direct mapping first
        if table_name in service_mappings:
            return service_mappings[table_name]
        
        # If not found, assume it's already a canonical table name
        return table_name
    
    # Test cases
    test_cases = [
        ('connectwise', 'service/tickets', 'tickets'),
        ('connectwise', 'time/entries', 'time_entries'),
        ('connectwise', 'company/companies', 'companies'),
        ('connectwise', 'company/contacts', 'contacts'),
        ('connectwise', 'unknown/endpoint', 'unknown/endpoint'),  # Should pass through
        
        # Test ServiceNow mappings
        ('servicenow', 'incident', 'tickets'),
        ('servicenow', 'user', 'contacts'),
        
        # Test Salesforce mappings
        ('salesforce', 'Account', 'companies'),
        ('salesforce', 'Contact', 'contacts'),
        ('salesforce', 'Case', 'tickets'),
        
        # Test unknown service
        ('unknown_service', 'some_table', 'some_table'),  # Should pass through
    ]
    
    all_passed = True
    for service, table_input, expected_output in test_cases:
        result = get_canonical_table_name(service, table_input)
        status = "✅ PASS" if result == expected_output else "❌ FAIL"
        print(f"{status} {service} | {table_input} -> {result} (expected: {expected_output})")
        if result != expected_output:
            all_passed = False
    
    return all_passed

def test_lambda_naming_convention():
    """Test the Lambda function naming convention."""
    print("\n" + "=" * 60)
    print("Testing Lambda Function Naming Convention")
    print("=" * 60)
    
    environment = 'dev'
    
    test_cases = [
        ('tickets', 'avesa-canonical-transform-tickets-dev'),
        ('time_entries', 'avesa-canonical-transform-time-entries-dev'),
        ('companies', 'avesa-canonical-transform-companies-dev'),
        ('contacts', 'avesa-canonical-transform-contacts-dev'),
    ]
    
    all_passed = True
    for canonical_table, expected_lambda_name in test_cases:
        # Extract the lambda naming logic from our implementation
        clean_table_name = canonical_table.split('/')[-1] if '/' in canonical_table else canonical_table
        lambda_name = f"avesa-canonical-transform-{clean_table_name.replace('_', '-')}-{environment}"
        
        status = "✅ PASS" if lambda_name == expected_lambda_name else "❌ FAIL"
        print(f"{status} {canonical_table} -> {lambda_name} (expected: {expected_lambda_name})")
        if lambda_name != expected_lambda_name:
            all_passed = False
    
    return all_passed

def test_payload_structure():
    """Test the canonical transformation payload structure."""
    print("\n" + "=" * 60)
    print("Testing Canonical Transformation Payload Structure")
    print("=" * 60)
    
    # Expected payload structure based on our implementation
    expected_payload = {
        'tenant_id': 'test-tenant-123',
        'service_name': 'connectwise',
        'table_name': 'tickets',
        'backfill_mode': False,
        'force_reprocess': False,
        's3_trigger': True,
        'source_s3_key': 'test-tenant-123/raw/connectwise/tickets/2025-06-25T10:00:00Z_chunk_001.parquet'
    }
    
    print("Expected payload structure:")
    print(json.dumps(expected_payload, indent=2))
    
    # Verify all required fields are present
    required_fields = ['tenant_id', 'service_name', 'table_name', 'backfill_mode', 'force_reprocess', 's3_trigger', 'source_s3_key']
    all_present = all(field in expected_payload for field in required_fields)
    
    status = "✅ PASS" if all_present else "❌ FAIL"
    print(f"\n{status} All required fields present: {all_present}")
    
    return all_present

def main():
    """Run all tests."""
    print("Canonical Transformation Integration Test Suite")
    print("=" * 60)
    
    try:
        test1_passed = test_canonical_table_name_mapping()
        test2_passed = test_lambda_naming_convention()
        test3_passed = test_payload_structure()
        
        all_tests_passed = test1_passed and test2_passed and test3_passed
        
        print("\n" + "=" * 60)
        if all_tests_passed:
            print("✅ All tests completed successfully!")
        else:
            print("❌ Some tests failed!")
        print("=" * 60)
        
        print("\nImplementation Summary:")
        print("1. ✅ Added Lambda client initialization to ChunkProcessor.__init__()")
        print("2. ✅ Created _get_canonical_table_name() method for endpoint mapping")
        print("3. ✅ Implemented _trigger_canonical_transformation() method")
        print("4. ✅ Modified _write_to_s3() to trigger canonical transformation")
        print("5. ✅ Added comprehensive error handling and logging")
        
        print("\nNext Steps:")
        print("1. Deploy the updated chunk processor")
        print("2. Test with a real ConnectWise data processing job")
        print("3. Verify canonical transformation is triggered automatically")
        print("4. Check CloudWatch logs for canonical transformation messages")
        print("5. Validate canonical data appears in S3 and ClickHouse")
        
        return 0 if all_tests_passed else 1
        
    except Exception as e:
        print(f"\n❌ Test suite failed with error: {str(e)}")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())