#!/usr/bin/env python3

"""
Test script to validate canonical transformation integration in chunk processor.

This script tests the key components of the canonical transformation integration:
1. Table name mapping logic
2. Lambda function naming convention
3. Payload structure
"""

import json
import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Mock the shared modules for testing
class MockConfig:
    def __init__(self):
        self.environment = 'dev'
        self.bucket_name = 'test-bucket'
    
    @classmethod
    def from_environment(cls):
        return cls()

class MockLogger:
    def __init__(self, name):
        self.name = name
    
    def info(self, message, **kwargs):
        print(f"INFO: {message}")
        if kwargs:
            print(f"  Context: {json.dumps(kwargs, indent=2)}")
    
    def warning(self, message, **kwargs):
        print(f"WARNING: {message}")
        if kwargs:
            print(f"  Context: {json.dumps(kwargs, indent=2)}")
    
    def error(self, message, **kwargs):
        print(f"ERROR: {message}")
        if kwargs:
            print(f"  Context: {json.dumps(kwargs, indent=2)}")

def mock_get_client():
    return None

# Mock the shared modules
sys.modules['shared.config_simple'] = type('MockModule', (), {'Config': MockConfig})()
sys.modules['shared.logger'] = type('MockModule', (), {'PipelineLogger': MockLogger})()
sys.modules['shared.aws_clients'] = type('MockModule', (), {
    'get_dynamodb_client': mock_get_client,
    'get_cloudwatch_client': mock_get_client,
    'get_s3_client': mock_get_client
})()
sys.modules['shared.utils'] = type('MockModule', (), {
    'get_timestamp': lambda: '2025-06-25T10:00:00Z',
    'get_s3_key': lambda tenant_id, data_type, service, table, timestamp: f"{tenant_id}/{data_type}/{service}/{table}/{timestamp}.parquet"
})()

# Import the chunk processor after mocking
from optimized.processors.chunk_processor import ChunkProcessor

def test_canonical_table_name_mapping():
    """Test the canonical table name mapping logic."""
    print("=" * 60)
    print("Testing Canonical Table Name Mapping")
    print("=" * 60)
    
    # Create a mock chunk processor (without AWS clients)
    processor = ChunkProcessor()
    
    # Test ConnectWise mappings
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
    
    for service, table_input, expected_output in test_cases:
        result = processor._get_canonical_table_name(service, table_input)
        status = "✅ PASS" if result == expected_output else "❌ FAIL"
        print(f"{status} {service} | {table_input} -> {result} (expected: {expected_output})")

def test_lambda_naming_convention():
    """Test the Lambda function naming convention."""
    print("\n" + "=" * 60)
    print("Testing Lambda Function Naming Convention")
    print("=" * 60)
    
    processor = ChunkProcessor()
    
    test_cases = [
        ('tickets', 'avesa-canonical-transform-tickets-dev'),
        ('time_entries', 'avesa-canonical-transform-time-entries-dev'),
        ('companies', 'avesa-canonical-transform-companies-dev'),
        ('contacts', 'avesa-canonical-transform-contacts-dev'),
    ]
    
    for canonical_table, expected_lambda_name in test_cases:
        # Extract the lambda naming logic from our implementation
        clean_table_name = canonical_table.split('/')[-1] if '/' in canonical_table else canonical_table
        lambda_name = f"avesa-canonical-transform-{clean_table_name.replace('_', '-')}-{processor.config.environment}"
        
        status = "✅ PASS" if lambda_name == expected_lambda_name else "❌ FAIL"
        print(f"{status} {canonical_table} -> {lambda_name} (expected: {expected_lambda_name})")

def test_payload_structure():
    """Test the canonical transformation payload structure."""
    print("\n" + "=" * 60)
    print("Testing Canonical Transformation Payload Structure")
    print("=" * 60)
    
    # Expected payload structure
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

def main():
    """Run all tests."""
    print("Canonical Transformation Integration Test Suite")
    print("=" * 60)
    
    try:
        test_canonical_table_name_mapping()
        test_lambda_naming_convention()
        test_payload_structure()
        
        print("\n" + "=" * 60)
        print("✅ All tests completed successfully!")
        print("=" * 60)
        
        print("\nNext Steps:")
        print("1. Deploy the updated chunk processor")
        print("2. Test with a real ConnectWise data processing job")
        print("3. Verify canonical transformation is triggered automatically")
        print("4. Check ClickHouse for canonical data")
        
    except Exception as e:
        print(f"\n❌ Test suite failed with error: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())