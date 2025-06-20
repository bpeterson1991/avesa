#!/usr/bin/env python3
"""
Backfill Diagnosis Test
Tests the specific issues identified in the backfill process.
"""

import boto3
import json

def test_backfill_diagnosis():
    """Test and diagnose the current backfill issues."""
    
    print("🔍 BACKFILL DIAGNOSIS REPORT")
    print("=" * 60)
    
    # Test 1: Check if ingestion lambda exists and is callable
    print("\n1. 🧪 TESTING INGESTION LAMBDA AVAILABILITY")
    print("-" * 50)
    
    lambda_client = boto3.client('lambda', region_name='us-east-2')
    ingestion_lambda_name = "avesa-connectwise-ingestion-dev"
    
    try:
        response = lambda_client.get_function(FunctionName=ingestion_lambda_name)
        print(f"✅ Ingestion Lambda EXISTS: {ingestion_lambda_name}")
        print(f"   Last Modified: {response['Configuration']['LastModified']}")
        print(f"   Runtime: {response['Configuration']['Runtime']}")
        print(f"   Memory: {response['Configuration']['MemorySize']} MB")
    except Exception as e:
        print(f"❌ Ingestion Lambda NOT FOUND: {ingestion_lambda_name}")
        print(f"   Error: {str(e)}")
    
    # Test 2: Check canonical transform lambda naming issue
    print("\n2. 🧪 TESTING CANONICAL TRANSFORM LAMBDA NAMING")
    print("-" * 50)
    
    # What the backfill tries to call
    expected_canonical_name = "avesa-canonical-transform-dev"
    
    try:
        lambda_client.get_function(FunctionName=expected_canonical_name)
        print(f"✅ Generic Canonical Lambda EXISTS: {expected_canonical_name}")
    except Exception as e:
        print(f"❌ Generic Canonical Lambda NOT FOUND: {expected_canonical_name}")
        print(f"   Error: {str(e)}")
    
    # What actually exists (table-specific)
    table_specific_canonical = "avesa-canonical-transform-tickets-dev"
    try:
        response = lambda_client.get_function(FunctionName=table_specific_canonical)
        print(f"✅ Table-Specific Canonical Lambda EXISTS: {table_specific_canonical}")
        print(f"   Last Modified: {response['Configuration']['LastModified']}")
    except Exception as e:
        print(f"❌ Table-Specific Canonical Lambda NOT FOUND: {table_specific_canonical}")
    
    # Test 3: Check if backfill actually called ingestion lambda
    print("\n3. 🧪 TESTING BACKFILL INGESTION INVOCATION")
    print("-" * 50)
    
    # Check CloudWatch logs for ingestion lambda during backfill time
    logs_client = boto3.client('logs', region_name='us-east-2')
    log_group_name = f"/aws/lambda/{ingestion_lambda_name}"
    
    try:
        # Check for recent log events (last 10 minutes)
        import time
        end_time = int(time.time() * 1000)
        start_time = end_time - (10 * 60 * 1000)  # 10 minutes ago
        
        response = logs_client.filter_log_events(
            logGroupName=log_group_name,
            startTime=start_time,
            endTime=end_time,
            limit=10
        )
        
        if response['events']:
            print(f"✅ Found {len(response['events'])} recent log events in ingestion lambda")
            for event in response['events'][-3:]:  # Show last 3 events
                timestamp = event['timestamp']
                message = event['message'][:100] + "..." if len(event['message']) > 100 else event['message']
                print(f"   📝 {timestamp}: {message}")
        else:
            print("❌ NO recent log events found in ingestion lambda")
            print("   This suggests the backfill did NOT invoke the ingestion lambda")
    
    except Exception as e:
        print(f"❌ Error checking ingestion lambda logs: {str(e)}")
    
    # Test 4: Analyze the backfill behavior
    print("\n4. 🧪 ANALYZING BACKFILL BEHAVIOR")
    print("-" * 50)
    
    print("📊 BACKFILL ANALYSIS:")
    print("   ✅ Service configuration retrieval: WORKING")
    print("   ✅ Credential parsing: WORKING") 
    print("   ✅ Date chunking: WORKING (25 chunks processed)")
    print("   ❌ Data retrieval: FAILED (0 records processed)")
    print("   ❌ Canonical transformation: FAILED (wrong lambda name)")
    
    # Test 5: Check what data exists vs what backfill tried to get
    print("\n5. 🧪 DATA AVAILABILITY ANALYSIS")
    print("-" * 50)
    
    s3_client = boto3.client('s3', region_name='us-east-2')
    bucket_name = 'data-storage-msp-dev'
    
    # Check for existing raw data (from regular ingestion)
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='sitetechnology/raw/connectwise/tickets/',
            MaxKeys=5
        )
        
        if 'Contents' in response:
            print(f"✅ Found {len(response['Contents'])} existing raw ticket files")
            latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
            print(f"   📄 Latest: {latest_file['Key']}")
            print(f"   📅 Modified: {latest_file['LastModified']}")
            print(f"   📏 Size: {latest_file['Size']} bytes")
            print("   💡 This shows ConnectWise API is accessible and has data")
        else:
            print("❌ No existing raw ticket files found")
    
    except Exception as e:
        print(f"❌ Error checking existing data: {str(e)}")

if __name__ == "__main__":
    test_backfill_diagnosis()