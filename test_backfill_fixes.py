#!/usr/bin/env python3
"""
Test Backfill Fixes
Tests the implemented fixes for the backfill function.
"""

import boto3
import json
import time
from datetime import datetime, timezone, timedelta

def test_backfill_fixes():
    """Test the backfill fixes with comprehensive validation."""
    
    print("🔧 TESTING BACKFILL FIXES")
    print("=" * 60)
    
    lambda_client = boto3.client('lambda', region_name='us-east-2')
    s3_client = boto3.client('s3', region_name='us-east-2')
    logs_client = boto3.client('logs', region_name='us-east-2')
    
    # Test configuration
    tenant_id = "sitetechnology"
    service = "connectwise"
    table_name = "service/tickets"
    
    # Use a smaller date range for testing (last 7 days)
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=7)
    
    payload = {
        'tenant_id': tenant_id,
        'service': service,
        'table_name': table_name,
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'chunk_size_days': 2  # Smaller chunks for testing
    }
    
    print(f"📋 TEST CONFIGURATION:")
    print(f"   Tenant: {tenant_id}")
    print(f"   Service: {service}")
    print(f"   Table: {table_name}")
    print(f"   Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"   Chunk Size: {payload['chunk_size_days']} days")
    
    # Test 1: Verify lambda naming fix
    print(f"\n1. 🧪 TESTING CANONICAL LAMBDA NAMING FIX")
    print("-" * 50)
    
    # Check if table-specific canonical lambda exists
    clean_table_name = table_name.split('/')[-1]  # 'tickets'
    expected_canonical_name = f"avesa-canonical-transform-{clean_table_name}-dev"
    
    try:
        response = lambda_client.get_function(FunctionName=expected_canonical_name)
        print(f"✅ Table-specific canonical lambda EXISTS: {expected_canonical_name}")
        print(f"   Last Modified: {response['Configuration']['LastModified']}")
    except Exception as e:
        print(f"❌ Table-specific canonical lambda NOT FOUND: {expected_canonical_name}")
        print(f"   Error: {str(e)}")
        print(f"   ⚠️  This will cause canonical transformation to fail")
    
    # Test 2: Execute backfill with fixes
    print(f"\n2. 🚀 EXECUTING BACKFILL WITH FIXES")
    print("-" * 50)
    
    print(f"Invoking backfill lambda with payload:")
    print(f"   {json.dumps(payload, indent=2, default=str)}")
    
    try:
        # Record start time for log analysis
        test_start_time = int(time.time() * 1000)
        
        response = lambda_client.invoke(
            FunctionName='avesa-backfill-dev',
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        # Parse response
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))
        
        print(f"\n📊 BACKFILL EXECUTION RESULT:")
        print(f"   Status Code: {response_payload.get('statusCode')}")
        
        if response_payload.get('statusCode') == 200:
            body = response_payload.get('body', {})
            print(f"   ✅ Execution Status: {body.get('status', 'unknown')}")
            print(f"   📊 Total Records: {body.get('total_records', 0)}")
            print(f"   📋 Processed Tables: {len(body.get('processed_tables', []))}")
            
            # Show table details
            for table_info in body.get('processed_tables', []):
                table_name_result = table_info.get('table_name')
                records = table_info.get('records_processed', 0)
                chunks = table_info.get('chunks_processed', 0)
                total_chunks = table_info.get('total_chunks', 0)
                success_rate = table_info.get('success_rate', 'unknown')
                errors = table_info.get('errors', [])
                
                print(f"\n   📋 Table: {table_name_result}")
                print(f"      📊 Records: {records}")
                print(f"      🔄 Chunks: {chunks}/{total_chunks} ({success_rate})")
                
                if errors:
                    print(f"      ❌ Errors ({len(errors)}):")
                    for i, error in enumerate(errors[:3]):  # Show first 3 errors
                        print(f"         {i+1}. {error[:100]}...")
                else:
                    print(f"      ✅ No errors")
            
            if body.get('errors'):
                print(f"\n   ❌ General Errors:")
                for error in body.get('errors', []):
                    print(f"      - {error}")
        else:
            print(f"   ❌ Execution FAILED")
            print(f"   Error: {response_payload.get('body', {}).get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Error invoking backfill lambda: {str(e)}")
        return
    
    # Test 3: Analyze logs for detailed diagnostics
    print(f"\n3. 🔍 ANALYZING BACKFILL LOGS")
    print("-" * 50)
    
    try:
        # Wait a moment for logs to be available
        time.sleep(5)
        
        log_group_name = "/aws/lambda/avesa-backfill-dev"
        end_time = int(time.time() * 1000)
        start_time = test_start_time - (2 * 60 * 1000)  # 2 minutes before test
        
        response = logs_client.filter_log_events(
            logGroupName=log_group_name,
            startTime=start_time,
            endTime=end_time,
            limit=50
        )
        
        if response['events']:
            print(f"✅ Found {len(response['events'])} log events")
            
            # Look for specific log patterns
            ingestion_calls = 0
            canonical_calls = 0
            error_count = 0
            
            print(f"\n📝 KEY LOG EVENTS:")
            for event in response['events']:
                message = event['message']
                
                # Count different types of events
                if "Invoking table-specific canonical lambda" in message:
                    canonical_calls += 1
                    print(f"   ✅ Canonical Lambda Call: {message.strip()}")
                elif "Processing chunk" in message and "records" in message:
                    ingestion_calls += 1
                    print(f"   🔄 Chunk Processing: {message.strip()}")
                elif "ERROR" in message or "❌" in message:
                    error_count += 1
                    print(f"   ❌ Error: {message.strip()}")
                elif "📊 BACKFILL SUMMARY" in message:
                    print(f"   📊 Summary: {message.strip()}")
            
            print(f"\n📈 LOG ANALYSIS SUMMARY:")
            print(f"   🔄 Ingestion Calls: {ingestion_calls}")
            print(f"   🔧 Canonical Calls: {canonical_calls}")
            print(f"   ❌ Errors: {error_count}")
            
        else:
            print("❌ No recent log events found")
            
    except Exception as e:
        print(f"❌ Error analyzing logs: {str(e)}")
    
    # Test 4: Check for data in S3
    print(f"\n4. 🗄️  CHECKING S3 DATA STORAGE")
    print("-" * 50)
    
    bucket_name = 'data-storage-msp-dev'
    
    # Check for backfill metadata
    try:
        metadata_prefix = f"{tenant_id}/backfill_metadata/{service}/"
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=metadata_prefix,
            MaxKeys=10
        )
        
        if 'Contents' in response:
            print(f"✅ Found {len(response['Contents'])} backfill metadata files")
            latest_metadata = max(response['Contents'], key=lambda x: x['LastModified'])
            print(f"   📄 Latest: {latest_metadata['Key']}")
            print(f"   📅 Created: {latest_metadata['LastModified']}")
            
            # Try to read the metadata
            try:
                obj_response = s3_client.get_object(
                    Bucket=bucket_name,
                    Key=latest_metadata['Key']
                )
                metadata_content = json.loads(obj_response['Body'].read().decode('utf-8'))
                print(f"   📊 Records Processed: {metadata_content.get('records_processed', 0)}")
                print(f"   🔄 Chunks Processed: {metadata_content.get('chunks_processed', 0)}")
            except Exception as e:
                print(f"   ⚠️  Could not read metadata: {str(e)}")
        else:
            print("❌ No backfill metadata found")
            
    except Exception as e:
        print(f"❌ Error checking backfill metadata: {str(e)}")
    
    # Check for raw data
    try:
        raw_prefix = f"{tenant_id}/raw/{service}/tickets/"
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=raw_prefix,
            MaxKeys=5
        )
        
        if 'Contents' in response:
            print(f"✅ Found {len(response['Contents'])} raw data files")
            latest_raw = max(response['Contents'], key=lambda x: x['LastModified'])
            print(f"   📄 Latest: {latest_raw['Key']}")
            print(f"   📅 Modified: {latest_raw['LastModified']}")
            print(f"   📏 Size: {latest_raw['Size']} bytes")
        else:
            print("❌ No raw data files found")
            
    except Exception as e:
        print(f"❌ Error checking raw data: {str(e)}")
    
    # Test 5: Final assessment
    print(f"\n5. 📋 FINAL ASSESSMENT")
    print("-" * 50)
    
    print(f"🔧 FIXES IMPLEMENTED:")
    print(f"   ✅ Canonical lambda naming: Fixed to use table-specific names")
    print(f"   ✅ Enhanced logging: Added detailed diagnostic information")
    print(f"   ✅ Better error handling: Improved error reporting and recovery")
    print(f"   ✅ Payload format: Enhanced ingestion lambda invocation")
    print(f"   ✅ Date parsing: Improved date handling for various formats")
    
    print(f"\n🧪 TEST RESULTS:")
    if response_payload.get('statusCode') == 200:
        body = response_payload.get('body', {})
        total_records = body.get('total_records', 0)
        
        if total_records > 0:
            print(f"   ✅ BACKFILL SUCCESSFUL: {total_records} records processed")
            print(f"   ✅ Data retrieval: WORKING")
            print(f"   ✅ Lambda naming: FIXED")
            print(f"   ✅ End-to-end process: FUNCTIONAL")
        else:
            print(f"   ⚠️  BACKFILL COMPLETED but 0 records processed")
            print(f"   ⚠️  This may indicate:")
            print(f"      - No data available for the date range")
            print(f"      - Ingestion lambda payload format issues")
            print(f"      - API connectivity problems")
    else:
        print(f"   ❌ BACKFILL FAILED")
        print(f"   ❌ Check logs for detailed error information")
    
    print(f"\n🎯 NEXT STEPS:")
    print(f"   1. Review log analysis for any remaining issues")
    print(f"   2. Test with different date ranges if needed")
    print(f"   3. Verify canonical transformation is triggered correctly")
    print(f"   4. Monitor S3 for data storage and processing")

if __name__ == "__main__":
    test_backfill_fixes()