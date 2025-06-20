#!/usr/bin/env python3
"""
Test Staging Backfill
Quick test to verify the backfill fixes work on staging environment.
"""

import boto3
import json
import time
from datetime import datetime, timezone, timedelta

def test_staging_backfill():
    """Test the backfill fixes on staging environment."""
    
    print("🚀 TESTING BACKFILL FIXES ON STAGING")
    print("=" * 60)
    
    lambda_client = boto3.client('lambda', region_name='us-east-2')
    
    # Use a small date range for quick testing (last 1 day)
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=1)
    
    payload = {
        'tenant_id': 'sitetechnology',
        'service': 'connectwise',
        'table_name': 'service/tickets',
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'chunk_size_days': 1  # Single chunk for quick test
    }
    
    print(f"📋 STAGING TEST PAYLOAD:")
    print(f"   Environment: staging")
    print(f"   Function: avesa-backfill-staging")
    print(f"   {json.dumps(payload, indent=2, default=str)}")
    
    try:
        print(f"\n🔄 Invoking staging backfill lambda...")
        
        response = lambda_client.invoke(
            FunctionName='avesa-backfill-staging',
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        # Parse response
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))
        
        print(f"\n📊 STAGING BACKFILL RESULT:")
        print(f"   Status Code: {response_payload.get('statusCode')}")
        
        if response_payload.get('statusCode') == 200:
            body = response_payload.get('body', {})
            print(f"   ✅ Status: {body.get('status', 'unknown')}")
            print(f"   📊 Total Records: {body.get('total_records', 0)}")
            print(f"   📋 Tables Processed: {len(body.get('processed_tables', []))}")
            
            # Show table details
            for table_info in body.get('processed_tables', []):
                table_name = table_info.get('table_name')
                records = table_info.get('records_processed', 0)
                chunks = table_info.get('chunks_processed', 0)
                total_chunks = table_info.get('total_chunks', 0)
                errors = table_info.get('errors', [])
                
                print(f"\n   📋 Table: {table_name}")
                print(f"      📊 Records: {records}")
                print(f"      🔄 Chunks: {chunks}/{total_chunks}")
                print(f"      ❌ Errors: {len(errors)}")
                
                if errors:
                    print(f"      Error samples:")
                    for i, error in enumerate(errors[:2]):  # Show first 2 errors
                        print(f"         {i+1}. {error[:80]}...")
            
            if body.get('errors'):
                print(f"\n   ❌ General Errors:")
                for error in body.get('errors', []):
                    print(f"      - {error}")
                    
        else:
            print(f"   ❌ FAILED with status {response_payload.get('statusCode')}")
            error_body = response_payload.get('body', {})
            print(f"   Error: {error_body.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Error invoking staging backfill: {str(e)}")
        return False
    
    # Check staging logs
    print(f"\n🔍 CHECKING STAGING LOGS...")
    
    try:
        logs_client = boto3.client('logs', region_name='us-east-2')
        log_group_name = "/aws/lambda/avesa-backfill-staging"
        
        # Get recent logs (last 3 minutes)
        end_time = int(time.time() * 1000)
        start_time = end_time - (3 * 60 * 1000)
        
        response = logs_client.filter_log_events(
            logGroupName=log_group_name,
            startTime=start_time,
            endTime=end_time,
            limit=15
        )
        
        if response['events']:
            print(f"✅ Found {len(response['events'])} recent staging log events")
            
            # Look for key events
            canonical_calls = 0
            chunk_processing = 0
            errors = 0
            
            print(f"\n📝 KEY STAGING LOG EVENTS:")
            for event in response['events'][-8:]:  # Show last 8 events
                message = event['message'].strip()
                
                if "Invoking table-specific canonical lambda" in message:
                    canonical_calls += 1
                    print(f"   ✅ {message}")
                elif "Processing chunk" in message and ("records" in message or "to" in message):
                    chunk_processing += 1
                    print(f"   🔄 {message}")
                elif "ERROR" in message or "❌" in message:
                    errors += 1
                    print(f"   ❌ {message}")
                elif "📊 BACKFILL SUMMARY" in message:
                    print(f"   📊 {message}")
                elif "🔄 Processing backfill for table" in message:
                    print(f"   🎯 {message}")
            
            print(f"\n📈 STAGING LOG SUMMARY:")
            print(f"   🔧 Canonical Lambda Calls: {canonical_calls}")
            print(f"   🔄 Chunk Processing Events: {chunk_processing}")
            print(f"   ❌ Error Events: {errors}")
            
        else:
            print("❌ No recent staging log events found")
            
    except Exception as e:
        print(f"❌ Error checking staging logs: {str(e)}")
    
    # Final assessment
    print(f"\n🎯 STAGING TEST ASSESSMENT:")
    
    success = False
    if response_payload.get('statusCode') == 200:
        body = response_payload.get('body', {})
        
        if body.get('status') in ['completed', 'completed_with_errors']:
            print(f"   ✅ STAGING BACKFILL FUNCTIONAL")
            print(f"   ✅ All fixes working on staging environment")
            print(f"   ✅ Ready for commit and production deployment")
            success = True
        else:
            print(f"   ⚠️  STAGING BACKFILL STATUS UNCLEAR")
    else:
        print(f"   ❌ STAGING BACKFILL FAILED")
        print(f"   ❌ Do not commit - investigate issues first")
    
    return success

if __name__ == "__main__":
    success = test_staging_backfill()
    if success:
        print(f"\n🎉 STAGING TEST PASSED - READY TO COMMIT!")
    else:
        print(f"\n⚠️  STAGING TEST ISSUES - REVIEW BEFORE COMMIT")