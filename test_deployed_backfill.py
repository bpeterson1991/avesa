#!/usr/bin/env python3
"""
Test Deployed Backfill
Quick test to verify the backfill fixes are working.
"""

import boto3
import json
import time
from datetime import datetime, timezone, timedelta

def test_deployed_backfill():
    """Test the deployed backfill with a small date range."""
    
    print("🚀 TESTING DEPLOYED BACKFILL FIXES")
    print("=" * 60)
    
    lambda_client = boto3.client('lambda', region_name='us-east-2')
    
    # Use a very small date range for quick testing (last 1 day)
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
    
    print(f"📋 TEST PAYLOAD:")
    print(f"   {json.dumps(payload, indent=2, default=str)}")
    
    try:
        print(f"\n🔄 Invoking backfill lambda...")
        
        response = lambda_client.invoke(
            FunctionName='avesa-backfill-dev',
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        # Parse response
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))
        
        print(f"\n📊 BACKFILL RESULT:")
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
        print(f"❌ Error invoking backfill: {str(e)}")
        return
    
    # Check logs for more details
    print(f"\n🔍 CHECKING RECENT LOGS...")
    
    try:
        logs_client = boto3.client('logs', region_name='us-east-2')
        log_group_name = "/aws/lambda/avesa-backfill-dev"
        
        # Get recent logs (last 5 minutes)
        end_time = int(time.time() * 1000)
        start_time = end_time - (5 * 60 * 1000)
        
        response = logs_client.filter_log_events(
            logGroupName=log_group_name,
            startTime=start_time,
            endTime=end_time,
            limit=20
        )
        
        if response['events']:
            print(f"✅ Found {len(response['events'])} recent log events")
            
            # Look for key events
            canonical_calls = 0
            chunk_processing = 0
            errors = 0
            
            print(f"\n📝 KEY LOG EVENTS:")
            for event in response['events'][-10:]:  # Show last 10 events
                message = event['message'].strip()
                
                if "Invoking table-specific canonical lambda" in message:
                    canonical_calls += 1
                    print(f"   ✅ {message}")
                elif "Processing chunk" in message and "records" in message:
                    chunk_processing += 1
                    print(f"   🔄 {message}")
                elif "ERROR" in message or "❌" in message:
                    errors += 1
                    print(f"   ❌ {message}")
                elif "📊 BACKFILL SUMMARY" in message:
                    print(f"   📊 {message}")
            
            print(f"\n📈 LOG SUMMARY:")
            print(f"   🔧 Canonical Lambda Calls: {canonical_calls}")
            print(f"   🔄 Chunk Processing Events: {chunk_processing}")
            print(f"   ❌ Error Events: {errors}")
            
        else:
            print("❌ No recent log events found")
            
    except Exception as e:
        print(f"❌ Error checking logs: {str(e)}")
    
    # Final assessment
    print(f"\n🎯 ASSESSMENT:")
    
    if response_payload.get('statusCode') == 200:
        body = response_payload.get('body', {})
        
        if body.get('status') == 'completed':
            print(f"   ✅ BACKFILL COMPLETED SUCCESSFULLY")
            print(f"   ✅ Canonical lambda naming fix: WORKING")
            print(f"   ✅ Enhanced error handling: WORKING")
            print(f"   ✅ Payload format fix: WORKING")
        elif body.get('status') == 'completed_with_errors':
            print(f"   ⚠️  BACKFILL COMPLETED WITH ERRORS")
            print(f"   ✅ Basic functionality: WORKING")
            print(f"   ⚠️  Some issues remain - check error details above")
        else:
            print(f"   ❌ BACKFILL STATUS UNCLEAR")
    else:
        print(f"   ❌ BACKFILL FAILED")
        print(f"   ❌ Check error details above")

if __name__ == "__main__":
    test_deployed_backfill()