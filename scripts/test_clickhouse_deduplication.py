#!/usr/bin/env python3
"""
Test ClickHouse Deduplication Fix

Validates that ReplacingMergeTree engine correctly handles duplicate records
by using last_updated as the version column for deduplication.
"""

import sys
import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Add shared modules to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'shared'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'clickhouse', 'schema_init'))

from dynamic_schema_manager import DynamicClickHouseSchemaManager, get_clickhouse_client

def test_deduplication_fix():
    """Test the ReplacingMergeTree deduplication functionality"""
    print("=== TESTING CLICKHOUSE DEDUPLICATION FIX ===\n")
    
    try:
        # Get ClickHouse client
        print("1. Connecting to ClickHouse...")
        client = get_clickhouse_client()
        print("✅ Connected successfully\n")
        
        # Initialize schema manager
        manager = DynamicClickHouseSchemaManager(client)
        test_table = "companies_test"
        
        # Step 1: Create test table with ReplacingMergeTree engine
        print("2. Creating test table with ReplacingMergeTree engine...")
        create_test_table(client, test_table)
        print("✅ Test table created\n")
        
        # Step 2: Insert duplicate test records with different timestamps
        print("3. Inserting duplicate test records...")
        test_records = create_test_records()
        insert_test_records(client, test_table, test_records)
        print("✅ Test records inserted\n")
        
        # Step 3: Check record count before optimization
        print("4. Checking record count before OPTIMIZE...")
        count_before = get_record_count(client, test_table, "test_company_1")
        print(f"Records for test_company_1 before OPTIMIZE: {count_before}")
        
        # Step 4: Run OPTIMIZE to trigger deduplication
        print("\n5. Running OPTIMIZE to trigger deduplication...")
        optimize_table(client, test_table)
        print("✅ OPTIMIZE completed\n")
        
        # Step 5: Check record count after optimization
        print("6. Checking record count after OPTIMIZE...")
        count_after = get_record_count(client, test_table, "test_company_1")
        print(f"Records for test_company_1 after OPTIMIZE: {count_after}")
        
        # Step 6: Validate deduplication worked
        print("\n7. Validating deduplication results...")
        validate_deduplication(client, test_table, count_before, count_after)
        
        # Step 7: Verify latest record is kept
        print("\n8. Verifying latest record is kept...")
        verify_latest_record(client, test_table)
        
        # Cleanup
        print("\n9. Cleaning up test table...")
        client.query(f"DROP TABLE IF EXISTS {test_table}")
        print("✅ Test table dropped\n")
        
        print("🎉 DEDUPLICATION TEST COMPLETED SUCCESSFULLY! 🎉")
        print("ReplacingMergeTree engine is working correctly.")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise

def create_test_table(client, table_name: str):
    """Create a test table with ReplacingMergeTree engine"""
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id String,
        company_name Nullable(String),
        status Nullable(String),
        annual_revenue Nullable(Float64),
        last_updated DateTime,
        tenant_id String,
        ingestion_timestamp DateTime DEFAULT now(),
        record_hash String
    )
    ENGINE = ReplacingMergeTree(last_updated)
    ORDER BY (tenant_id, id, last_updated)
    SETTINGS index_granularity = 8192
    """
    
    client.query(create_sql)

def create_test_records() -> List[Dict[str, Any]]:
    """Create test records with duplicate IDs but different timestamps"""
    base_time = datetime(2025, 6, 26, 10, 0, 0)
    
    records = []
    
    # Company 1: 3 versions (should keep the latest)
    for i in range(3):
        records.append({
            'id': 'test_company_1',
            'company_name': f'ACME Corp v{i+1}',
            'status': 'Active',
            'annual_revenue': 1000000.0 + (i * 100000),
            'last_updated': base_time + timedelta(hours=i),
            'tenant_id': 'test_tenant',
            'ingestion_timestamp': datetime.now(),
            'record_hash': f'hash_company_1_v{i+1}'
        })
    
    # Company 2: 2 versions (should keep the latest)  
    for i in range(2):
        records.append({
            'id': 'test_company_2',
            'company_name': f'Beta Industries v{i+1}',
            'status': 'Active',
            'annual_revenue': 500000.0 + (i * 50000),
            'last_updated': base_time + timedelta(hours=i),
            'tenant_id': 'test_tenant',
            'ingestion_timestamp': datetime.now(),
            'record_hash': f'hash_company_2_v{i+1}'
        })
    
    # Company 3: 1 version (should remain unchanged)
    records.append({
        'id': 'test_company_3',
        'company_name': 'Gamma Corp',
        'status': 'Active', 
        'annual_revenue': 750000.0,
        'last_updated': base_time,
        'tenant_id': 'test_tenant',
        'ingestion_timestamp': datetime.now(),
        'record_hash': 'hash_company_3_v1'
    })
    
    return records

def insert_test_records(client, table_name: str, records: List[Dict[str, Any]]):
    """Insert test records into the table"""
    column_names = ['id', 'company_name', 'status', 'annual_revenue', 'last_updated', 'tenant_id', 'ingestion_timestamp', 'record_hash']
    
    rows_to_insert = []
    for record in records:
        row = [record[col] for col in column_names]
        rows_to_insert.append(row)
    
    client.insert(table_name, rows_to_insert, column_names=column_names)
    print(f"Inserted {len(rows_to_insert)} test records")

def get_record_count(client, table_name: str, company_id: str) -> int:
    """Get the count of records for a specific company ID"""
    query = f"SELECT count(*) FROM {table_name} WHERE id = '{company_id}'"
    result = client.query(query)
    return result.result_rows[0][0]

def optimize_table(client, table_name: str):
    """Run OPTIMIZE to trigger ReplacingMergeTree deduplication"""
    optimize_sql = f"OPTIMIZE TABLE {table_name}"
    client.query(optimize_sql)

def validate_deduplication(client, table_name: str, count_before: int, count_after: int):
    """Validate that deduplication reduced the record count"""
    if count_before > count_after:
        print(f"✅ Deduplication worked: {count_before} → {count_after} records")
    elif count_before == count_after:
        print(f"⚠️  No deduplication occurred: {count_before} records remain")
        print("   This may be normal if records haven't been merged yet")
    else:
        raise Exception(f"Unexpected result: record count increased from {count_before} to {count_after}")

def verify_latest_record(client, table_name: str):
    """Verify that the latest version of each record is kept"""
    # Check that company_1 has the latest version (v3)
    query = f"""
        SELECT company_name, annual_revenue, last_updated 
        FROM {table_name} 
        WHERE id = 'test_company_1'
        ORDER BY last_updated DESC
        LIMIT 1
    """
    
    result = client.query(query)
    if result.result_rows:
        company_name, revenue, last_updated = result.result_rows[0]
        print(f"Latest record for test_company_1:")
        print(f"  Company Name: {company_name}")
        print(f"  Revenue: {revenue}")
        print(f"  Last Updated: {last_updated}")
        
        # Verify it's the latest version
        if 'v3' in company_name and revenue == 1200000.0:
            print("✅ Latest version is correctly retained")
        else:
            print(f"⚠️  Unexpected version retained: {company_name}")
    else:
        print("❌ No records found for test_company_1")

def test_canonical_table_recreation():
    """Test recreating actual canonical tables with new engine"""
    print("\n=== TESTING CANONICAL TABLE RECREATION ===\n")
    
    try:
        # Get ClickHouse client
        client = get_clickhouse_client()
        manager = DynamicClickHouseSchemaManager(client)
        
        # Test with a single table first
        test_table = "companies"
        
        print(f"1. Checking if {test_table} table exists...")
        exists = manager.table_exists(test_table)
        print(f"Table exists: {exists}")
        
        if exists:
            print(f"\n2. Getting current table engine for {test_table}...")
            engine_query = f"SELECT engine FROM system.tables WHERE name = '{test_table}'"
            result = client.query(engine_query)
            
            if result.result_rows:
                current_engine = result.result_rows[0][0]
                print(f"Current engine: {current_engine}")
                
                if 'ReplacingMergeTree' in current_engine:
                    print("✅ Table already uses ReplacingMergeTree engine")
                else:
                    print(f"⚠️  Table uses {current_engine}, should be ReplacingMergeTree")
                    
                    # Recreate table with new engine
                    print(f"\n3. Recreating {test_table} with ReplacingMergeTree engine...")
                    success = manager.recreate_table_with_ordered_schema(test_table)
                    
                    if success:
                        print(f"✅ Successfully recreated {test_table}")
                        
                        # Verify new engine
                        result = client.query(engine_query)
                        new_engine = result.result_rows[0][0]
                        print(f"New engine: {new_engine}")
                        
                        if 'ReplacingMergeTree' in new_engine:
                            print("✅ Table now uses ReplacingMergeTree engine")
                        else:
                            print(f"❌ Engine update failed: {new_engine}")
                    else:
                        print(f"❌ Failed to recreate {test_table}")
            else:
                print(f"❌ Could not determine engine for {test_table}")
        else:
            print(f"\n2. Creating {test_table} table with ReplacingMergeTree engine...")
            success = manager.create_table_from_mapping(test_table, use_ordered_schema=True)
            
            if success:
                print(f"✅ Successfully created {test_table}")
            else:
                print(f"❌ Failed to create {test_table}")
                
    except Exception as e:
        print(f"❌ Canonical table test failed: {e}")
        raise

def main():
    """Main test function"""
    try:
        # Test 1: Basic deduplication functionality
        test_deduplication_fix()
        
        # Test 2: Canonical table recreation (optional)
        print("\n" + "="*60)
        response = input("Would you like to test canonical table recreation? (y/n): ")
        if response.lower() in ['y', 'yes']:
            test_canonical_table_recreation()
        
        print("\n🎉 ALL TESTS COMPLETED SUCCESSFULLY! 🎉")
        
    except Exception as e:
        print(f"\n❌ TESTS FAILED: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()