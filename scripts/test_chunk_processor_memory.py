#!/usr/bin/env python3
"""
Test script to validate memory usage patterns in chunk processor.
Tests different batch sizes and record counts to identify memory inefficiencies.
"""

import json
import time
import gc
import sys
import os
from datetime import datetime
from typing import List, Dict, Any
from multiprocessing import Process, Queue
import random
import string

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import psutil
except ImportError:
    print("Error: Missing required package: psutil")
    print("Please install: pip install psutil")
    sys.exit(1)


def generate_test_record(record_id: int, include_nested: bool = True) -> Dict[str, Any]:
    """Generate a test record with configurable complexity."""
    record = {
        'id': f'record_{record_id}',
        'created_at': datetime.now().isoformat(),
        'updated_at': datetime.now().isoformat(),
    }
    
    # Add 20 regular fields with ~100 chars each
    for i in range(20):
        field_name = f'field_{i}'
        record[field_name] = ''.join(random.choices(string.ascii_letters + string.digits, k=100))
    
    # Add nested JSON fields if requested
    if include_nested:
        for i in range(20):
            nested_field = f'nested_{i}'
            record[nested_field] = {
                f'sub_{j}': ''.join(random.choices(string.ascii_letters, k=50))
                for j in range(5)
            }
    
    return record


def measure_memory_usage(process: psutil.Process) -> Dict[str, float]:
    """Measure current memory usage of a process."""
    memory_info = process.memory_info()
    return {
        'rss_mb': memory_info.rss / 1024 / 1024,
        'vms_mb': memory_info.vms / 1024 / 1024,
    }


def test_batch_processing(batch_size: int, total_records: int, result_queue: Queue):
    """Test memory usage for a specific batch size."""
    process = psutil.Process()
    
    # Record initial memory
    initial_memory = measure_memory_usage(process)
    peak_memory = initial_memory['rss_mb']
    
    print(f"\n{'='*60}")
    print(f"Testing batch size: {batch_size}")
    print(f"Total records: {total_records:,}")
    print(f"Initial memory: {initial_memory['rss_mb']:.1f} MB")
    print(f"{'='*60}\n")
    
    # Simulate batch processing
    batch_buffer = []
    records_processed = 0
    batch_count = 0
    
    start_time = time.time()
    
    while records_processed < total_records:
        # Generate a batch of records
        for i in range(batch_size):
            if records_processed >= total_records:
                break
            
            record = generate_test_record(records_processed)
            batch_buffer.append(record)
            records_processed += 1
        
        # Check memory after building batch
        current_memory = measure_memory_usage(process)
        peak_memory = max(peak_memory, current_memory['rss_mb'])
        
        # Simulate batch writing (serialization + write)
        if batch_buffer:
            batch_count += 1
            
            # Simulate JSON serialization (memory intensive operation)
            json_data = json.dumps(batch_buffer)
            json_size_mb = len(json_data) / 1024 / 1024
            
            print(f"Batch {batch_count}: {len(batch_buffer)} records, "
                  f"JSON size: {json_size_mb:.1f} MB, "
                  f"Memory: {current_memory['rss_mb']:.1f} MB")
            
            # Check memory during serialization
            serialization_memory = measure_memory_usage(process)
            peak_memory = max(peak_memory, serialization_memory['rss_mb'])
            
            # Clear batch buffer
            batch_buffer.clear()
            del json_data
            
            # Force garbage collection
            gc.collect()
            
            # Progress update
            if records_processed % 2000 == 0:
                elapsed = time.time() - start_time
                rate = records_processed / elapsed if elapsed > 0 else 0
                print(f"\nProgress: {records_processed:,}/{total_records:,} records "
                      f"({rate:.0f} records/sec)")
    
    # Final memory measurement
    gc.collect()
    final_memory = measure_memory_usage(process)
    elapsed_time = time.time() - start_time
    
    # Calculate results
    results = {
        'batch_size': batch_size,
        'total_records': total_records,
        'initial_memory_mb': initial_memory['rss_mb'],
        'peak_memory_mb': peak_memory,
        'final_memory_mb': final_memory['rss_mb'],
        'memory_per_record': peak_memory / total_records,
        'elapsed_time': elapsed_time,
        'records_per_second': total_records / elapsed_time if elapsed_time > 0 else 0,
        'batch_count': batch_count,
    }
    
    print(f"\n{'='*60}")
    print(f"Batch size {batch_size} Results:")
    print(f"  Peak memory: {peak_memory:.1f} MB")
    print(f"  Memory per record: {results['memory_per_record']:.3f} MB")
    print(f"  Processing rate: {results['records_per_second']:.0f} records/sec")
    print(f"  Total batches: {batch_count}")
    print(f"{'='*60}\n")
    
    result_queue.put(results)


def main():
    """Run memory tests with different batch sizes."""
    print("Chunk Processor Memory Usage Test")
    print("=================================\n")
    
    # Test parameters
    batch_sizes = [500, 1000, 2000, 5000]
    total_records = 10000
    
    results = []
    result_queue = Queue()
    
    # Run tests in separate processes for clean memory measurements
    for batch_size in batch_sizes:
        p = Process(target=test_batch_processing, args=(batch_size, total_records, result_queue))
        p.start()
        p.join()
        
        # Get results from queue
        if not result_queue.empty():
            results.append(result_queue.get())
        
        # Brief pause between tests
        time.sleep(2)
    
    # Print summary
    print("\n" + "="*80)
    print("MEMORY USAGE SUMMARY")
    print("="*80)
    print(f"{'Batch Size':<12} {'Peak MB':<10} {'MB/Record':<12} {'Records/sec':<12}")
    print("-"*50)
    
    for result in results:
        print(f"{result['batch_size']:<12} "
              f"{result['peak_memory_mb']:<10.1f} "
              f"{result['memory_per_record']:<12.4f} "
              f"{result['records_per_second']:<12.0f}")
    
    print("\nKey Findings:")
    print("-------------")
    
    # Analyze memory scaling
    if len(results) >= 2:
        first = results[0]
        last = results[-1]
        
        batch_increase = last['batch_size'] / first['batch_size']
        memory_increase = last['peak_memory_mb'] / first['peak_memory_mb']
        
        print(f"- Batch size increased {batch_increase:.1f}x")
        print(f"- Peak memory increased {memory_increase:.1f}x")
        
        if memory_increase > batch_increase * 1.2:
            print("- ⚠️  WARNING: Memory usage scales super-linearly with batch size!")
            print("  This indicates potential memory inefficiency in batch processing")
        else:
            print("- ✅ Memory usage scales appropriately with batch size")
    
    # Check if any test exceeded Lambda limits
    lambda_limit_mb = 1024
    for result in results:
        if result['peak_memory_mb'] > lambda_limit_mb * 0.8:
            print(f"\n⚠️  Batch size {result['batch_size']} used {result['peak_memory_mb']:.1f} MB "
                  f"({(result['peak_memory_mb']/lambda_limit_mb)*100:.0f}% of Lambda limit)")


if __name__ == "__main__":
    main()