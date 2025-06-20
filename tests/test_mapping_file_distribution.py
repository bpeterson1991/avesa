#!/usr/bin/env python3
"""
Test script to verify mapping file distribution across all environments.

This test verifies that mapping files are properly accessible in:
1. Development environment (local filesystem)
2. Lambda package environment (bundled files)
3. S3 fallback environment (if configured)
"""

import sys
import os
import json
import tempfile
import zipfile
import subprocess
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'shared'))

def test_local_mapping_access():
    """Test mapping file access in local development environment."""
    print("=== Testing Local Development Environment ===")
    
    try:
        from utils import (
            discover_available_services,
            discover_canonical_tables,
            load_canonical_mapping,
            load_service_configuration,
            load_endpoint_configuration
        )
        
        # Test service discovery
        services = discover_available_services()
        print(f"✓ Discovered {len(services)} services: {services}")
        
        # Test canonical table discovery
        canonical_tables = discover_canonical_tables()
        print(f"✓ Discovered {len(canonical_tables)} canonical tables: {canonical_tables}")
        
        # Test loading configurations
        success_count = 0
        total_tests = 0
        
        for service in services:
            total_tests += 1
            service_config = load_service_configuration(service)
            if service_config:
                success_count += 1
                print(f"✓ Loaded service config for {service}")
            else:
                print(f"✗ Failed to load service config for {service}")
            
            total_tests += 1
            endpoint_config = load_endpoint_configuration(service)
            if endpoint_config:
                success_count += 1
                print(f"✓ Loaded endpoint config for {service}")
            else:
                print(f"✗ Failed to load endpoint config for {service}")
        
        for table in canonical_tables:
            total_tests += 1
            mapping = load_canonical_mapping(table)
            if mapping:
                success_count += 1
                print(f"✓ Loaded canonical mapping for {table}")
            else:
                print(f"✗ Failed to load canonical mapping for {table}")
        
        print(f"Local environment test: {success_count}/{total_tests} successful")
        return success_count == total_tests
        
    except Exception as e:
        print(f"✗ Local environment test failed: {e}")
        return False


def test_lambda_package_access():
    """Test mapping file access in Lambda package environment."""
    print("\n=== Testing Lambda Package Environment ===")
    
    try:
        # Create a Lambda package using the packaging script
        package_dir = "test-lambda-package"
        result = subprocess.run([
            sys.executable, "scripts/package-lightweight-lambdas.py",
            "--function", "canonical",
            "--output-dir", package_dir
        ], capture_output=True, text=True, cwd=os.path.dirname(os.path.dirname(__file__)))
        
        if result.returncode != 0:
            print(f"✗ Failed to create Lambda package: {result.stderr}")
            return False
        
        print("✓ Created Lambda package successfully")
        
        # Extract and test the package
        package_path = os.path.join(package_dir, "canonical-transform.zip")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(package_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            # Test that mapping files are present
            mappings_dir = os.path.join(temp_dir, "mappings")
            if not os.path.exists(mappings_dir):
                print("✗ Mappings directory not found in package")
                return False
            
            # Count mapping files
            mapping_count = 0
            for root, dirs, files in os.walk(mappings_dir):
                mapping_count += len([f for f in files if f.endswith('.json')])
            
            print(f"✓ Found {mapping_count} mapping files in package")
            
            # Test loading from bundled location
            original_path = sys.path[:]
            try:
                sys.path.insert(0, temp_dir)
                
                # Import utilities from the package
                import utils
                
                # Test discovery functions
                services = utils.discover_available_services()
                canonical_tables = utils.discover_canonical_tables()
                
                print(f"✓ Package environment: {len(services)} services, {len(canonical_tables)} canonical tables")
                
                # Test loading a few mappings
                test_success = True
                if canonical_tables:
                    mapping = utils.load_canonical_mapping(canonical_tables[0])
                    if mapping:
                        print(f"✓ Successfully loaded canonical mapping from package")
                    else:
                        print(f"✗ Failed to load canonical mapping from package")
                        test_success = False
                
                if services:
                    service_config = utils.load_service_configuration(services[0])
                    if service_config:
                        print(f"✓ Successfully loaded service config from package")
                    else:
                        print(f"✗ Failed to load service config from package")
                        test_success = False
                
                return test_success
                
            finally:
                sys.path[:] = original_path
        
    except Exception as e:
        print(f"✗ Lambda package test failed: {e}")
        return False
    finally:
        # Clean up
        if os.path.exists(package_dir):
            import shutil
            shutil.rmtree(package_dir)


def test_fallback_behavior():
    """Test fallback behavior when mapping files are not found."""
    print("\n=== Testing Fallback Behavior ===")
    
    try:
        # Test with a non-existent mapping
        from utils import load_canonical_mapping
        
        # This should return empty dict for non-existent mapping
        result = load_canonical_mapping("non_existent_table")
        if result == {}:
            print("✓ Correctly returns empty dict for non-existent mapping")
            return True
        else:
            print(f"✗ Expected empty dict, got: {result}")
            return False
            
    except Exception as e:
        print(f"✗ Fallback test failed: {e}")
        return False


def test_mapping_file_consistency():
    """Test that mapping files are consistent and valid JSON."""
    print("\n=== Testing Mapping File Consistency ===")
    
    try:
        mappings_dir = Path("mappings")
        if not mappings_dir.exists():
            print("✗ Mappings directory not found")
            return False
        
        json_files = list(mappings_dir.rglob("*.json"))
        print(f"Found {len(json_files)} JSON mapping files")
        
        valid_files = 0
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    json.load(f)
                valid_files += 1
                print(f"✓ {json_file.relative_to(mappings_dir)} is valid JSON")
            except json.JSONDecodeError as e:
                print(f"✗ {json_file.relative_to(mappings_dir)} has invalid JSON: {e}")
        
        print(f"Consistency test: {valid_files}/{len(json_files)} files are valid")
        return valid_files == len(json_files)
        
    except Exception as e:
        print(f"✗ Consistency test failed: {e}")
        return False


def run_all_tests():
    """Run all mapping file distribution tests."""
    print("Testing Mapping File Distribution System")
    print("=" * 50)
    
    tests = [
        ("Local Development Environment", test_local_mapping_access),
        ("Lambda Package Environment", test_lambda_package_access),
        ("Fallback Behavior", test_fallback_behavior),
        ("Mapping File Consistency", test_mapping_file_consistency)
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
    
    if passed == total:
        print("\n🎉 All mapping file distribution tests passed!")
        print("✅ Mapping files are properly distributed and accessible in all environments")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
        print("❌ Mapping file distribution needs attention")
    
    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)