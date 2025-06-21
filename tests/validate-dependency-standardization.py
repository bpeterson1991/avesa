#!/usr/bin/env python3
"""
Validation script for dependency version standardization.
Verifies that all requirements files use the standardized versions.
"""

import os
import re
from pathlib import Path

def parse_requirements_file(file_path):
    """Parse a requirements.txt file and extract package versions."""
    packages = {}
    if not os.path.exists(file_path):
        return packages
    
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                # Handle different version specifiers
                if '==' in line:
                    package, version = line.split('==', 1)
                    packages[package] = ('==', version)
                elif '>=' in line:
                    package, version = line.split('>=', 1)
                    packages[package] = ('>=', version)
                elif '>' in line:
                    package, version = line.split('>', 1)
                    packages[package] = ('>', version)
    
    return packages

def validate_standardization():
    """Validate that all requirements files follow standardization rules."""
    print("🔍 VALIDATING DEPENDENCY VERSION STANDARDIZATION")
    print("=" * 60)
    
    # Expected standardized versions
    expected_versions = {
        'boto3': '1.38.39',
        'botocore': '1.38.39',
        'clickhouse-connect': '0.8.17',
        'pandas': '2.2.3',
        'pyarrow': '18.1.0',
        'aws-cdk-lib': '2.100.0'
    }
    
    # Files to check with their expected version types
    files_to_check = {
        'src/clickhouse/schema_init/requirements.txt': {
            'boto3': '==',
            'botocore': '==',
            'clickhouse-connect': '=='
        },
        'src/clickhouse/data_loader/requirements.txt': {
            'boto3': '==',
            'botocore': '==',
            'clickhouse-connect': '==',
            'pandas': '==',
            'pyarrow': '=='
        },
        'src/clickhouse/scd_processor/requirements.txt': {
            'boto3': '==',
            'botocore': '==',
            'clickhouse-connect': '==',
            'pandas': '==',
            'pyarrow': '=='
        },
        'src/backfill/requirements.txt': {
            'boto3': '==',
            'botocore': '=='
        },
        'src/optimized/orchestrator/requirements.txt': {
            'boto3': '==',
            'botocore': '=='
        },
        'infrastructure/requirements.txt': {
            'boto3': '>=',
            'botocore': '>=',
            'aws-cdk-lib': '=='
        },
        'requirements.txt': {
            'boto3': '>=',
            'pandas': '>=',
            'pyarrow': '>=',
            'aws-cdk-lib': '>='
        }
    }
    
    all_valid = True
    
    for file_path, expected_packages in files_to_check.items():
        print(f"\n📄 Checking: {file_path}")
        
        if not os.path.exists(file_path):
            print(f"   ❌ File not found: {file_path}")
            all_valid = False
            continue
        
        packages = parse_requirements_file(file_path)
        
        for package, expected_operator in expected_packages.items():
            if package not in packages:
                print(f"   ❌ Missing package: {package}")
                all_valid = False
                continue
            
            operator, version = packages[package]
            expected_version = expected_versions[package]
            
            # Check operator
            if operator != expected_operator:
                print(f"   ❌ {package}: Expected operator '{expected_operator}', got '{operator}'")
                all_valid = False
                continue
            
            # Check version
            if operator == '==' and version != expected_version:
                print(f"   ❌ {package}: Expected version '{expected_version}', got '{version}'")
                all_valid = False
                continue
            elif operator == '>=' and not version_meets_minimum(version, expected_version):
                print(f"   ❌ {package}: Version '{version}' does not meet minimum '{expected_version}'")
                all_valid = False
                continue
            else:
                print(f"   ✅ {package}: {operator}{version}")
    
    print(f"\n{'=' * 60}")
    if all_valid:
        print("✅ ALL DEPENDENCY VERSIONS ARE STANDARDIZED CORRECTLY!")
        print("\n📋 Summary of standardized versions:")
        for package, version in expected_versions.items():
            print(f"   • {package}: {version}")
        
        print(f"\n📁 Files updated:")
        for file_path in files_to_check.keys():
            if os.path.exists(file_path):
                print(f"   • {file_path}")
        
        print(f"\n📚 Documentation created:")
        print(f"   • docs/DEPENDENCY_VERSION_STANDARDIZATION.md")
        
    else:
        print("❌ STANDARDIZATION VALIDATION FAILED!")
        print("   Please review the errors above and fix the requirements files.")
    
    return all_valid

def version_meets_minimum(current, minimum):
    """Check if current version meets minimum requirement."""
    # Simple version comparison for major.minor.patch format
    try:
        current_parts = [int(x) for x in current.split('.')]
        minimum_parts = [int(x) for x in minimum.split('.')]
        
        # Pad shorter version with zeros
        max_len = max(len(current_parts), len(minimum_parts))
        current_parts.extend([0] * (max_len - len(current_parts)))
        minimum_parts.extend([0] * (max_len - len(minimum_parts)))
        
        return current_parts >= minimum_parts
    except:
        return False

if __name__ == "__main__":
    validate_standardization()