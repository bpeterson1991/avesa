#!/usr/bin/env python3
"""
Rebuild and update ClickHouse Lambda functions with proper dependencies.
"""

import os
import sys
import subprocess
import tempfile
import shutil
import zipfile
import boto3
from pathlib import Path

def run_command(cmd, cwd=None):
    """Run a shell command and return the result."""
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return False
    print(f"Success: {result.stdout}")
    return True

def create_lambda_package(source_dir, function_name):
    """Create a Lambda deployment package with dependencies."""
    print(f"\n🔧 Building Lambda package for {function_name}")
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        package_dir = os.path.join(temp_dir, 'package')
        os.makedirs(package_dir)
        
        # Install dependencies
        requirements_file = os.path.join(source_dir, 'requirements.txt')
        if os.path.exists(requirements_file):
            print(f"Installing dependencies from {requirements_file}")
            cmd = f"pip install -r {requirements_file} -t {package_dir}"
            if not run_command(cmd):
                return None
        
        # Copy Lambda function code
        lambda_file = os.path.join(source_dir, 'lambda_function.py')
        if os.path.exists(lambda_file):
            shutil.copy2(lambda_file, package_dir)
        
        # Create ZIP file
        zip_path = os.path.join(temp_dir, f'{function_name}.zip')
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(package_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, package_dir)
                    zipf.write(file_path, arcname)
        
        # Read ZIP file content
        with open(zip_path, 'rb') as f:
            return f.read()

def update_lambda_function(function_name, zip_content):
    """Update Lambda function code."""
    print(f"\n🚀 Updating Lambda function: {function_name}")
    
    lambda_client = boto3.client('lambda', region_name='us-east-2')
    
    try:
        response = lambda_client.update_function_code(
            FunctionName=function_name,
            ZipFile=zip_content
        )
        print(f"✅ Successfully updated {function_name}")
        print(f"   Code Size: {response['CodeSize']} bytes")
        print(f"   Last Modified: {response['LastModified']}")
        return True
    except Exception as e:
        print(f"❌ Failed to update {function_name}: {e}")
        return False

def main():
    """Main function to rebuild all ClickHouse Lambda functions."""
    print("🔧 Rebuilding ClickHouse Lambda Functions")
    print("=" * 60)
    
    # Define Lambda functions to rebuild
    functions = [
        {
            'name': 'clickhouse-schema-init-dev',
            'source_dir': 'src/clickhouse/schema_init'
        },
        {
            'name': 'clickhouse-loader-companies-dev',
            'source_dir': 'src/clickhouse/data_loader'
        },
        {
            'name': 'clickhouse-loader-contacts-dev',
            'source_dir': 'src/clickhouse/data_loader'
        },
        {
            'name': 'clickhouse-loader-tickets-dev',
            'source_dir': 'src/clickhouse/data_loader'
        },
        {
            'name': 'clickhouse-loader-time-entries-dev',
            'source_dir': 'src/clickhouse/data_loader'
        },
        {
            'name': 'clickhouse-scd-processor-dev',
            'source_dir': 'src/clickhouse/scd_processor'
        }
    ]
    
    success_count = 0
    total_count = len(functions)
    
    for func in functions:
        print(f"\n📦 Processing {func['name']}")
        
        # Create package
        zip_content = create_lambda_package(func['source_dir'], func['name'])
        if zip_content is None:
            print(f"❌ Failed to create package for {func['name']}")
            continue
        
        # Update function
        if update_lambda_function(func['name'], zip_content):
            success_count += 1
    
    print(f"\n📊 REBUILD SUMMARY")
    print("=" * 60)
    print(f"✅ Successfully updated: {success_count}/{total_count} functions")
    
    if success_count == total_count:
        print("🎉 All Lambda functions updated successfully!")
        return True
    else:
        print("⚠️  Some functions failed to update")
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)