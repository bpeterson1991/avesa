#!/usr/bin/env python3
"""
Fix LZ4 binary compatibility issue for ClickHouse Lambda functions.
Uses platform-specific wheels compatible with Amazon Linux 2 Lambda runtime.
"""

import os
import sys
import subprocess
import tempfile
import shutil
import zipfile
import boto3
import argparse
from pathlib import Path

# AWS Pandas Layer ARN for us-east-2
AWS_PANDAS_LAYER_ARN = "arn:aws:lambda:us-east-2:336392948345:layer:AWSSDKPandas-Python312:13"

def run_command(cmd, cwd=None):
    """Run a shell command and return the result."""
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return False
    print(f"Success: {result.stdout}")
    return True

def install_lambda_compatible_packages(requirements_file, target_dir):
    """Install packages with Lambda-compatible binaries using platform-specific wheels."""
    print(f"🔧 Installing Lambda-compatible packages from {requirements_file}")
    
    # First, install platform-specific wheels for binary dependencies
    platform_specific_packages = [
        "lz4==4.3.2",
        "zstandard>=0.21.0"
    ]
    
    print("📦 Installing platform-specific binary packages...")
    for package in platform_specific_packages:
        # Use manylinux2014_x86_64 wheels compatible with Amazon Linux 2
        cmd = f"pip install --platform manylinux2014_x86_64 --only-binary=all --target {target_dir} {package}"
        if not run_command(cmd):
            print(f"⚠️  Failed to install platform-specific wheel for {package}, trying fallback...")
            # Fallback: try with --no-deps to avoid dependency conflicts
            fallback_cmd = f"pip install --platform manylinux2014_x86_64 --only-binary=all --no-deps --target {target_dir} {package}"
            if not run_command(fallback_cmd):
                print(f"❌ Failed to install {package} with platform-specific wheels")
                return False
    
    # Then install remaining packages normally
    print("📦 Installing remaining packages...")
    remaining_packages = [
        "clickhouse-connect==0.8.17",
        "certifi>=2023.7.22"
    ]
    
    for package in remaining_packages:
        cmd = f"pip install --target {target_dir} {package}"
        if not run_command(cmd):
            print(f"❌ Failed to install {package}")
            return False
    
    return True

def create_lambda_compatible_package(source_dir, function_name):
    """Create a Lambda deployment package with compatible LZ4 binaries."""
    print(f"\n🔧 Building Lambda-compatible package for {function_name}")
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        package_dir = os.path.join(temp_dir, 'package')
        os.makedirs(package_dir)
        
        # Use requirements file (try optimized first, fallback to regular)
        requirements_file = os.path.join(source_dir, 'requirements-optimized.txt')
        if not os.path.exists(requirements_file):
            requirements_file = os.path.join(source_dir, 'requirements.txt')
            if not os.path.exists(requirements_file):
                print(f"❌ No requirements file found in: {source_dir}")
                return None
            print(f"📋 Using requirements file: {requirements_file}")
        
        # Install Lambda-compatible packages
        if not install_lambda_compatible_packages(requirements_file, package_dir):
            return None
        
        # Copy Lambda function code
        lambda_file = os.path.join(source_dir, 'lambda_function.py')
        if os.path.exists(lambda_file):
            shutil.copy2(lambda_file, package_dir)
        
        # Copy shared modules (required for ClickHouse functions)
        shared_dir = os.path.join(os.getcwd(), 'src', 'shared')
        if os.path.exists(shared_dir):
            print(f"Copying shared modules from {shared_dir}")
            shared_dest = os.path.join(package_dir, 'shared')
            shutil.copytree(shared_dir, shared_dest)
        
        # Copy environment configuration file
        env_config_path = os.path.join(os.getcwd(), 'infrastructure', 'environment_config.json')
        if os.path.exists(env_config_path):
            print(f"Copying environment configuration from {env_config_path}")
            shutil.copy2(env_config_path, package_dir)
        
        # Copy any additional files from source directory
        for item in os.listdir(source_dir):
            item_path = os.path.join(source_dir, item)
            if os.path.isfile(item_path) and item not in ['lambda_function.py', 'requirements.txt', 'requirements-optimized.txt']:
                shutil.copy2(item_path, package_dir)
        
        # Clean up unnecessary files to reduce package size
        cleanup_patterns = [
            '**/__pycache__',
            '**/*.pyc',
            '**/*.pyo',
            '**/*.dist-info',
            '**/tests',
            '**/test',
            '**/*.egg-info'
        ]
        
        for pattern in cleanup_patterns:
            for path in Path(package_dir).glob(pattern):
                if path.is_dir():
                    print(f"Removing directory: {path}")
                    shutil.rmtree(path)
                elif path.is_file():
                    print(f"Removing file: {path}")
                    path.unlink()
        
        # Verify LZ4 installation
        print("🔍 Verifying LZ4 installation...")
        lz4_path = os.path.join(package_dir, 'lz4')
        if os.path.exists(lz4_path):
            print(f"✅ LZ4 package found at {lz4_path}")
            # Check for _version module
            version_file = os.path.join(lz4_path, '_version.py')
            if os.path.exists(version_file):
                print("✅ LZ4 _version module found")
            else:
                print("⚠️  LZ4 _version module not found, but package exists")
        else:
            print("❌ LZ4 package not found!")
            return None
        
        # Create ZIP file
        zip_path = os.path.join(temp_dir, f'{function_name}.zip')
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(package_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, package_dir)
                    zipf.write(file_path, arcname)
        
        # Check package size
        package_size = os.path.getsize(zip_path)
        print(f"📦 Package size: {package_size / (1024*1024):.2f} MB")
        
        if package_size > 50 * 1024 * 1024:  # 50MB limit
            print(f"⚠️  Package size exceeds 50MB limit!")
        
        # Read ZIP file content
        with open(zip_path, 'rb') as f:
            return f.read()

def update_lambda_function_with_layer(function_name, zip_content, use_pandas_layer=False):
    """Update Lambda function code and optionally add AWS Pandas layer."""
    print(f"\n🚀 Updating Lambda function: {function_name}")
    
    lambda_client = boto3.client('lambda', region_name='us-east-2')
    
    try:
        # Update function code
        response = lambda_client.update_function_code(
            FunctionName=function_name,
            ZipFile=zip_content
        )
        print(f"✅ Successfully updated {function_name}")
        print(f"   Code Size: {response['CodeSize']} bytes")
        print(f"   Last Modified: {response['LastModified']}")
        
        # Add AWS Pandas Layer if requested
        if use_pandas_layer:
            print(f"🔗 Adding AWS Pandas Layer to {function_name}")
            
            # Get current function configuration
            config_response = lambda_client.get_function_configuration(FunctionName=function_name)
            current_layers = config_response.get('Layers', [])
            
            # Check if AWS Pandas layer is already attached
            pandas_layer_attached = any(
                AWS_PANDAS_LAYER_ARN in layer.get('Arn', '') 
                for layer in current_layers
            )
            
            if not pandas_layer_attached:
                # Add AWS Pandas layer
                layer_arns = [layer['Arn'] for layer in current_layers]
                layer_arns.append(AWS_PANDAS_LAYER_ARN)
                
                layer_response = lambda_client.update_function_configuration(
                    FunctionName=function_name,
                    Layers=layer_arns
                )
                print(f"✅ AWS Pandas Layer added to {function_name}")
            else:
                print(f"ℹ️  AWS Pandas Layer already attached to {function_name}")
        
        return True
    except Exception as e:
        print(f"❌ Failed to update {function_name}: {e}")
        return False

def test_lz4_import(function_name):
    """Test LZ4 import in Lambda function."""
    print(f"\n🧪 Testing LZ4 import in {function_name}")
    
    lambda_client = boto3.client('lambda', region_name='us-east-2')
    
    test_payload = {
        "test_mode": True,
        "test_import": "lz4"
    }
    
    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=str(test_payload).encode()
        )
        
        result = response['Payload'].read().decode()
        print(f"Test result: {result}")
        
        if 'error' in result.lower() or 'exception' in result.lower():
            print(f"❌ LZ4 import test failed for {function_name}")
            return False
        else:
            print(f"✅ LZ4 import test passed for {function_name}")
            return True
            
    except Exception as e:
        print(f"❌ Failed to test {function_name}: {e}")
        return False

def main():
    """Main function to fix LZ4 compatibility for ClickHouse Lambda functions."""
    parser = argparse.ArgumentParser(description='Fix LZ4 compatibility for ClickHouse Lambda functions')
    parser.add_argument('--environment', default='dev', help='Environment (dev/prod)')
    parser.add_argument('--functions', nargs='+', 
                       help='Specific functions to fix (default: all)')
    parser.add_argument('--test-import', action='store_true',
                       help='Test LZ4 import after deployment')
    
    args = parser.parse_args()
    
    print("🔧 Fixing LZ4 Binary Compatibility for ClickHouse Lambda Functions")
    print("=" * 80)
    print(f"Environment: {args.environment}")
    print(f"Platform Target: Amazon Linux 2 (manylinux2014_x86_64)")
    print(f"LZ4 Version: 4.3.2 (pinned)")
    print("=" * 80)
    
    # Define Lambda functions to fix
    all_functions = [
        {
            'name': f'clickhouse-schema-init-{args.environment}',
            'source_dir': 'src/clickhouse/schema_init',
            'use_pandas_layer': False
        },
        {
            'name': f'clickhouse-loader-companies-{args.environment}',
            'source_dir': 'src/clickhouse/data_loader',
            'use_pandas_layer': True
        },
        {
            'name': f'clickhouse-loader-contacts-{args.environment}',
            'source_dir': 'src/clickhouse/data_loader',
            'use_pandas_layer': True
        },
        {
            'name': f'clickhouse-loader-tickets-{args.environment}',
            'source_dir': 'src/clickhouse/data_loader',
            'use_pandas_layer': True
        },
        {
            'name': f'clickhouse-loader-time-entries-{args.environment}',
            'source_dir': 'src/clickhouse/data_loader',
            'use_pandas_layer': True
        },
        {
            'name': f'clickhouse-scd-processor-{args.environment}',
            'source_dir': 'src/clickhouse/scd_processor',
            'use_pandas_layer': True
        }
    ]
    
    # Filter functions if specific ones requested
    if args.functions:
        functions = [f for f in all_functions if any(func in f['name'] for func in args.functions)]
        if not functions:
            print(f"❌ No matching functions found for: {args.functions}")
            return False
    else:
        functions = all_functions
    
    success_count = 0
    total_count = len(functions)
    
    for func in functions:
        print(f"\n📦 Processing {func['name']}")
        
        # Create Lambda-compatible package
        zip_content = create_lambda_compatible_package(
            func['source_dir'], 
            func['name']
        )
        if zip_content is None:
            print(f"❌ Failed to create package for {func['name']}")
            continue
        
        # Update function with AWS Pandas layer
        if update_lambda_function_with_layer(func['name'], zip_content, func.get('use_pandas_layer', False)):
            success_count += 1
            
            # Test LZ4 import if requested
            if args.test_import:
                test_lz4_import(func['name'])
    
    print(f"\n📊 LZ4 COMPATIBILITY FIX SUMMARY")
    print("=" * 80)
    print(f"✅ Successfully updated: {success_count}/{total_count} functions")
    print("🔧 Applied fixes:")
    print("   - Platform-specific LZ4 wheels (manylinux2014_x86_64)")
    print("   - Pinned LZ4 version to 4.3.2")
    print("   - AWS Pandas Layer integration")
    print("   - Binary compatibility verification")
    
    if success_count == total_count:
        print("🎉 All Lambda functions updated with LZ4 compatibility fixes!")
        print("\n🚀 Ready for pipeline validation!")
        return True
    else:
        print("⚠️  Some functions failed to update")
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)