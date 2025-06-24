#!/usr/bin/env python3
"""
Rebuild and update ClickHouse Lambda functions with optimized dependencies.
Leverages AWS Pandas Layer to minimize package sizes and avoid deployment timeouts.
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

def create_optimized_lambda_package(source_dir, function_name, optimize_packages=False):
    """Create an optimized Lambda deployment package with minimal dependencies."""
    print(f"\n🔧 Building optimized Lambda package for {function_name}")
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        package_dir = os.path.join(temp_dir, 'package')
        os.makedirs(package_dir)
        
        # Choose requirements file based on optimization flag
        if optimize_packages:
            requirements_file = os.path.join(source_dir, 'requirements-optimized.txt')
            if not os.path.exists(requirements_file):
                print(f"⚠️  Optimized requirements not found, falling back to standard requirements")
                requirements_file = os.path.join(source_dir, 'requirements.txt')
        else:
            requirements_file = os.path.join(source_dir, 'requirements.txt')
        
        # Install dependencies
        if os.path.exists(requirements_file):
            print(f"Installing dependencies from {requirements_file}")
            
            # Use optimized pip install with platform-specific wheels for Lambda compatibility
            if optimize_packages:
                print("🚀 Using optimized installation with Lambda-compatible binaries")
                # Install zstandard with platform-specific wheel (required for clickhouse-connect)
                print("Installing zstandard with platform-specific wheel")
                zstd_cmd = f"pip install --platform manylinux2014_x86_64 --only-binary=all --no-deps -t {package_dir} zstandard>=0.21.0"
                if not run_command(zstd_cmd):
                    print(f"❌ Failed to install zstandard")
                    return None
                
                # Install lz4 with platform-specific wheel (required for clickhouse-connect)
                print("Installing lz4 with platform-specific wheel")
                lz4_cmd = f"pip install --platform manylinux2014_x86_64 --only-binary=all --no-deps -t {package_dir} lz4>=3.1.0"
                if not run_command(lz4_cmd):
                    print(f"❌ Failed to install lz4")
                    return None
                
                # Install clickhouse-connect with all required dependencies
                remaining_cmd = f"pip install clickhouse-connect==0.8.17 certifi>=2023.7.22 --no-deps -t {package_dir}"
                if not run_command(remaining_cmd):
                    return None
            else:
                # For non-optimized builds, still need zstandard and lz4 for clickhouse-connect
                cmd = f"pip install -r {requirements_file} zstandard>=0.21.0 lz4>=3.1.0 -t {package_dir}"
                if not run_command(cmd):
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
        
        # Copy environment configuration file (critical for Lambda functions)
        env_config_path = os.path.join(os.getcwd(), 'infrastructure', 'environment_config.json')
        if os.path.exists(env_config_path):
            print(f"Copying environment configuration from {env_config_path}")
            shutil.copy2(env_config_path, package_dir)
        else:
            print(f"⚠️  Environment configuration file not found at {env_config_path}")
        
        # Copy any additional files from source directory
        for item in os.listdir(source_dir):
            item_path = os.path.join(source_dir, item)
            if os.path.isfile(item_path) and item not in ['lambda_function.py', 'requirements.txt']:
                shutil.copy2(item_path, package_dir)
        
        # Remove unnecessary files to reduce package size
        if optimize_packages:
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
            if not optimize_packages:
                print("💡 Try running with --optimize-packages flag")
        
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

def main():
    """Main function to rebuild all ClickHouse Lambda functions."""
    parser = argparse.ArgumentParser(description='Rebuild ClickHouse Lambda functions')
    parser.add_argument('--environment', default='dev', help='Environment (dev/prod)')
    parser.add_argument('--optimize-packages', action='store_true', 
                       help='Use optimized packages with AWS Pandas Layer')
    parser.add_argument('--functions', nargs='+', 
                       help='Specific functions to rebuild (default: all)')
    
    args = parser.parse_args()
    
    print("🔧 Rebuilding ClickHouse Lambda Functions")
    print("=" * 60)
    print(f"Environment: {args.environment}")
    print(f"Package Optimization: {'ENABLED' if args.optimize_packages else 'DISABLED'}")
    print(f"AWS Pandas Layer: {'ENABLED' if args.optimize_packages else 'DISABLED'}")
    print("=" * 60)
    
    # Define Lambda functions to rebuild
    all_functions = [
        {
            'name': f'clickhouse-schema-init-{args.environment}',
            'source_dir': 'src/clickhouse/schema_init',
            'use_pandas_layer': False  # Schema init doesn't need pandas
        },
        {
            'name': f'clickhouse-loader-companies-{args.environment}',
            'source_dir': 'src/clickhouse/data_loader',
            'use_pandas_layer': True  # Data loaders need pandas/pyarrow
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
            'use_pandas_layer': True  # SCD processor needs pandas
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
        
        # Create package
        zip_content = create_optimized_lambda_package(
            func['source_dir'], 
            func['name'], 
            args.optimize_packages
        )
        if zip_content is None:
            print(f"❌ Failed to create package for {func['name']}")
            continue
        
        # Update function with optional AWS Pandas layer
        use_layer = args.optimize_packages and func.get('use_pandas_layer', False)
        if update_lambda_function_with_layer(func['name'], zip_content, use_layer):
            success_count += 1
    
    print(f"\n📊 REBUILD SUMMARY")
    print("=" * 60)
    print(f"✅ Successfully updated: {success_count}/{total_count} functions")
    
    if args.optimize_packages:
        print("🚀 Optimization Features Used:")
        print("   - AWS Pandas Layer for pandas/pyarrow/boto3")
        print("   - Minimal dependencies (only clickhouse-connect)")
        print("   - Cleanup of unnecessary files")
    
    if success_count == total_count:
        print("🎉 All Lambda functions updated successfully!")
        return True
    else:
        print("⚠️  Some functions failed to update")
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)