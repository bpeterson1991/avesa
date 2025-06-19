#!/usr/bin/env python3
"""
Lambda Function Deployment Script

This script redeploys Lambda functions with fixed dependencies and proper packaging.
It addresses the pydantic_core import issues and ensures shared modules are properly included.
"""

import argparse
import boto3
import os
import sys
import zipfile
import tempfile
import shutil
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description='Deploy Lambda functions with fixed dependencies')
    parser.add_argument('--environment', default='dev', choices=['dev', 'staging', 'prod'],
                       help='Environment to deploy to (default: dev)')
    parser.add_argument('--region', default='us-east-2', help='AWS region (default: us-east-2)')
    parser.add_argument('--function', choices=['connectwise', 'canonical', 'all'], default='all',
                       help='Which functions to deploy (default: all)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be deployed without deploying')
    
    args = parser.parse_args()
    
    # Initialize AWS client
    lambda_client = boto3.client('lambda', region_name=args.region)
    
    print(f"🚀 Deploying Lambda functions for environment: {args.environment}")
    print(f"Region: {args.region}")
    print(f"Functions: {args.function}")
    print(f"Dry Run: {args.dry_run}")
    print()
    
    try:
        if args.function in ['connectwise', 'all']:
            deploy_connectwise_function(lambda_client, args.environment, args.region, args.dry_run)
        
        if args.function in ['canonical', 'all']:
            deploy_canonical_functions(lambda_client, args.environment, args.region, args.dry_run)
        
        print()
        print("✅ Lambda function deployment completed successfully!")
        print()
        print("Next steps:")
        print("1. Test the functions:")
        print(f"   python scripts/test-lambda-functions.py --environment {args.environment} --region {args.region}")
        print()
        print("2. Check the logs:")
        print(f"   aws logs tail /aws/lambda/avesa-connectwise-ingestion-{args.environment} --follow --region {args.region}")
        
    except Exception as e:
        print(f"❌ Error deploying Lambda functions: {str(e)}", file=sys.stderr)
        sys.exit(1)


def deploy_connectwise_function(lambda_client, environment: str, region: str, dry_run: bool):
    """Deploy ConnectWise ingestion function."""
    print("📦 Deploying ConnectWise ingestion function...")
    
    function_name = f"avesa-connectwise-ingestion-{environment}"
    source_dir = "src/integrations/connectwise"
    
    if dry_run:
        print(f"  ✓ Would deploy function: {function_name}")
        return
    
    # Create deployment package
    zip_path = create_deployment_package(source_dir, "connectwise")
    
    try:
        # Update function code
        with open(zip_path, 'rb') as zip_file:
            lambda_client.update_function_code(
                FunctionName=function_name,
                ZipFile=zip_file.read()
            )
        
        print(f"  ✅ Updated function: {function_name}")
        
        # Update environment variables
        lambda_client.update_function_configuration(
            FunctionName=function_name,
            Environment={
                'Variables': {
                    'BUCKET_NAME': f'data-storage-msp-{environment}' if environment != 'prod' else 'data-storage-msp',
                    'TENANT_SERVICES_TABLE': f'TenantServices-{environment}' if environment != 'prod' else 'TenantServices',
                    'LAST_UPDATED_TABLE': f'LastUpdated-{environment}' if environment != 'prod' else 'LastUpdated',
                    'ENVIRONMENT': environment,
                    'SERVICE_NAME': 'connectwise'
                }
            }
        )
        
        print(f"  ✅ Updated environment variables for: {function_name}")
        
    except Exception as e:
        print(f"  ❌ Failed to deploy {function_name}: {e}")
        raise
    finally:
        # Clean up
        if os.path.exists(zip_path):
            os.remove(zip_path)


def deploy_canonical_functions(lambda_client, environment: str, region: str, dry_run: bool):
    """Deploy canonical transform functions."""
    print("📦 Deploying canonical transform functions...")
    
    canonical_tables = ['tickets', 'time-entries', 'companies', 'contacts']
    
    for table in canonical_tables:
        function_name = f"avesa-canonical-transform-{table}-{environment}"
        
        if dry_run:
            print(f"  ✓ Would deploy function: {function_name}")
            continue
        
        # Create deployment package
        zip_path = create_deployment_package("src/canonical_transform", f"canonical-{table}")
        
        try:
            # Update function code
            with open(zip_path, 'rb') as zip_file:
                lambda_client.update_function_code(
                    FunctionName=function_name,
                    ZipFile=zip_file.read()
                )
            
            print(f"  ✅ Updated function: {function_name}")
            
            # Update environment variables
            lambda_client.update_function_configuration(
                FunctionName=function_name,
                Environment={
                    'Variables': {
                        'BUCKET_NAME': f'data-storage-msp-{environment}' if environment != 'prod' else 'data-storage-msp',
                        'TENANT_SERVICES_TABLE': f'TenantServices-{environment}' if environment != 'prod' else 'TenantServices',
                        'ENVIRONMENT': environment,
                        'CANONICAL_TABLE': table.replace('-', '_')
                    }
                }
            )
            
            print(f"  ✅ Updated environment variables for: {function_name}")
            
        except Exception as e:
            print(f"  ❌ Failed to deploy {function_name}: {e}")
            raise
        finally:
            # Clean up
            if os.path.exists(zip_path):
                os.remove(zip_path)


def create_deployment_package(source_dir: str, package_name: str) -> str:
    """Create deployment package with dependencies and shared modules."""
    print(f"  📝 Creating deployment package for {package_name}...")
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        package_dir = os.path.join(temp_dir, "package")
        os.makedirs(package_dir)
        
        # Copy only the main Lambda function file (not bundled dependencies)
        source_path = Path(source_dir)
        lambda_file = source_path / "lambda_function.py"
        if lambda_file.exists():
            shutil.copy2(lambda_file, os.path.join(package_dir, "lambda_function.py"))
            print(f"    ✓ Copied lambda_function.py")
        else:
            raise FileNotFoundError(f"lambda_function.py not found in {source_dir}")
        
        # Copy any other .py files that are not part of bundled dependencies
        for py_file in source_path.glob("*.py"):
            if py_file.name != "lambda_function.py":
                # Skip if it's part of a bundled dependency directory
                if not any(dep_dir in str(py_file) for dep_dir in ['boto3', 'pandas', 'pyarrow', 'dateutil']):
                    shutil.copy2(py_file, os.path.join(package_dir, py_file.name))
                    print(f"    ✓ Copied {py_file.name}")
        
        # Copy shared modules
        shared_dir = Path("src/shared")
        shared_dest = Path(package_dir) / "shared"
        if shared_dir.exists():
            shutil.copytree(shared_dir, shared_dest)
            print(f"    ✓ Copied shared modules")
        else:
            raise FileNotFoundError(f"Shared modules directory not found: {shared_dir}")
        
        # Install clean dependencies
        install_clean_dependencies(package_dir)
        
        # Create zip file
        zip_path = f"/tmp/{package_name}-deployment.zip"
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for root, dirs, files in os.walk(package_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arc_name = os.path.relpath(file_path, package_dir)
                    zip_file.write(file_path, arc_name)
        
        # Get package size
        size_mb = os.path.getsize(zip_path) / (1024 * 1024)
        print(f"  ✅ Created deployment package: {zip_path} ({size_mb:.1f} MB)")
        return zip_path


def install_clean_dependencies(package_dir: str):
    """Install clean dependencies without conflicts."""
    print(f"    📦 Installing clean dependencies...")
    
    # Create a requirements.txt with the essential dependencies
    requirements_content = """
boto3>=1.26.0
botocore>=1.29.0
pandas>=2.0.0
pyarrow>=10.0.0
pydantic>=2.5.0
pydantic-core>=2.14.0
requests>=2.28.0
python-dateutil>=2.8.0
""".strip()
    
    requirements_file = os.path.join(package_dir, "requirements.txt")
    with open(requirements_file, 'w') as f:
        f.write(requirements_content)
    
    # Install dependencies
    try:
        subprocess.run([
            sys.executable, "-m", "pip", "install",
            "-r", requirements_file,
            "-t", package_dir,
            "--no-deps",  # Don't install dependencies of dependencies
            "--upgrade"
        ], check=True, capture_output=True, text=True)
        
        print(f"      ✓ Installed dependencies")
        
        # Remove the requirements.txt from the package
        os.remove(requirements_file)
        
    except subprocess.CalledProcessError as e:
        print(f"      ❌ Failed to install dependencies: {e}")
        raise


if __name__ == '__main__':
    main()