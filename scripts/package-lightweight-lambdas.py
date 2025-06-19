#!/usr/bin/env python3
"""
Lightweight Lambda Function Packaging Script

This script creates minimal Lambda packages that rely on the AWS Pandas layer
for heavy dependencies like pandas, numpy, pyarrow, etc.
"""

import argparse
import os
import sys
import zipfile
import tempfile
import shutil
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description='Package lightweight Lambda functions')
    parser.add_argument('--function', choices=['connectwise', 'canonical', 'all'], default='all',
                       help='Which functions to package (default: all)')
    parser.add_argument('--output-dir', default='lambda-packages',
                       help='Output directory for packages (default: lambda-packages)')
    
    args = parser.parse_args()
    
    print("📦 Packaging lightweight Lambda functions")
    print(f"Output directory: {args.output_dir}")
    print(f"Functions: {args.function}")
    print()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    try:
        if args.function in ['connectwise', 'all']:
            package_connectwise_function(args.output_dir)
        
        if args.function in ['canonical', 'all']:
            package_canonical_function(args.output_dir)
        
        print()
        print("✅ Lambda function packaging completed successfully!")
        print()
        print("Next steps:")
        print("1. Deploy using CDK:")
        print("   cd infrastructure && cdk deploy --context environment=dev")
        
    except Exception as e:
        print(f"❌ Error packaging Lambda functions: {str(e)}", file=sys.stderr)
        sys.exit(1)


def package_connectwise_function(output_dir: str):
    """Package ConnectWise ingestion function."""
    print("📦 Packaging ConnectWise ingestion function...")
    
    source_dir = "src/integrations/connectwise"
    package_name = "connectwise-ingestion"
    
    zip_path = create_lightweight_package(source_dir, package_name, output_dir)
    print(f"  ✅ Created package: {zip_path}")


def package_canonical_function(output_dir: str):
    """Package canonical transform function."""
    print("📦 Packaging canonical transform function...")
    
    source_dir = "src/canonical_transform"
    package_name = "canonical-transform"
    
    zip_path = create_lightweight_package(source_dir, package_name, output_dir)
    print(f"  ✅ Created package: {zip_path}")


def create_lightweight_package(source_dir: str, package_name: str, output_dir: str) -> str:
    """Create lightweight deployment package without heavy dependencies."""
    print(f"  📝 Creating deployment package for {package_name}...")
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        package_dir = os.path.join(temp_dir, "package")
        os.makedirs(package_dir)
        
        # Copy Lambda source files (excluding bundled dependencies)
        source_path = Path(source_dir)
        print(f"  📁 Copying Lambda source files from {source_dir}...")
        
        # Skip any AWS layer package directories
        aws_layer_dirs = ['boto3', 'botocore', 'pandas', 'numpy', 'pyarrow', 'dateutil', 'pytz',
                         's3transfer', 'urllib3', 'jmespath', 'requests', 'certifi', 'charset_normalizer',
                         'idna', 'tzdata', 'six', 'typing_extensions', 'annotated_types', 'typing_inspection']
        
        # Remove any AWS layer directories that might exist
        for aws_dir in aws_layer_dirs:
            for pattern in [f"{aws_dir}*", f"{aws_dir}-*"]:
                for path in source_path.glob(pattern):
                    if path.is_dir():
                        print(f"    ✗ Skipping AWS layer directory: {path.name}")
        
        # Copy main lambda function
        lambda_file = source_path / "lambda_function.py"
        if lambda_file.exists():
            shutil.copy2(lambda_file, os.path.join(package_dir, "lambda_function.py"))
            print(f"    ✓ Copied lambda_function.py")
        else:
            raise FileNotFoundError(f"lambda_function.py not found in {source_dir}")
        
        # Copy any other Python files that are NOT part of bundled dependencies
        for py_file in source_path.glob("*.py"):
            if py_file.name != "lambda_function.py":
                # Skip AWS layer packages and other bundled dependencies
                aws_layer_packages = ['boto3', 'botocore', 'pandas', 'numpy', 'pyarrow',
                                    'dateutil', 'pytz', 's3transfer', 'urllib3', 'jmespath',
                                    'requests', 'pydantic', 'six', 'typing_extensions', 'certifi',
                                    'charset_normalizer', 'idna', 'tzdata']
                if not any(dep_dir in str(py_file) for dep_dir in aws_layer_packages):
                    shutil.copy2(py_file, os.path.join(package_dir, py_file.name))
                    print(f"    ✓ Copied {py_file.name}")
                else:
                    print(f"    ✗ Skipped {py_file.name} (AWS layer package)")
        
        # Copy shared modules to package root (not in subdirectory)
        print(f"  📁 Copying shared modules...")
        shared_dir = Path("src/shared")
        if shared_dir.exists():
            for shared_file in shared_dir.glob("*.py"):
                # Skip the old config.py that uses pydantic, use config_simple.py instead
                if shared_file.name == "config.py":
                    print(f"    ✓ Skipped {shared_file.name} (using config_simple.py instead)")
                    continue
                shutil.copy2(shared_file, os.path.join(package_dir, shared_file.name))
                print(f"    ✓ Copied {shared_file.name} to package root")
        else:
            raise FileNotFoundError(f"Shared modules directory not found: {shared_dir}")
        
        # Install only lightweight dependencies
        install_lightweight_dependencies(package_dir)
        
        # Create zip file
        zip_path = os.path.join(output_dir, f"{package_name}.zip")
        print(f"  🗜️  Creating zip package...")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for root, dirs, files in os.walk(package_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arc_name = os.path.relpath(file_path, package_dir)
                    zip_file.write(file_path, arc_name)
        
        # Get package size
        size_mb = os.path.getsize(zip_path) / (1024 * 1024)
        print(f"    ✓ Package size: {size_mb:.1f} MB")
        
        return zip_path


def install_lightweight_dependencies(package_dir: str):
    """Install only lightweight dependencies (AWS Pandas layer provides the heavy ones)."""
    print(f"  📦 Installing lightweight dependencies...")
    
    # Only install dependencies NOT provided by AWS Pandas layer
    # AWS Pandas layer provides: pandas, numpy, pyarrow, boto3, botocore,
    # requests, python-dateutil, pytz, s3transfer, urllib3, jmespath, six,
    # typing_extensions, certifi, charset_normalizer, idna, tzdata
    # We only need application-specific dependencies
    lightweight_requirements = """
# Application-specific dependencies only
# All heavy dependencies provided by AWS Pandas layer:
# - pandas, numpy, pyarrow
# - boto3, botocore, s3transfer
# - requests, urllib3, certifi, charset_normalizer, idna
# - python-dateutil, pytz, tzdata
# - jmespath, six, typing_extensions
""".strip()
    
    requirements_file = os.path.join(package_dir, "requirements.txt")
    with open(requirements_file, 'w') as f:
        f.write(lightweight_requirements)
    
    # Install dependencies
    try:
        import subprocess
        subprocess.run([
            sys.executable, "-m", "pip", "install",
            "-r", requirements_file,
            "-t", package_dir,
            "--upgrade"
        ], check=True, capture_output=True, text=True)
        
        print(f"    ✓ Installed dependencies")
        
        # Remove the requirements.txt from the package
        os.remove(requirements_file)
        
    except subprocess.CalledProcessError as e:
        print(f"    ❌ Failed to install dependencies: {e}")
        raise


if __name__ == '__main__':
    main()