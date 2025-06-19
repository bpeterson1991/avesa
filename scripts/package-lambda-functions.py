#!/usr/bin/env python3
"""
Lambda Function Packaging Script

This script creates proper deployment packages for Lambda functions with shared modules.
It addresses the import issues by ensuring shared modules are properly included.
"""

import argparse
import os
import sys
import zipfile
import tempfile
import shutil
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description='Package Lambda functions with shared modules')
    parser.add_argument('--function', choices=['connectwise', 'canonical', 'all'], default='all',
                       help='Which functions to package (default: all)')
    parser.add_argument('--output-dir', default='./lambda-packages',
                       help='Output directory for packages (default: ./lambda-packages)')
    parser.add_argument('--clean', action='store_true', help='Clean output directory first')
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    if args.clean and output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📦 Packaging Lambda functions")
    print(f"Output directory: {output_dir}")
    print(f"Functions: {args.function}")
    print()
    
    try:
        if args.function in ['connectwise', 'all']:
            package_connectwise_function(output_dir)
        
        if args.function in ['canonical', 'all']:
            package_canonical_function(output_dir)
        
        print()
        print("✅ Lambda function packaging completed successfully!")
        print()
        print("Next steps:")
        print("1. Deploy using CDK:")
        print("   cd infrastructure && cdk deploy --context environment=dev")
        print()
        print("2. Or update functions directly:")
        print("   python scripts/deploy-lambda-functions.py --environment dev")
        
    except Exception as e:
        print(f"❌ Error packaging Lambda functions: {str(e)}", file=sys.stderr)
        sys.exit(1)


def package_connectwise_function(output_dir: Path):
    """Package ConnectWise ingestion function with shared modules."""
    print("📦 Packaging ConnectWise ingestion function...")
    
    source_dir = Path("src/integrations/connectwise")
    package_path = output_dir / "connectwise-ingestion.zip"
    
    # Create deployment package
    create_deployment_package(
        source_dir=source_dir,
        package_path=package_path,
        include_shared=True,
        function_name="connectwise-ingestion"
    )
    
    print(f"  ✅ Created package: {package_path}")


def package_canonical_function(output_dir: Path):
    """Package canonical transform function with shared modules."""
    print("📦 Packaging canonical transform function...")
    
    source_dir = Path("src/canonical_transform")
    package_path = output_dir / "canonical-transform.zip"
    
    # Create deployment package
    create_deployment_package(
        source_dir=source_dir,
        package_path=package_path,
        include_shared=True,
        function_name="canonical-transform"
    )
    
    print(f"  ✅ Created package: {package_path}")


def create_deployment_package(source_dir: Path, package_path: Path, include_shared: bool = True, function_name: str = ""):
    """Create deployment package with dependencies."""
    print(f"  📝 Creating deployment package for {function_name}...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        package_dir = Path(temp_dir) / "package"
        package_dir.mkdir()
        
        # Copy Lambda function files (only .py files, not the bundled dependencies)
        copy_lambda_source_files(source_dir, package_dir)
        
        # Copy shared modules if requested
        if include_shared:
            copy_shared_modules(package_dir)
        
        # Install clean dependencies
        install_clean_dependencies(package_dir, function_name)
        
        # Create zip file
        create_zip_package(package_dir, package_path)
        
        print(f"  ✅ Created deployment package: {package_path}")


def copy_lambda_source_files(source_dir: Path, package_dir: Path):
    """Copy only the Lambda function source files, excluding bundled dependencies."""
    print(f"  📁 Copying Lambda source files from {source_dir}...")
    
    # Only copy the main lambda_function.py file
    lambda_file = source_dir / "lambda_function.py"
    if lambda_file.exists():
        shutil.copy2(lambda_file, package_dir / "lambda_function.py")
        print(f"    ✓ Copied lambda_function.py")
    else:
        raise FileNotFoundError(f"lambda_function.py not found in {source_dir}")
    
    # Copy any other .py files that are not part of bundled dependencies
    for py_file in source_dir.glob("*.py"):
        if py_file.name != "lambda_function.py":
            # Skip if it's part of a bundled dependency directory
            if not any(dep_dir in str(py_file) for dep_dir in ['boto3', 'pandas', 'pyarrow', 'dateutil']):
                shutil.copy2(py_file, package_dir / py_file.name)
                print(f"    ✓ Copied {py_file.name}")


def copy_shared_modules(package_dir: Path):
    """Copy shared modules to package directory root."""
    print(f"  📁 Copying shared modules...")
    
    shared_dir = Path("src/shared")
    
    if shared_dir.exists():
        # Copy each shared module file directly to the package root
        for py_file in shared_dir.glob("*.py"):
            shutil.copy2(py_file, package_dir / py_file.name)
            print(f"    ✓ Copied {py_file.name} to package root")
    else:
        raise FileNotFoundError(f"Shared modules directory not found: {shared_dir}")


def install_clean_dependencies(package_dir: Path, function_name: str):
    """Install clean dependencies without conflicts."""
    print(f"  📦 Installing clean dependencies...")
    
    # Create a requirements.txt with the essential dependencies
    requirements_content = """
boto3>=1.26.0
botocore>=1.29.0
pandas>=2.0.0
pyarrow>=10.0.0
requests>=2.28.0
python-dateutil>=2.8.0
""".strip()
    
    requirements_file = package_dir / "requirements.txt"
    with open(requirements_file, 'w') as f:
        f.write(requirements_content)
    
    # Install dependencies
    try:
        subprocess.run([
            sys.executable, "-m", "pip", "install",
            "-r", str(requirements_file),
            "-t", str(package_dir),
            "--upgrade"
        ], check=True, capture_output=True, text=True)
        
        print(f"    ✓ Installed dependencies")
        
        # Remove the requirements.txt from the package
        requirements_file.unlink()
        
    except subprocess.CalledProcessError as e:
        print(f"    ❌ Failed to install dependencies: {e}")
        print(f"    stdout: {e.stdout}")
        print(f"    stderr: {e.stderr}")
        raise


def create_zip_package(package_dir: Path, package_path: Path):
    """Create zip file from package directory."""
    print(f"  🗜️  Creating zip package...")
    
    with zipfile.ZipFile(package_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for root, dirs, files in os.walk(package_dir):
            for file in files:
                file_path = Path(root) / file
                arc_name = file_path.relative_to(package_dir)
                zip_file.write(file_path, arc_name)
    
    # Get package size
    size_mb = package_path.stat().st_size / (1024 * 1024)
    print(f"    ✓ Package size: {size_mb:.1f} MB")


if __name__ == '__main__':
    main()