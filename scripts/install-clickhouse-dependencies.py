#!/usr/bin/env python3
"""
Install ClickHouse dependencies directly into Lambda function packages
"""

import os
import subprocess
import sys
from pathlib import Path

def run_command(cmd, cwd=None):
    """Run a command and return the result."""
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return False
    print(f"Success: {result.stdout}")
    return True

def install_dependencies_for_function(function_path):
    """Install dependencies for a specific Lambda function."""
    print(f"\n=== Installing dependencies for {function_path} ===")
    
    # Remove existing packages first
    packages_to_remove = ['clickhouse_connect', 'zstandard', 'lz4', 'certifi', 'urllib3', 'pytz']
    for pkg in packages_to_remove:
        pkg_path = function_path / pkg
        if pkg_path.exists():
            import shutil
            shutil.rmtree(pkg_path)
            print(f"Removed existing {pkg}")
    
    # Also remove .dist-info directories
    for item in function_path.iterdir():
        if item.is_dir() and item.name.endswith('.dist-info'):
            import shutil
            shutil.rmtree(item)
            print(f"Removed {item.name}")
    
    # Create a temporary directory for pip install
    temp_dir = function_path / "temp_packages"
    temp_dir.mkdir(exist_ok=True)
    
    try:
        # Try installing with Docker-like approach for Linux packages
        cmd = [
            sys.executable, "-m", "pip", "install",
            "--target", str(temp_dir),
            "--no-deps",  # Install without dependencies first
            "clickhouse-connect==0.8.17"
        ]
        
        if not run_command(cmd):
            print(f"Failed to install clickhouse-connect for {function_path}")
            return False
            
        # Install dependencies separately
        deps_cmd = [
            sys.executable, "-m", "pip", "install",
            "--target", str(temp_dir),
            "zstandard==0.23.0",
            "lz4==4.4.4",
            "certifi",
            "urllib3",
            "pytz"
        ]
        
        if not run_command(deps_cmd):
            print(f"Failed to install dependencies for {function_path}")
            return False
        
        # Move packages to function directory
        for item in temp_dir.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                target = function_path / item.name
                if target.exists():
                    # Remove existing directory
                    import shutil
                    shutil.rmtree(target)
                # Move new directory
                import shutil
                shutil.move(str(item), str(target))
                print(f"Moved {item.name} to {function_path}")
        
        return True
        
    finally:
        # Clean up temporary directory
        import shutil
        if temp_dir.exists():
            shutil.rmtree(temp_dir)

def main():
    """Main function to install dependencies for all ClickHouse Lambda functions."""
    base_path = Path(__file__).parent.parent
    clickhouse_functions = [
        "src/clickhouse/data_loader",
        "src/clickhouse/schema_init", 
        "src/clickhouse/scd_processor"
    ]
    
    print("Installing ClickHouse dependencies for Lambda functions...")
    
    success_count = 0
    for func_path in clickhouse_functions:
        full_path = base_path / func_path
        if full_path.exists():
            if install_dependencies_for_function(full_path):
                success_count += 1
            else:
                print(f"Failed to install dependencies for {func_path}")
        else:
            print(f"Function path does not exist: {full_path}")
    
    print(f"\n=== Summary ===")
    print(f"Successfully installed dependencies for {success_count}/{len(clickhouse_functions)} functions")
    
    if success_count == len(clickhouse_functions):
        print("All ClickHouse Lambda functions now have required dependencies!")
        return True
    else:
        print("Some functions failed to install dependencies.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)