#!/usr/bin/env python3
"""
Prepare optimized lambda functions by copying shared dependencies.

This script copies the shared modules into each optimized lambda directory
so they can be packaged without Docker bundling while still having access
to shared functionality.
"""

import os
import shutil
import sys
from pathlib import Path

def copy_shared_modules():
    """Copy shared modules to optimized lambda directories."""
    
    # Get the project root directory
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    
    # Define source and target directories
    shared_src = project_root / "src" / "shared"
    optimized_dirs = [
        project_root / "src" / "optimized" / "orchestrator",
        project_root / "src" / "optimized" / "processors", 
        project_root / "src" / "optimized" / "helpers"
    ]
    
    if not shared_src.exists():
        print(f"ERROR: Shared source directory not found: {shared_src}")
        return False
    
    print(f"Copying shared modules from: {shared_src}")
    
    # Copy shared modules to each optimized lambda directory
    for target_dir in optimized_dirs:
        if not target_dir.exists():
            print(f"WARNING: Target directory not found: {target_dir}")
            continue
            
        shared_target = target_dir / "shared"
        
        # Remove existing shared directory if it exists
        if shared_target.exists():
            print(f"Removing existing shared directory: {shared_target}")
            shutil.rmtree(shared_target)
        
        # Copy shared modules
        print(f"Copying shared modules to: {shared_target}")
        shutil.copytree(shared_src, shared_target)
        
        # Create __init__.py if it doesn't exist
        init_file = shared_target / "__init__.py"
        if not init_file.exists():
            init_file.touch()
    
    print("✅ Successfully copied shared modules to all optimized lambda directories")
    return True

def install_dependencies():
    """Install dependencies for optimized lambda functions."""
    
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    
    # Define optimized lambda directories with requirements
    lambda_dirs = [
        project_root / "src" / "optimized" / "orchestrator",
        project_root / "src" / "optimized" / "processors", 
        project_root / "src" / "optimized" / "helpers"
    ]
    
    for lambda_dir in lambda_dirs:
        requirements_file = lambda_dir / "requirements.txt"
        
        if not requirements_file.exists():
            print(f"WARNING: No requirements.txt found in {lambda_dir}")
            continue
            
        print(f"Installing dependencies for: {lambda_dir}")
        
        # Install dependencies to the lambda directory
        cmd = f"pip install -r {requirements_file} -t {lambda_dir} --no-deps"
        result = os.system(cmd)
        
        if result != 0:
            print(f"ERROR: Failed to install dependencies for {lambda_dir}")
            return False
        else:
            print(f"✅ Successfully installed dependencies for {lambda_dir}")
    
    return True

def clean_optimized_lambdas():
    """Clean up optimized lambda directories by removing shared modules and dependencies."""
    
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    
    optimized_dirs = [
        project_root / "src" / "optimized" / "orchestrator",
        project_root / "src" / "optimized" / "processors", 
        project_root / "src" / "optimized" / "helpers"
    ]
    
    for target_dir in optimized_dirs:
        if not target_dir.exists():
            continue
            
        # Remove shared directory
        shared_target = target_dir / "shared"
        if shared_target.exists():
            print(f"Removing shared directory: {shared_target}")
            shutil.rmtree(shared_target)
        
        # Remove installed packages (common Python package directories)
        cleanup_patterns = [
            "boto3*", "botocore*", "requests*", "pandas*", "pyarrow*",
            "*.dist-info", "__pycache__", "*.pyc"
        ]
        
        for pattern in cleanup_patterns:
            for item in target_dir.glob(pattern):
                if item.is_dir():
                    print(f"Removing directory: {item}")
                    shutil.rmtree(item)
                elif item.is_file():
                    print(f"Removing file: {item}")
                    item.unlink()
    
    print("✅ Successfully cleaned optimized lambda directories")

def main():
    """Main function."""
    if len(sys.argv) > 1 and sys.argv[1] == "clean":
        clean_optimized_lambdas()
        return
    
    print("Preparing optimized lambda functions...")
    
    # Copy shared modules
    if not copy_shared_modules():
        sys.exit(1)
    
    # Note: We're not installing dependencies here since Lambda runtime provides boto3
    # and we want to keep packages lightweight. Dependencies will be installed
    # only if they're not available in the Lambda runtime.
    
    print("✅ Optimized lambda functions prepared successfully!")
    print("\nNote: Dependencies are not pre-installed to keep packages lightweight.")
    print("Lambda runtime provides boto3/botocore. Other dependencies will be installed")
    print("only if needed and not available in the runtime.")

if __name__ == "__main__":
    main()