#!/usr/bin/env python3
"""
Build optimized Lambda packages for the AVESA pipeline.
"""

import os
import shutil
import zipfile
import tempfile
from pathlib import Path

def create_optimized_orchestrator_package():
    """Create the optimized orchestrator package."""
    print("📦 Building optimized orchestrator package...")
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        package_dir = Path(temp_dir) / "package"
        package_dir.mkdir()
        
        # Copy orchestrator lambda function
        orchestrator_src = Path("src/optimized/orchestrator/lambda_function.py")
        shutil.copy2(orchestrator_src, package_dir / "lambda_function.py")
        
        # Copy shared modules
        shared_src = Path("src/shared")
        for py_file in shared_src.glob("*.py"):
            shutil.copy2(py_file, package_dir / py_file.name)
        
        # Copy mappings directory
        mappings_src = Path("mappings")
        mappings_dst = package_dir / "mappings"
        shutil.copytree(mappings_src, mappings_dst)
        
        # Create zip file
        zip_path = Path("lambda-packages/optimized-orchestrator.zip")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in package_dir.rglob("*"):
                if file_path.is_file():
                    arcname = file_path.relative_to(package_dir)
                    zipf.write(file_path, arcname)
        
        print(f"✅ Created {zip_path}")

def create_optimized_processors_package():
    """Create the optimized processors package."""
    print("📦 Building optimized processors package...")
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        package_dir = Path(temp_dir) / "package"
        package_dir.mkdir()
        
        # Copy all processor files
        processors_src = Path("src/optimized/processors")
        for py_file in processors_src.glob("*.py"):
            shutil.copy2(py_file, package_dir / py_file.name)
        
        # Copy all helper files
        helpers_src = Path("src/optimized/helpers")
        for py_file in helpers_src.glob("*.py"):
            shutil.copy2(py_file, package_dir / py_file.name)
        
        # Copy shared modules
        shared_src = Path("src/shared")
        for py_file in shared_src.glob("*.py"):
            shutil.copy2(py_file, package_dir / py_file.name)
        
        # Copy mappings directory
        mappings_src = Path("mappings")
        mappings_dst = package_dir / "mappings"
        shutil.copytree(mappings_src, mappings_dst)
        
        # Create zip file
        zip_path = Path("lambda-packages/optimized-processors.zip")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in package_dir.rglob("*"):
                if file_path.is_file():
                    arcname = file_path.relative_to(package_dir)
                    zipf.write(file_path, arcname)
        
        print(f"✅ Created {zip_path}")

def main():
    """Main function."""
    print("🚀 Building optimized Lambda packages...")
    
    # Ensure lambda-packages directory exists
    os.makedirs("lambda-packages", exist_ok=True)
    
    # Build packages
    create_optimized_orchestrator_package()
    create_optimized_processors_package()
    
    print("✅ All optimized packages built successfully!")

if __name__ == "__main__":
    main()