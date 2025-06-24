#!/usr/bin/env python3
"""
Create AWS Lambda Layer with ClickHouse dependencies using pip with Linux target
"""

import os
import shutil
import zipfile
import subprocess
import sys
from pathlib import Path

def create_layer():
    """Create the ClickHouse Lambda layer using pip with Linux target."""
    print("Creating ClickHouse Lambda Layer with Linux x86_64 dependencies...")
    
    base_path = Path(__file__).parent.parent
    layer_dir = base_path / "lambda-layers" / "clickhouse"
    python_dir = layer_dir / "python"
    
    # Clean and recreate the python directory
    if python_dir.exists():
        shutil.rmtree(python_dir)
    python_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Installing packages to: {python_dir}")
    
    # Install packages with Linux target platform
    packages = [
        "clickhouse-connect==0.8.17",
        "zstandard==0.23.0", 
        "lz4==4.4.4",
        "certifi>=2023.7.22",
        "urllib3>=1.26.0",
        "pytz>=2023.3"
    ]
    
    for package in packages:
        print(f"Installing {package}...")
        
        # Try different platform specifications
        platform_options = [
            ["--platform", "linux_x86_64", "--only-binary=:all:"],
            ["--platform", "manylinux1_x86_64", "--only-binary=:all:"],
            ["--platform", "manylinux2014_x86_64", "--only-binary=:all:"],
            []  # Fallback to default (may get source packages)
        ]
        
        success = False
        for platform_args in platform_options:
            cmd = [
                sys.executable, "-m", "pip", "install",
                "--target", str(python_dir),
                "--upgrade",
                package
            ] + platform_args
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ Successfully installed {package}")
                success = True
                break
            else:
                print(f"⚠️  Failed with platform {platform_args}: {result.stderr.strip()}")
        
        if not success:
            print(f"❌ Failed to install {package} with any platform option")
            return False
    
    # Create the layer zip file
    layer_zip = layer_dir / "clickhouse-layer.zip"
    if layer_zip.exists():
        layer_zip.unlink()
    
    print(f"Creating layer zip: {layer_zip}")
    with zipfile.ZipFile(layer_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(python_dir):
            for file in files:
                file_path = Path(root) / file
                arcname = file_path.relative_to(layer_dir)
                zipf.write(file_path, arcname)
    
    if layer_zip.exists():
        size_mb = layer_zip.stat().st_size / 1024 / 1024
        print(f"✅ Successfully created ClickHouse layer: {layer_zip}")
        print(f"Layer size: {size_mb:.2f} MB")
        
        # Verify the contents
        print("\nLayer contents:")
        with zipfile.ZipFile(layer_zip, 'r') as zipf:
            for name in sorted(zipf.namelist())[:20]:  # Show first 20 files
                print(f"  {name}")
            if len(zipf.namelist()) > 20:
                print(f"  ... and {len(zipf.namelist()) - 20} more files")
        
        return True
    else:
        print("❌ Layer zip file not found")
        return False

def main():
    """Main function."""
    if create_layer():
        print("\n🎉 ClickHouse Lambda layer created successfully!")
        print("\nNext steps:")
        print("1. Update the ClickHouse stack to use the new layer")
        print("2. Remove the direct package installations from Lambda functions")
        print("3. Deploy the updated stack")
        return True
    else:
        print("\n❌ Failed to create ClickHouse Lambda layer")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)