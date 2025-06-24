#!/usr/bin/env python3
"""
Clean up directly installed ClickHouse packages from Lambda functions
since they will now be provided by the Lambda layer.
"""

import os
import shutil
import sys
from pathlib import Path

def cleanup_packages():
    """Remove ClickHouse packages from Lambda function directories."""
    print("Cleaning up ClickHouse packages from Lambda function directories...")
    
    base_path = Path(__file__).parent.parent
    clickhouse_dir = base_path / "src" / "clickhouse"
    
    # Lambda function directories to clean
    lambda_dirs = [
        "data_loader",
        "scd_processor", 
        "schema_init"
    ]
    
    # Packages to remove (these will be provided by the Lambda layer)
    packages_to_remove = [
        "clickhouse_connect",
        "clickhouse_connect-0.8.17.dist-info",
        "lz4",
        "lz4-4.4.4.dist-info",
        "zstandard", 
        "zstandard-0.23.0.dist-info",
        "certifi",
        "certifi-2025.6.15.dist-info",
        "urllib3",
        "urllib3-2.5.0.dist-info",
        "pytz",
        "pytz-2025.2.dist-info"
    ]
    
    total_removed = 0
    total_size_saved = 0
    
    for lambda_dir in lambda_dirs:
        lambda_path = clickhouse_dir / lambda_dir
        if not lambda_path.exists():
            print(f"⚠️  Lambda directory not found: {lambda_path}")
            continue
            
        print(f"\nCleaning {lambda_dir}...")
        
        for package in packages_to_remove:
            package_path = lambda_path / package
            if package_path.exists():
                # Calculate size before removal
                if package_path.is_dir():
                    size = sum(f.stat().st_size for f in package_path.rglob('*') if f.is_file())
                else:
                    size = package_path.stat().st_size
                
                # Remove the package
                if package_path.is_dir():
                    shutil.rmtree(package_path)
                else:
                    package_path.unlink()
                
                total_size_saved += size
                total_removed += 1
                print(f"  ✅ Removed {package} ({size / 1024 / 1024:.2f} MB)")
            else:
                print(f"  ⚠️  Package not found: {package}")
    
    print(f"\n🎉 Cleanup completed!")
    print(f"📦 Removed {total_removed} packages")
    print(f"💾 Total space saved: {total_size_saved / 1024 / 1024:.2f} MB")
    
    return True

def main():
    """Main function."""
    if cleanup_packages():
        print("\n✅ Lambda package cleanup completed successfully!")
        print("\nNext steps:")
        print("1. Deploy the updated ClickHouse stack with the new Lambda layer")
        print("2. Test the ClickHouse Lambda functions")
        return True
    else:
        print("\n❌ Failed to clean up Lambda packages")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)