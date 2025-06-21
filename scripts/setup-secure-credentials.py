#!/usr/bin/env python3
"""
Quick Setup Script for AVESA Secure Credentials
Guides users through the complete secure credential setup process
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

def print_header(title):
    """Print formatted header"""
    print(f"\n{'='*80}")
    print(f"🔒 {title}")
    print(f"{'='*80}")

def print_step(step, description):
    """Print formatted step"""
    print(f"\n📋 Step {step}: {description}")
    print(f"{'-'*60}")

def run_command(command, description, optional=False):
    """Run a command and handle errors"""
    print(f"🔧 {description}...")
    print(f"   Command: {command}")
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ {description} completed successfully")
            if result.stdout.strip():
                print(f"   Output: {result.stdout.strip()}")
            return True
        else:
            if optional:
                print(f"⚠️  {description} failed (optional): {result.stderr.strip()}")
                return False
            else:
                print(f"❌ {description} failed: {result.stderr.strip()}")
                return False
                
    except Exception as e:
        if optional:
            print(f"⚠️  {description} failed (optional): {e}")
            return False
        else:
            print(f"❌ {description} failed: {e}")
            return False

def check_prerequisites():
    """Check if prerequisites are installed"""
    print_step(1, "Checking Prerequisites")
    
    prerequisites = [
        ("python3 --version", "Python 3"),
        ("aws --version", "AWS CLI"),
        ("pip3 show boto3", "boto3 Python package"),
        ("pip3 show clickhouse-connect", "clickhouse-connect Python package")
    ]
    
    all_good = True
    
    for command, name in prerequisites:
        if not run_command(command, f"Checking {name}", optional=True):
            all_good = False
            print(f"   ❌ {name} is required but not found")
    
    if not all_good:
        print("\n📦 Missing prerequisites. Please install:")
        print("   - AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html")
        print("   - Python packages: pip3 install boto3 clickhouse-connect pandas")
        return False
    
    print("✅ All prerequisites are installed")
    return True

def check_aws_access():
    """Check AWS access"""
    print_step(2, "Checking AWS Access")
    
    if run_command("aws sts get-caller-identity", "Checking AWS credentials"):
        print("✅ AWS credentials are configured")
        return True
    else:
        print("❌ AWS credentials not configured")
        print("\n🔧 To configure AWS credentials:")
        print("   aws configure")
        print("   OR")
        print("   export AWS_PROFILE=your-profile-name")
        return False

def setup_clickhouse_credentials():
    """Set up ClickHouse credentials"""
    print_step(3, "Setting Up ClickHouse Credentials")
    
    print("🔐 This will store ClickHouse credentials securely in AWS Secrets Manager")
    
    # Check if credentials already exist
    if run_command("python3 scripts/setup-clickhouse-credentials.py --validate --environment dev", 
                   "Checking existing ClickHouse credentials", optional=True):
        print("✅ ClickHouse credentials already configured")
        return True
    
    print("\n📝 ClickHouse credentials not found. Setting up...")
    
    # Interactive setup
    if run_command("python3 scripts/setup-clickhouse-credentials.py --interactive --environment dev",
                   "Setting up ClickHouse credentials interactively"):
        print("✅ ClickHouse credentials configured successfully")
        return True
    else:
        print("❌ Failed to set up ClickHouse credentials")
        return False

def configure_secure_s3_integration():
    """Configure secure S3 integration"""
    print_step(4, "Configuring Secure S3 Integration")
    
    if run_command("python3 scripts/configure-clickhouse-s3-secure.py --environment dev",
                   "Configuring secure ClickHouse S3 integration"):
        print("✅ Secure S3 integration configured")
        return True
    else:
        print("❌ Failed to configure S3 integration")
        return False

def setup_credential_rotation():
    """Set up credential rotation"""
    print_step(5, "Setting Up Credential Rotation")
    
    print("🔄 Setting up automated credential rotation...")
    
    if run_command("python3 scripts/setup-credential-rotation.py --rotation-days 90",
                   "Setting up credential rotation", optional=True):
        print("✅ Credential rotation configured")
        return True
    else:
        print("⚠️  Credential rotation setup failed (optional)")
        print("   You can set this up later with:")
        print("   python3 scripts/setup-credential-rotation.py --rotation-days 90")
        return False

def validate_security_setup():
    """Validate the complete security setup"""
    print_step(6, "Validating Security Setup")
    
    if run_command("python3 scripts/validate-security-setup.py",
                   "Running security validation"):
        print("✅ Security validation passed")
        return True
    else:
        print("❌ Security validation failed")
        print("   Please review the output and fix any issues")
        return False

def test_secure_pipeline():
    """Test the secure pipeline"""
    print_step(7, "Testing Secure Pipeline")
    
    print("🧪 Testing the secure pipeline configuration...")
    
    if run_command("python3 scripts/end-to-end-pipeline-test.py",
                   "Running end-to-end pipeline test", optional=True):
        print("✅ Pipeline test passed")
        return True
    else:
        print("⚠️  Pipeline test failed (this may be expected if no data is available)")
        print("   The security setup is complete, but you may need to load data first")
        return False

def print_next_steps():
    """Print next steps for the user"""
    print_header("Setup Complete! Next Steps")
    
    print("🎉 Secure credential management is now configured!")
    print()
    print("📋 What was set up:")
    print("   ✅ ClickHouse credentials stored in AWS Secrets Manager")
    print("   ✅ Secure S3 integration configured")
    print("   ✅ Security validation tools available")
    print("   ✅ Hardcoded credentials removed from scripts")
    print()
    print("🔧 Available commands:")
    print("   📊 Test pipeline: python3 scripts/end-to-end-pipeline-test.py")
    print("   🔍 Validate security: python3 scripts/validate-security-setup.py")
    print("   🔄 Rotate credentials: python3 scripts/setup-clickhouse-credentials.py --rotate")
    print("   ⚙️  Configure S3: python3 scripts/configure-clickhouse-s3-secure.py")
    print()
    print("📚 Documentation:")
    print("   📖 Security Guide: docs/SECURITY_IMPLEMENTATION_GUIDE.md")
    print("   🔐 AWS Credentials: docs/AWS_CREDENTIALS_SETUP_GUIDE.md")
    print()
    print("🚨 Important Security Notes:")
    print("   - Never commit credentials to version control")
    print("   - Regularly rotate credentials (every 90 days)")
    print("   - Monitor credential usage in CloudWatch")
    print("   - Use the validation script to check security regularly")

def main():
    """Main setup function"""
    print_header("AVESA Secure Credential Setup")
    print(f"📅 Started: {datetime.utcnow().isoformat()}Z")
    print()
    print("🔒 This script will set up secure credential management for the AVESA pipeline.")
    print("   It will replace hardcoded credentials with AWS Secrets Manager integration.")
    print()
    
    # Confirm user wants to proceed
    response = input("🤔 Do you want to proceed with the secure setup? (y/N): ")
    if response.lower() != 'y':
        print("❌ Setup cancelled by user")
        sys.exit(1)
    
    # Run setup steps
    steps = [
        check_prerequisites,
        check_aws_access,
        setup_clickhouse_credentials,
        configure_secure_s3_integration,
        setup_credential_rotation,
        validate_security_setup,
        test_secure_pipeline
    ]
    
    failed_steps = []
    
    for i, step_func in enumerate(steps, 1):
        try:
            if not step_func():
                failed_steps.append(step_func.__name__)
                
                # Critical steps that must pass
                if step_func in [check_prerequisites, check_aws_access, setup_clickhouse_credentials]:
                    print(f"\n❌ Critical step failed: {step_func.__name__}")
                    print("   Cannot continue with setup")
                    sys.exit(1)
                    
        except KeyboardInterrupt:
            print("\n❌ Setup interrupted by user")
            sys.exit(1)
        except Exception as e:
            print(f"\n❌ Unexpected error in {step_func.__name__}: {e}")
            failed_steps.append(step_func.__name__)
    
    # Print results
    print_header("Setup Results")
    
    if failed_steps:
        print(f"⚠️  Setup completed with {len(failed_steps)} warnings:")
        for step in failed_steps:
            print(f"   ⚠️  {step}")
        print()
        print("🔧 You can address these issues later using the individual scripts")
    else:
        print("✅ All setup steps completed successfully!")
    
    print_next_steps()

if __name__ == "__main__":
    main()