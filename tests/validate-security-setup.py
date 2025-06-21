#!/usr/bin/env python3
"""
Security Validation Script for AVESA Pipeline
Validates that all hardcoded credentials have been removed and secure practices are implemented
"""

import os
import sys
import re
import json
import boto3
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from shared.credential_manager import get_credential_manager
from shared.logger import get_logger

logger = get_logger(__name__)

class SecurityValidator:
    """Validates security configuration for AVESA pipeline"""
    
    def __init__(self):
        self.credential_manager = get_credential_manager()
        self.issues = []
        self.warnings = []
        self.successes = []
        
    def add_issue(self, severity: str, message: str, file_path: str = None):
        """Add a security issue"""
        issue = {
            'severity': severity,
            'message': message,
            'file': file_path,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        if severity == 'CRITICAL':
            self.issues.append(issue)
        elif severity == 'WARNING':
            self.warnings.append(issue)
        else:
            self.successes.append(issue)
    
    def scan_for_hardcoded_credentials(self) -> bool:
        """Scan all files for hardcoded credentials"""
        
        print("🔍 Scanning for hardcoded credentials...")
        
        # Patterns to look for
        credential_patterns = [
            (r'password\s*=\s*[\'"][^\'"\s]{8,}[\'"]', 'Hardcoded password'),
            (r'secret\s*=\s*[\'"][^\'"\s]{20,}[\'"]', 'Hardcoded secret'),
            (r'AKIA[0-9A-Z]{16}', 'AWS Access Key ID'),
            (r'[\'"][0-9a-zA-Z/+]{40}[\'"]', 'Potential AWS Secret Key'),
            (r'AdministratorAccess-\d+', 'Hardcoded AWS profile'),
            (r'UTqye_f~3GKay', 'Specific hardcoded ClickHouse password'),
            (r'wmk4p0wi7n\.us-east-2\.aws\.clickhouse\.cloud', 'Hardcoded ClickHouse host'),
        ]
        
        # Files to scan
        scan_paths = [
            'scripts/',
            'src/',
            'infrastructure/',
            'tests/'
        ]
        
        issues_found = False
        
        for scan_path in scan_paths:
            if not os.path.exists(scan_path):
                continue
                
            for root, dirs, files in os.walk(scan_path):
                # Skip certain directories
                dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ['__pycache__', 'node_modules']]
                
                for file in files:
                    if file.endswith(('.py', '.js', '.ts', '.json', '.yaml', '.yml')):
                        file_path = os.path.join(root, file)
                        
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                content = f.read()
                                
                            for pattern, description in credential_patterns:
                                matches = re.finditer(pattern, content, re.IGNORECASE)
                                for match in matches:
                                    line_num = content[:match.start()].count('\n') + 1
                                    self.add_issue(
                                        'CRITICAL',
                                        f"{description} found at line {line_num}: {match.group()[:50]}...",
                                        file_path
                                    )
                                    issues_found = True
                                    
                        except Exception as e:
                            self.add_issue('WARNING', f"Could not scan file: {e}", file_path)
        
        if not issues_found:
            self.add_issue('SUCCESS', "No hardcoded credentials found in scanned files")
            
        return not issues_found
    
    def validate_aws_credentials(self) -> bool:
        """Validate AWS credential configuration"""
        
        print("🔍 Validating AWS credential configuration...")
        
        try:
            # Check if we can get credentials without hardcoded values
            aws_creds = self.credential_manager.get_aws_credentials_for_service('validation')
            
            if aws_creds.get('access_key_id') and aws_creds.get('secret_access_key'):
                self.add_issue('SUCCESS', "AWS credentials available through IAM roles/profiles")
                
                # Validate credentials work
                if self.credential_manager.validate_credentials('s3'):
                    self.add_issue('SUCCESS', "AWS S3 credentials validated successfully")
                else:
                    self.add_issue('CRITICAL', "AWS S3 credentials validation failed")
                    return False
                    
                if self.credential_manager.validate_credentials('lambda'):
                    self.add_issue('SUCCESS', "AWS Lambda credentials validated successfully")
                else:
                    self.add_issue('WARNING', "AWS Lambda credentials validation failed")
                    
                return True
            else:
                self.add_issue('CRITICAL', "No AWS credentials available")
                return False
                
        except Exception as e:
            self.add_issue('CRITICAL', f"AWS credential validation failed: {e}")
            return False
    
    def validate_clickhouse_credentials(self) -> bool:
        """Validate ClickHouse credential configuration"""
        
        print("🔍 Validating ClickHouse credential configuration...")
        
        try:
            # Check if ClickHouse credentials are in Secrets Manager
            clickhouse_creds = self.credential_manager.get_clickhouse_credentials('dev')
            
            if clickhouse_creds:
                self.add_issue('SUCCESS', "ClickHouse credentials found in AWS Secrets Manager")
                
                # Validate credentials work
                if self.credential_manager.validate_credentials('clickhouse', 'dev'):
                    self.add_issue('SUCCESS', "ClickHouse credentials validated successfully")
                    return True
                else:
                    self.add_issue('CRITICAL', "ClickHouse credentials validation failed")
                    return False
            else:
                self.add_issue('CRITICAL', "ClickHouse credentials not found in Secrets Manager")
                return False
                
        except Exception as e:
            self.add_issue('CRITICAL', f"ClickHouse credential validation failed: {e}")
            return False
    
    def validate_secrets_manager_setup(self) -> bool:
        """Validate AWS Secrets Manager configuration"""
        
        print("🔍 Validating AWS Secrets Manager setup...")
        
        try:
            secrets_client = boto3.client('secretsmanager', region_name='us-east-2')
            
            # List AVESA secrets
            response = secrets_client.list_secrets(
                Filters=[
                    {
                        'Key': 'name',
                        'Values': ['avesa/']
                    }
                ]
            )
            
            avesa_secrets = response.get('SecretList', [])
            
            if avesa_secrets:
                self.add_issue('SUCCESS', f"Found {len(avesa_secrets)} AVESA secrets in Secrets Manager")
                
                for secret in avesa_secrets:
                    secret_name = secret['Name']
                    
                    # Check secret permissions
                    try:
                        secrets_client.describe_secret(SecretId=secret_name)
                        self.add_issue('SUCCESS', f"Secret accessible: {secret_name}")
                    except Exception as e:
                        self.add_issue('CRITICAL', f"Cannot access secret {secret_name}: {e}")
                        
                return True
            else:
                self.add_issue('WARNING', "No AVESA secrets found in Secrets Manager")
                return False
                
        except Exception as e:
            self.add_issue('CRITICAL', f"Secrets Manager validation failed: {e}")
            return False
    
    def validate_iam_roles(self) -> bool:
        """Validate IAM role configuration"""
        
        print("🔍 Validating IAM role configuration...")
        
        try:
            sts_client = boto3.client('sts', region_name='us-east-2')
            
            # Get current identity
            identity = sts_client.get_caller_identity()
            current_arn = identity.get('Arn', '')
            
            if 'role/' in current_arn:
                self.add_issue('SUCCESS', f"Using IAM role: {current_arn}")
                
                # Check if it's a proper service role (not user credentials)
                if any(service in current_arn for service in ['lambda', 'ec2', 'ecs']):
                    self.add_issue('SUCCESS', "Using service-based IAM role")
                else:
                    self.add_issue('WARNING', "Using assumed role (ensure it's properly configured)")
                    
                return True
            elif 'user/' in current_arn:
                self.add_issue('WARNING', f"Using IAM user credentials: {current_arn}")
                self.add_issue('WARNING', "Consider using IAM roles instead of user credentials")
                return True
            else:
                self.add_issue('CRITICAL', f"Unknown identity type: {current_arn}")
                return False
                
        except Exception as e:
            self.add_issue('CRITICAL', f"IAM role validation failed: {e}")
            return False
    
    def validate_lambda_functions(self) -> bool:
        """Validate Lambda function security configuration"""
        
        print("🔍 Validating Lambda function security...")
        
        try:
            lambda_client = boto3.client('lambda', region_name='us-east-2')
            
            # List AVESA Lambda functions
            response = lambda_client.list_functions()
            avesa_functions = [
                func for func in response['Functions']
                if any(name in func['FunctionName'] for name in ['canonical-transform', 'clickhouse-scd-processor'])
            ]
            
            if not avesa_functions:
                self.add_issue('WARNING', "No AVESA Lambda functions found")
                return False
            
            for func in avesa_functions:
                func_name = func['FunctionName']
                
                # Check environment variables for hardcoded secrets
                env_vars = func.get('Environment', {}).get('Variables', {})
                
                for var_name, var_value in env_vars.items():
                    if any(keyword in var_name.lower() for keyword in ['password', 'secret', 'key']):
                        if len(var_value) > 20:  # Likely a hardcoded secret
                            self.add_issue('CRITICAL', f"Potential hardcoded secret in {func_name}: {var_name}")
                        else:
                            self.add_issue('SUCCESS', f"Environment variable {var_name} in {func_name} appears safe")
                
                # Check if function has proper IAM role
                role_arn = func.get('Role', '')
                if role_arn:
                    self.add_issue('SUCCESS', f"Lambda {func_name} has IAM role: {role_arn}")
                else:
                    self.add_issue('CRITICAL', f"Lambda {func_name} missing IAM role")
            
            return True
            
        except Exception as e:
            self.add_issue('CRITICAL', f"Lambda function validation failed: {e}")
            return False
    
    def check_credential_rotation(self) -> bool:
        """Check if credential rotation is set up"""
        
        print("🔍 Checking credential rotation setup...")
        
        try:
            lambda_client = boto3.client('lambda', region_name='us-east-2')
            events_client = boto3.client('events', region_name='us-east-2')
            
            # Check if rotation Lambda exists
            try:
                lambda_client.get_function(FunctionName='avesa-credential-rotation')
                self.add_issue('SUCCESS', "Credential rotation Lambda function exists")
                
                # Check if EventBridge rule exists
                try:
                    events_client.describe_rule(Name='avesa-credential-rotation-schedule')
                    self.add_issue('SUCCESS', "Credential rotation schedule configured")
                    return True
                except events_client.exceptions.ResourceNotFoundException:
                    self.add_issue('WARNING', "Credential rotation schedule not configured")
                    return False
                    
            except lambda_client.exceptions.ResourceNotFoundException:
                self.add_issue('WARNING', "Credential rotation Lambda function not found")
                return False
                
        except Exception as e:
            self.add_issue('WARNING', f"Could not check credential rotation: {e}")
            return False
    
    def validate_network_security(self) -> bool:
        """Validate network security configuration"""
        
        print("🔍 Validating network security...")
        
        # This is a placeholder for network security validation
        # In a real implementation, you would check:
        # - VPC configuration
        # - Security groups
        # - NACLs
        # - Encryption in transit
        
        self.add_issue('SUCCESS', "Network security validation placeholder - implement based on your infrastructure")
        return True
    
    def generate_security_report(self) -> Dict:
        """Generate comprehensive security report"""
        
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'summary': {
                'critical_issues': len(self.issues),
                'warnings': len(self.warnings),
                'successes': len(self.successes)
            },
            'critical_issues': self.issues,
            'warnings': self.warnings,
            'successes': self.successes,
            'recommendations': []
        }
        
        # Add recommendations based on findings
        if self.issues:
            report['recommendations'].append("🚨 CRITICAL: Address all critical security issues immediately")
        
        if self.warnings:
            report['recommendations'].append("⚠️  Review and address security warnings")
        
        if not any('rotation' in str(issue) for issue in self.successes):
            report['recommendations'].append("🔄 Set up automated credential rotation")
        
        if not any('Secrets Manager' in str(issue) for issue in self.successes):
            report['recommendations'].append("🔐 Store all credentials in AWS Secrets Manager")
        
        report['recommendations'].extend([
            "📋 Regularly audit credential usage",
            "🔍 Implement continuous security monitoring",
            "📚 Train team on security best practices",
            "🔒 Enable AWS CloudTrail for audit logging"
        ])
        
        return report
    
    def run_full_validation(self) -> bool:
        """Run complete security validation"""
        
        print("🛡️  AVESA Pipeline Security Validation")
        print("=" * 60)
        
        validations = [
            ("Hardcoded Credentials Scan", self.scan_for_hardcoded_credentials),
            ("AWS Credentials", self.validate_aws_credentials),
            ("ClickHouse Credentials", self.validate_clickhouse_credentials),
            ("Secrets Manager Setup", self.validate_secrets_manager_setup),
            ("IAM Roles", self.validate_iam_roles),
            ("Lambda Functions", self.validate_lambda_functions),
            ("Credential Rotation", self.check_credential_rotation),
            ("Network Security", self.validate_network_security)
        ]
        
        results = {}
        overall_success = True
        
        for validation_name, validation_func in validations:
            try:
                result = validation_func()
                results[validation_name] = result
                if not result:
                    overall_success = False
                    
            except Exception as e:
                self.add_issue('CRITICAL', f"{validation_name} validation failed: {e}")
                results[validation_name] = False
                overall_success = False
        
        return overall_success

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate AVESA pipeline security configuration')
    parser.add_argument('--report-file', '-r', 
                       help='Save detailed report to file')
    parser.add_argument('--json-output', action='store_true',
                       help='Output results in JSON format')
    
    args = parser.parse_args()
    
    validator = SecurityValidator()
    
    print(f"🚀 Starting Security Validation")
    print(f"📅 Timestamp: {datetime.utcnow().isoformat()}Z")
    print()
    
    try:
        # Run validation
        success = validator.run_full_validation()
        
        # Generate report
        report = validator.generate_security_report()
        
        # Output results
        if args.json_output:
            print(json.dumps(report, indent=2))
        else:
            print("\n" + "=" * 60)
            print("🛡️  SECURITY VALIDATION SUMMARY")
            print("=" * 60)
            
            print(f"✅ Successes: {report['summary']['successes']}")
            print(f"⚠️  Warnings: {report['summary']['warnings']}")
            print(f"🚨 Critical Issues: {report['summary']['critical_issues']}")
            
            if report['critical_issues']:
                print("\n🚨 CRITICAL ISSUES:")
                for issue in report['critical_issues']:
                    print(f"   ❌ {issue['message']}")
                    if issue['file']:
                        print(f"      📁 File: {issue['file']}")
            
            if report['warnings']:
                print("\n⚠️  WARNINGS:")
                for warning in report['warnings']:
                    print(f"   ⚠️  {warning['message']}")
                    if warning['file']:
                        print(f"      📁 File: {warning['file']}")
            
            print("\n📋 RECOMMENDATIONS:")
            for rec in report['recommendations']:
                print(f"   {rec}")
        
        # Save report if requested
        if args.report_file:
            with open(args.report_file, 'w') as f:
                json.dump(report, f, indent=2)
            print(f"\n📄 Detailed report saved to: {args.report_file}")
        
        # Exit with appropriate code
        if success and not report['critical_issues']:
            print("\n🎉 Security validation passed!")
            sys.exit(0)
        else:
            print("\n❌ Security validation failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Validation failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()