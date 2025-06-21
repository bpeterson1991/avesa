#!/usr/bin/env python3
"""
Validation Testing Suite
========================

Unified test suite that consolidates:
- Security validation testing
- Dependency standardization validation
- ClickHouse deployment verification
- Shared utilities unit testing

Supports multiple test modes:
- full: Complete validation test suite
- security: Security validation only
- dependencies: Dependency validation only
- deployment: Deployment verification only
- utils: Unit tests for shared utilities
"""

import os
import sys
import re
import json
import boto3
import requests
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any
import argparse

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from shared.credential_manager import get_credential_manager
from shared.logger import get_logger
from shared.utils import (
    flatten_json, get_timestamp, get_s3_key, chunk_list,
    safe_get, normalize_datetime, calculate_data_freshness
)

logger = get_logger(__name__)

class ValidationTestSuite:
    def __init__(self, mode='full'):
        self.mode = mode
        self.issues = []
        self.warnings = []
        self.successes = []
        
    def print_header(self, title):
        """Print formatted test section header"""
        print(f"\n{'='*80}")
        print(f"🚀 {title}")
        print(f"{'='*80}")
        
    def print_step(self, step):
        """Print formatted test step"""
        print(f"\n📋 {step}")
        print(f"{'-'*60}")
        
    def print_result(self, message, success=True):
        """Print formatted test result"""
        icon = "✅" if success else "❌"
        print(f"{icon} {message}")
    
    def add_issue(self, severity: str, message: str, file_path: str = None):
        """Add a validation issue"""
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

    # ============================================================================
    # SECURITY VALIDATION TESTING
    # ============================================================================
    
    def test_security_validation(self):
        """Test security validation functionality"""
        self.print_header("SECURITY VALIDATION TEST")
        
        try:
            credential_manager = get_credential_manager()
            
            # Test hardcoded credentials scan
            self.print_step("Scanning for Hardcoded Credentials")
            hardcoded_found = self._scan_for_hardcoded_credentials()
            
            if not hardcoded_found:
                self.add_issue('SUCCESS', "No hardcoded credentials found")
                self.print_result("No hardcoded credentials found")
            else:
                self.add_issue('CRITICAL', f"Found {hardcoded_found} hardcoded credentials")
                self.print_result(f"Found {hardcoded_found} hardcoded credentials", False)
            
            # Test AWS credentials
            self.print_step("Validating AWS Credentials")
            try:
                aws_creds = credential_manager.get_aws_credentials_for_service('validation')
                if aws_creds.get('access_key_id') and aws_creds.get('secret_access_key'):
                    self.add_issue('SUCCESS', "AWS credentials available through secure methods")
                    self.print_result("AWS credentials validated")
                    aws_valid = True
                else:
                    self.add_issue('CRITICAL', "No AWS credentials available")
                    self.print_result("AWS credentials not available", False)
                    aws_valid = False
            except Exception as e:
                self.add_issue('CRITICAL', f"AWS credential validation failed: {e}")
                self.print_result(f"AWS credential validation failed: {e}", False)
                aws_valid = False
            
            # Test ClickHouse credentials
            self.print_step("Validating ClickHouse Credentials")
            try:
                clickhouse_creds = credential_manager.get_clickhouse_credentials('dev')
                if clickhouse_creds:
                    self.add_issue('SUCCESS', "ClickHouse credentials found in AWS Secrets Manager")
                    self.print_result("ClickHouse credentials validated")
                    clickhouse_valid = True
                else:
                    self.add_issue('CRITICAL', "ClickHouse credentials not found")
                    self.print_result("ClickHouse credentials not found", False)
                    clickhouse_valid = False
            except Exception as e:
                self.add_issue('CRITICAL', f"ClickHouse credential validation failed: {e}")
                self.print_result(f"ClickHouse credential validation failed: {e}", False)
                clickhouse_valid = False
            
            # Test Secrets Manager setup
            self.print_step("Validating Secrets Manager Setup")
            secrets_valid = self._validate_secrets_manager()
            
            overall_success = (not hardcoded_found) and aws_valid and clickhouse_valid and secrets_valid
            return overall_success
            
        except Exception as e:
            self.print_result(f"Security validation test failed: {e}", False)
            return False
    
    def _scan_for_hardcoded_credentials(self):
        """Scan for hardcoded credentials"""
        credential_patterns = [
            (r'password\s*=\s*[\'"][^\'"\s]{8,}[\'"]', 'Hardcoded password'),
            (r'secret\s*=\s*[\'"][^\'"\s]{20,}[\'"]', 'Hardcoded secret'),
            (r'AKIA[0-9A-Z]{16}', 'AWS Access Key ID'),
            (r'AdministratorAccess-\d+', 'Hardcoded AWS profile'),
        ]
        
        scan_paths = ['scripts/', 'src/', 'infrastructure/', 'tests/']
        issues_found = 0
        
        for scan_path in scan_paths:
            if not os.path.exists(scan_path):
                continue
                
            for root, dirs, files in os.walk(scan_path):
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
                                    self.add_issue('CRITICAL', f"{description} found at line {line_num}", file_path)
                                    issues_found += 1
                                    
                        except Exception:
                            continue
        
        return issues_found
    
    def _validate_secrets_manager(self):
        """Validate AWS Secrets Manager setup"""
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
                self.print_result(f"Found {len(avesa_secrets)} AVESA secrets")
                return True
            else:
                self.add_issue('WARNING', "No AVESA secrets found in Secrets Manager")
                self.print_result("No AVESA secrets found", False)
                return False
                
        except Exception as e:
            self.add_issue('CRITICAL', f"Secrets Manager validation failed: {e}")
            self.print_result(f"Secrets Manager validation failed: {e}", False)
            return False

    # ============================================================================
    # DEPENDENCY VALIDATION TESTING
    # ============================================================================
    
    def test_dependency_validation(self):
        """Test dependency standardization validation"""
        self.print_header("DEPENDENCY VALIDATION TEST")
        
        # Expected standardized versions
        expected_versions = {
            'boto3': '1.38.39',
            'botocore': '1.38.39',
            'clickhouse-connect': '0.8.17',
            'pandas': '2.2.3',
            'pyarrow': '18.1.0',
            'aws-cdk-lib': '2.100.0'
        }
        
        # Files to check with their expected version types
        files_to_check = {
            'src/clickhouse/schema_init/requirements.txt': {
                'boto3': '==',
                'botocore': '==',
                'clickhouse-connect': '=='
            },
            'src/clickhouse/data_loader/requirements.txt': {
                'boto3': '==',
                'botocore': '==',
                'clickhouse-connect': '==',
                'pandas': '==',
                'pyarrow': '=='
            },
            'src/clickhouse/scd_processor/requirements.txt': {
                'boto3': '==',
                'botocore': '==',
                'clickhouse-connect': '==',
                'pandas': '==',
                'pyarrow': '=='
            },
            'infrastructure/requirements.txt': {
                'boto3': '>=',
                'botocore': '>=',
                'aws-cdk-lib': '=='
            },
            'requirements.txt': {
                'boto3': '>=',
                'pandas': '>=',
                'pyarrow': '>=',
                'aws-cdk-lib': '>='
            }
        }
        
        all_valid = True
        
        for file_path, expected_packages in files_to_check.items():
            self.print_step(f"Checking: {file_path}")
            
            if not os.path.exists(file_path):
                self.print_result(f"File not found: {file_path}", False)
                all_valid = False
                continue
            
            packages = self._parse_requirements_file(file_path)
            
            for package, expected_operator in expected_packages.items():
                if package not in packages:
                    self.print_result(f"Missing package: {package}", False)
                    all_valid = False
                    continue
                
                operator, version = packages[package]
                expected_version = expected_versions[package]
                
                # Check operator
                if operator != expected_operator:
                    self.print_result(f"{package}: Expected operator '{expected_operator}', got '{operator}'", False)
                    all_valid = False
                    continue
                
                # Check version
                if operator == '==' and version != expected_version:
                    self.print_result(f"{package}: Expected version '{expected_version}', got '{version}'", False)
                    all_valid = False
                    continue
                elif operator == '>=' and not self._version_meets_minimum(version, expected_version):
                    self.print_result(f"{package}: Version '{version}' does not meet minimum '{expected_version}'", False)
                    all_valid = False
                    continue
                else:
                    self.print_result(f"{package}: {operator}{version}")
        
        if all_valid:
            self.print_result("All dependency versions are standardized correctly")
        else:
            self.print_result("Dependency standardization validation failed", False)
        
        return all_valid
    
    def _parse_requirements_file(self, file_path):
        """Parse a requirements.txt file and extract package versions"""
        packages = {}
        if not os.path.exists(file_path):
            return packages
        
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    # Handle different version specifiers
                    if '==' in line:
                        package, version = line.split('==', 1)
                        packages[package] = ('==', version)
                    elif '>=' in line:
                        package, version = line.split('>=', 1)
                        packages[package] = ('>=', version)
                    elif '>' in line:
                        package, version = line.split('>', 1)
                        packages[package] = ('>', version)
        
        return packages
    
    def _version_meets_minimum(self, current, minimum):
        """Check if current version meets minimum requirement"""
        try:
            current_parts = [int(x) for x in current.split('.')]
            minimum_parts = [int(x) for x in minimum.split('.')]
            
            # Pad shorter version with zeros
            max_len = max(len(current_parts), len(minimum_parts))
            current_parts.extend([0] * (max_len - len(current_parts)))
            minimum_parts.extend([0] * (max_len - len(minimum_parts)))
            
            return current_parts >= minimum_parts
        except:
            return False

    # ============================================================================
    # DEPLOYMENT VERIFICATION TESTING
    # ============================================================================
    
    def test_deployment_verification(self):
        """Test ClickHouse deployment verification"""
        self.print_header("DEPLOYMENT VERIFICATION TEST")
        
        region = 'us-east-2'
        stack_name = 'AVESAClickHouse-dev'
        
        try:
            # Initialize AWS clients
            cf_client = boto3.client('cloudformation', region_name=region)
            lambda_client = boto3.client('lambda', region_name=region)
            secrets_client = boto3.client('secretsmanager', region_name=region)
            
            # Check CloudFormation Stack
            self.print_step("Checking CloudFormation Stack")
            stack_info = self._check_cloudformation_stack(cf_client, stack_name)
            
            if stack_info['status'] == 'CREATE_COMPLETE':
                self.print_result(f"CloudFormation stack status: {stack_info['status']}")
                stack_valid = True
                
                # Extract outputs
                outputs = stack_info['outputs']
                secret_arn = outputs.get('ClickHouseSecretArn', '')
                
            else:
                self.print_result(f"CloudFormation stack status: {stack_info['status']}", False)
                stack_valid = False
                secret_arn = ''
            
            # Check Lambda Functions
            self.print_step("Checking Lambda Functions")
            lambda_functions = [
                'clickhouse-schema-init-dev',
                'clickhouse-loader-companies-dev',
                'clickhouse-scd-processor-dev'
            ]
            
            lambda_results = self._check_lambda_functions(lambda_client, lambda_functions)
            lambda_valid = all(result.get('state') == 'Active' for result in lambda_results.values())
            
            if lambda_valid:
                self.print_result(f"All {len(lambda_functions)} Lambda functions are active")
            else:
                active_count = sum(1 for result in lambda_results.values() if result.get('state') == 'Active')
                self.print_result(f"Only {active_count}/{len(lambda_functions)} Lambda functions are active", False)
            
            # Check Secrets Manager
            self.print_step("Checking Secrets Manager")
            if secret_arn:
                secret_info = self._check_secrets_manager(secrets_client, secret_arn)
                secrets_valid = secret_info['status'] == 'EXISTS'
                
                if secrets_valid:
                    self.print_result(f"ClickHouse secret exists: {secret_info['name']}")
                else:
                    self.print_result("ClickHouse secret not found", False)
            else:
                self.print_result("No secret ARN found in stack outputs", False)
                secrets_valid = False
            
            overall_success = stack_valid and lambda_valid and secrets_valid
            return overall_success
            
        except Exception as e:
            self.print_result(f"Deployment verification test failed: {e}", False)
            return False
    
    def _check_cloudformation_stack(self, cf_client, stack_name):
        """Check CloudFormation stack status and outputs"""
        try:
            response = cf_client.describe_stacks(StackName=stack_name)
            stack = response['Stacks'][0]
            
            outputs = {}
            if 'Outputs' in stack:
                outputs = {output['OutputKey']: output['OutputValue'] 
                          for output in stack['Outputs']}
            
            return {
                'status': stack['StackStatus'],
                'outputs': outputs,
                'creation_time': stack['CreationTime']
            }
        except Exception as e:
            return {'status': 'NOT_FOUND', 'error': str(e)}
    
    def _check_lambda_functions(self, lambda_client, function_names):
        """Check Lambda function status"""
        results = {}
        for func_name in function_names:
            try:
                response = lambda_client.get_function(FunctionName=func_name)
                config = response['Configuration']
                results[func_name] = {
                    'state': config.get('State', 'Unknown'),
                    'runtime': config.get('Runtime', 'Unknown'),
                    'last_modified': config.get('LastModified', 'Unknown')
                }
            except Exception as e:
                results[func_name] = {'state': 'NOT_FOUND', 'error': str(e)}
        
        return results
    
    def _check_secrets_manager(self, secrets_client, secret_arn):
        """Check Secrets Manager secret"""
        try:
            response = secrets_client.describe_secret(SecretId=secret_arn)
            return {
                'status': 'EXISTS',
                'name': response['Name'],
                'last_changed': response.get('LastChangedDate', 'Unknown')
            }
        except Exception as e:
            return {'status': 'NOT_FOUND', 'error': str(e)}

    # ============================================================================
    # SHARED UTILITIES TESTING
    # ============================================================================
    
    def test_shared_utils(self):
        """Test shared utilities functionality"""
        self.print_header("SHARED UTILITIES TEST")
        
        try:
            # Test flatten_json
            self.print_step("Testing flatten_json")
            test_data = {
                "user": {
                    "name": "John",
                    "details": {
                        "age": 30,
                        "city": "New York"
                    }
                }
            }
            expected = {
                "user__name": "John",
                "user__details__age": 30,
                "user__details__city": "New York"
            }
            result = flatten_json(test_data)
            flatten_success = result == expected
            self.print_result(f"flatten_json test: {'PASS' if flatten_success else 'FAIL'}", flatten_success)
            
            # Test get_s3_key
            self.print_step("Testing get_s3_key")
            key = get_s3_key("tenant1", "raw", "connectwise", "tickets", "2024-01-01T00:00:00Z")
            expected_key = "tenant1/raw/connectwise/tickets/2024-01-01T00:00:00Z.parquet"
            s3_key_success = key == expected_key
            self.print_result(f"get_s3_key test: {'PASS' if s3_key_success else 'FAIL'}", s3_key_success)
            
            # Test chunk_list
            self.print_step("Testing chunk_list")
            data = [1, 2, 3, 4, 5, 6]
            chunks = chunk_list(data, 2)
            expected_chunks = [[1, 2], [3, 4], [5, 6]]
            chunk_success = chunks == expected_chunks
            self.print_result(f"chunk_list test: {'PASS' if chunk_success else 'FAIL'}", chunk_success)
            
            # Test safe_get
            self.print_step("Testing safe_get")
            test_data = {
                "user": {
                    "profile": {
                        "name": "John"
                    }
                }
            }
            result = safe_get(test_data, "user.profile.name")
            safe_get_success = result == "John"
            self.print_result(f"safe_get test: {'PASS' if safe_get_success else 'FAIL'}", safe_get_success)
            
            # Test normalize_datetime
            self.print_step("Testing normalize_datetime")
            dt_str = "2024-01-01T12:30:45Z"
            result = normalize_datetime(dt_str)
            normalize_success = result == "2024-01-01T12:30:45Z"
            self.print_result(f"normalize_datetime test: {'PASS' if normalize_success else 'FAIL'}", normalize_success)
            
            # Test get_timestamp
            self.print_step("Testing get_timestamp")
            timestamp = get_timestamp()
            timestamp_success = timestamp.endswith('Z') and 'T' in timestamp
            self.print_result(f"get_timestamp test: {'PASS' if timestamp_success else 'FAIL'}", timestamp_success)
            
            # Calculate overall success
            all_tests = [flatten_success, s3_key_success, chunk_success, safe_get_success, normalize_success, timestamp_success]
            overall_success = all(all_tests)
            
            passed_count = sum(all_tests)
            total_count = len(all_tests)
            self.print_result(f"Shared utilities tests: {passed_count}/{total_count} passed")
            
            return overall_success
            
        except Exception as e:
            self.print_result(f"Shared utilities test failed: {e}", False)
            return False

    # ============================================================================
    # TEST EXECUTION MODES
    # ============================================================================
    
    def run_full_test(self):
        """Run complete validation test suite"""
        self.print_header("VALIDATION TEST SUITE - FULL MODE")
        
        try:
            test_results = [
                ('Security Validation', self.test_security_validation()),
                ('Dependency Validation', self.test_dependency_validation()),
                ('Deployment Verification', self.test_deployment_verification()),
                ('Shared Utilities', self.test_shared_utils())
            ]
            
            return self._generate_test_report(test_results)
            
        except Exception as e:
            print(f"❌ CRITICAL ERROR: {str(e)}")
            return False
    
    def run_security_test(self):
        """Run security validation test"""
        self.print_header("VALIDATION TEST SUITE - SECURITY MODE")
        
        try:
            test_results = [
                ('Security Validation', self.test_security_validation())
            ]
            
            return self._generate_test_report(test_results)
            
        except Exception as e:
            print(f"❌ CRITICAL ERROR: {str(e)}")
            return False
    
    def run_dependencies_test(self):
        """Run dependency validation test"""
        self.print_header("VALIDATION TEST SUITE - DEPENDENCIES MODE")
        
        try:
            test_results = [
                ('Dependency Validation', self.test_dependency_validation())
            ]
            
            return self._generate_test_report(test_results)
            
        except Exception as e:
            print(f"❌ CRITICAL ERROR: {str(e)}")
            return False
    
    def run_deployment_test(self):
        """Run deployment verification test"""
        self.print_header("VALIDATION TEST SUITE - DEPLOYMENT MODE")
        
        try:
            test_results = [
                ('Deployment Verification', self.test_deployment_verification())
            ]
            
            return self._generate_test_report(test_results)
            
        except Exception as e:
            print(f"❌ CRITICAL ERROR: {str(e)}")
            return False
    
    def run_utils_test(self):
        """Run shared utilities test"""
        self.print_header("VALIDATION TEST SUITE - UTILS MODE")
        
        try:
            test_results = [
                ('Shared Utilities', self.test_shared_utils())
            ]
            
            return self._generate_test_report(test_results)
            
        except Exception as e:
            print(f"❌ CRITICAL ERROR: {str(e)}")
            return False
    
    def _generate_test_report(self, test_results):
        """Generate test report"""
        self.print_header("TEST EXECUTION SUMMARY")
        
        overall_success = True
        for test_name, success in test_results:
            icon = "✅" if success else "❌"
            print(f"{icon} {test_name}: {'SUCCESS' if success else 'FAILED'}")
            if not success:
                overall_success = False
        
        # Summary of issues
        if self.issues:
            print(f"\n🚨 Critical Issues: {len(self.issues)}")
            for issue in self.issues[:5]:  # Show first 5 issues
                print(f"   ❌ {issue['message']}")
            if len(self.issues) > 5:
                print(f"   ... and {len(self.issues) - 5} more issues")
        
        if self.warnings:
            print(f"\n⚠️  Warnings: {len(self.warnings)}")
            for warning in self.warnings[:3]:  # Show first 3 warnings
                print(f"   ⚠️  {warning['message']}")
            if len(self.warnings) > 3:
                print(f"   ... and {len(self.warnings) - 3} more warnings")
        
        overall_status = "✅ VALIDATION PASSED" if overall_success else "❌ VALIDATION FAILED"
        print(f"\n🎯 OVERALL STATUS: {overall_status}")
        
        return overall_success

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Validation Testing Suite')
    parser.add_argument('--mode', '-m', choices=['full', 'security', 'dependencies', 'deployment', 'utils'], 
                       default='full', help='Test mode to run')
    
    args = parser.parse_args()
    
    runner = ValidationTestSuite(args.mode)
    
    if args.mode == 'full':
        success = runner.run_full_test()
    elif args.mode == 'security':
        success = runner.run_security_test()
    elif args.mode == 'dependencies':
        success = runner.run_dependencies_test()
    elif args.mode == 'deployment':
        success = runner.run_deployment_test()
    elif args.mode == 'utils':
        success = runner.run_utils_test()
    else:
        print(f"Unknown mode: {args.mode}")
        sys.exit(1)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()