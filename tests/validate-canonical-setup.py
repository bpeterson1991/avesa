#!/usr/bin/env python3
"""
Canonical Transform Setup Validation

This script validates the canonical transform pipeline setup by checking:
1. Required files and configurations exist
2. Canonical mappings are valid
3. Lambda function code structure
4. Infrastructure configuration
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Any


class CanonicalSetupValidator:
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.validation_results = {
            'files_check': {},
            'mappings_check': {},
            'lambda_code_check': {},
            'infrastructure_check': {},
            'overall_valid': False
        }
    
    def validate_required_files(self) -> Dict[str, bool]:
        """Validate that all required files exist."""
        print("🔍 Validating required files...")
        
        required_files = {
            'canonical_lambda': 'src/canonical_transform/lambda_function.py',
            'canonical_requirements': 'src/canonical_transform/requirements.txt',
            'companies_mapping': 'mappings/canonical/companies.json',
            'contacts_mapping': 'mappings/canonical/contacts.json',
            'tickets_mapping': 'mappings/canonical/tickets.json',
            'time_entries_mapping': 'mappings/canonical/time_entries.json',
            'infrastructure_stack': 'infrastructure/stacks/data_pipeline_stack.py',
            'test_lambda_functions': 'tests/test-lambda-functions.py',
            'test_canonical_pipeline': 'tests/test-canonical-transform-pipeline.py'
        }
        
        file_results = {}
        for name, file_path in required_files.items():
            full_path = self.project_root / file_path
            exists = full_path.exists()
            file_results[name] = exists
            
            status = "✅" if exists else "❌"
            print(f"   {status} {name}: {file_path}")
        
        self.validation_results['files_check'] = file_results
        return file_results
    
    def validate_canonical_mappings(self) -> Dict[str, Any]:
        """Validate canonical mapping files."""
        print("\n🗺️  Validating canonical mappings...")
        
        mapping_files = [
            'mappings/canonical/companies.json',
            'mappings/canonical/contacts.json', 
            'mappings/canonical/tickets.json',
            'mappings/canonical/time_entries.json'
        ]
        
        mapping_results = {}
        
        for mapping_file in mapping_files:
            table_name = Path(mapping_file).stem
            print(f"\n   📊 Validating {table_name} mapping...")
            
            full_path = self.project_root / mapping_file
            
            if not full_path.exists():
                mapping_results[table_name] = {'valid': False, 'error': 'File not found'}
                print(f"      ❌ File not found: {mapping_file}")
                continue
            
            try:
                with open(full_path, 'r') as f:
                    mapping_data = json.load(f)
                
                # Validate mapping structure
                validation = self._validate_mapping_structure(mapping_data, table_name)
                mapping_results[table_name] = validation
                
                if validation['valid']:
                    print(f"      ✅ Valid mapping structure")
                    print(f"         Services: {', '.join(validation['services'])}")
                    print(f"         ConnectWise fields: {validation['connectwise_field_count']}")
                else:
                    print(f"      ❌ Invalid mapping: {validation['error']}")
                    
            except json.JSONDecodeError as e:
                mapping_results[table_name] = {'valid': False, 'error': f'Invalid JSON: {str(e)}'}
                print(f"      ❌ Invalid JSON: {str(e)}")
            except Exception as e:
                mapping_results[table_name] = {'valid': False, 'error': str(e)}
                print(f"      ❌ Error: {str(e)}")
        
        self.validation_results['mappings_check'] = mapping_results
        return mapping_results
    
    def _validate_mapping_structure(self, mapping_data: Dict, table_name: str) -> Dict[str, Any]:
        """Validate the structure of a canonical mapping."""
        try:
            # Check for required services
            if 'connectwise' not in mapping_data:
                return {'valid': False, 'error': 'Missing connectwise mapping'}
            
            # Get ConnectWise mapping
            connectwise_mapping = mapping_data['connectwise']
            
            # Expected ConnectWise endpoints for each table
            expected_endpoints = {
                'companies': 'company/companies',
                'contacts': 'company/contacts',
                'tickets': 'service/tickets',
                'time_entries': 'time/entries'
            }
            
            expected_endpoint = expected_endpoints.get(table_name)
            if expected_endpoint and expected_endpoint not in connectwise_mapping:
                return {'valid': False, 'error': f'Missing expected endpoint: {expected_endpoint}'}
            
            # Count fields in ConnectWise mapping
            connectwise_fields = 0
            if expected_endpoint and expected_endpoint in connectwise_mapping:
                connectwise_fields = len(connectwise_mapping[expected_endpoint])
            
            # Get all services
            services = list(mapping_data.keys())
            
            return {
                'valid': True,
                'services': services,
                'connectwise_field_count': connectwise_fields,
                'has_required_fields': connectwise_fields > 5  # Minimum field count
            }
            
        except Exception as e:
            return {'valid': False, 'error': str(e)}
    
    def validate_lambda_code(self) -> Dict[str, Any]:
        """Validate canonical transform Lambda function code."""
        print("\n🔧 Validating Lambda function code...")
        
        lambda_file = self.project_root / 'src/canonical_transform/lambda_function.py'
        
        if not lambda_file.exists():
            result = {'valid': False, 'error': 'Lambda function file not found'}
            print("   ❌ Lambda function file not found")
            self.validation_results['lambda_code_check'] = result
            return result
        
        try:
            with open(lambda_file, 'r') as f:
                code_content = f.read()
            
            # Check for required functions and imports
            required_elements = {
                'lambda_handler': 'def lambda_handler(',
                'process_tenant_canonical_data': 'def process_tenant_canonical_data(',
                'load_and_transform_raw_data': 'def load_and_transform_raw_data(',
                'write_canonical_data_to_s3': 'def write_canonical_data_to_s3(',
                'apply_scd_type2_logic': 'def apply_scd_type2_logic(',
                'pandas_import': 'import pandas',
                's3_client': 'get_s3_client',
                'dynamodb_client': 'get_dynamodb_client'
            }
            
            missing_elements = []
            for element, pattern in required_elements.items():
                if pattern not in code_content:
                    missing_elements.append(element)
            
            # Check for canonical table environment variable usage
            has_canonical_table_env = 'CANONICAL_TABLE' in code_content
            
            # Check for parquet writing capability
            has_parquet_support = 'to_parquet' in code_content
            
            result = {
                'valid': len(missing_elements) == 0,
                'missing_elements': missing_elements,
                'has_canonical_table_env': has_canonical_table_env,
                'has_parquet_support': has_parquet_support,
                'code_length': len(code_content)
            }
            
            if result['valid']:
                print("   ✅ Lambda function code structure is valid")
                print(f"      Code length: {result['code_length']} characters")
                print(f"      Canonical table env support: {'✅' if has_canonical_table_env else '❌'}")
                print(f"      Parquet support: {'✅' if has_parquet_support else '❌'}")
            else:
                print("   ❌ Lambda function code issues found")
                print(f"      Missing elements: {', '.join(missing_elements)}")
            
            self.validation_results['lambda_code_check'] = result
            return result
            
        except Exception as e:
            result = {'valid': False, 'error': str(e)}
            print(f"   ❌ Error reading Lambda code: {str(e)}")
            self.validation_results['lambda_code_check'] = result
            return result
    
    def validate_infrastructure_config(self) -> Dict[str, Any]:
        """Validate infrastructure configuration."""
        print("\n🏗️  Validating infrastructure configuration...")
        
        stack_file = self.project_root / 'infrastructure/stacks/data_pipeline_stack.py'
        
        if not stack_file.exists():
            result = {'valid': False, 'error': 'Infrastructure stack file not found'}
            print("   ❌ Infrastructure stack file not found")
            self.validation_results['infrastructure_check'] = result
            return result
        
        try:
            with open(stack_file, 'r') as f:
                stack_content = f.read()
            
            # Check for canonical transform Lambda creation
            required_infrastructure = {
                'canonical_transform_lambdas': '_create_canonical_transform_lambdas',
                'canonical_tables_list': 'canonical_tables = ["tickets", "time_entries", "companies", "contacts"]',
                'lambda_environment_vars': 'CANONICAL_TABLE',
                's3_bucket_creation': '_create_data_bucket',
                'dynamodb_tables': '_create_tenant_services_table',
                'iam_role': '_create_lambda_role'
            }
            
            missing_infrastructure = []
            for element, pattern in required_infrastructure.items():
                if pattern not in stack_content:
                    missing_infrastructure.append(element)
            
            # Check for proper function naming
            has_proper_naming = 'avesa-canonical-transform-' in stack_content
            
            # Check for environment variable configuration
            has_env_config = 'CANONICAL_TABLE": table' in stack_content
            
            result = {
                'valid': len(missing_infrastructure) == 0,
                'missing_infrastructure': missing_infrastructure,
                'has_proper_naming': has_proper_naming,
                'has_env_config': has_env_config,
                'stack_length': len(stack_content)
            }
            
            if result['valid']:
                print("   ✅ Infrastructure configuration is valid")
                print(f"      Proper function naming: {'✅' if has_proper_naming else '❌'}")
                print(f"      Environment config: {'✅' if has_env_config else '❌'}")
            else:
                print("   ❌ Infrastructure configuration issues found")
                print(f"      Missing infrastructure: {', '.join(missing_infrastructure)}")
            
            self.validation_results['infrastructure_check'] = result
            return result
            
        except Exception as e:
            result = {'valid': False, 'error': str(e)}
            print(f"   ❌ Error reading infrastructure code: {str(e)}")
            self.validation_results['infrastructure_check'] = result
            return result
    
    def generate_validation_report(self) -> bool:
        """Generate final validation report."""
        print(f"\n{'='*60}")
        print("📊 CANONICAL TRANSFORM SETUP VALIDATION REPORT")
        print(f"{'='*60}")
        
        # Files validation summary
        files_valid = all(self.validation_results['files_check'].values())
        print(f"📁 Required Files: {'✅ VALID' if files_valid else '❌ INVALID'}")
        
        # Mappings validation summary
        mappings_valid = all(
            result.get('valid', False) 
            for result in self.validation_results['mappings_check'].values()
        )
        print(f"🗺️  Canonical Mappings: {'✅ VALID' if mappings_valid else '❌ INVALID'}")
        
        # Lambda code validation summary
        lambda_valid = self.validation_results['lambda_code_check'].get('valid', False)
        print(f"🔧 Lambda Function Code: {'✅ VALID' if lambda_valid else '❌ INVALID'}")
        
        # Infrastructure validation summary
        infra_valid = self.validation_results['infrastructure_check'].get('valid', False)
        print(f"🏗️  Infrastructure Config: {'✅ VALID' if infra_valid else '❌ INVALID'}")
        
        # Overall validation
        overall_valid = files_valid and mappings_valid and lambda_valid and infra_valid
        self.validation_results['overall_valid'] = overall_valid
        
        print(f"\n🎯 Overall Setup Status: {'✅ READY FOR TESTING' if overall_valid else '❌ NEEDS ATTENTION'}")
        
        if overall_valid:
            print("\n🎉 Canonical Transform Pipeline Setup is Valid!")
            print("✅ All required files and configurations are present")
            print("✅ Canonical mappings are properly structured")
            print("✅ Lambda function code is complete")
            print("✅ Infrastructure configuration is correct")
            print("\n📋 Next Steps:")
            print("   1. Configure AWS credentials")
            print("   2. Deploy infrastructure: cd infrastructure && cdk deploy --context environment=dev")
            print("   3. Run canonical transform tests: cd tests && python3 test-canonical-transform-pipeline.py")
        else:
            print("\n❌ Canonical Transform Pipeline Setup has Issues!")
            print("⚠️  Please address the validation errors above before testing")
            
            # Provide specific recommendations
            if not files_valid:
                print("\n📁 File Issues:")
                for name, valid in self.validation_results['files_check'].items():
                    if not valid:
                        print(f"   - Missing: {name}")
            
            if not mappings_valid:
                print("\n🗺️  Mapping Issues:")
                for table, result in self.validation_results['mappings_check'].items():
                    if not result.get('valid', False):
                        print(f"   - {table}: {result.get('error', 'Unknown error')}")
            
            if not lambda_valid:
                print("\n🔧 Lambda Code Issues:")
                missing = self.validation_results['lambda_code_check'].get('missing_elements', [])
                if missing:
                    print(f"   - Missing elements: {', '.join(missing)}")
            
            if not infra_valid:
                print("\n🏗️  Infrastructure Issues:")
                missing = self.validation_results['infrastructure_check'].get('missing_infrastructure', [])
                if missing:
                    print(f"   - Missing infrastructure: {', '.join(missing)}")
        
        return overall_valid
    
    def run_validation(self) -> bool:
        """Run complete validation."""
        print("🔍 AVESA Canonical Transform Setup Validation")
        print("="*60)
        
        # Run all validations
        self.validate_required_files()
        self.validate_canonical_mappings()
        self.validate_lambda_code()
        self.validate_infrastructure_config()
        
        # Generate report
        return self.generate_validation_report()


def main():
    """Main execution function."""
    validator = CanonicalSetupValidator()
    success = validator.run_validation()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())