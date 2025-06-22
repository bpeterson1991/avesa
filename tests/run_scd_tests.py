#!/usr/bin/env python3
"""
SCD Test Runner
===============

Comprehensive test runner for SCD-aware validation that includes:
- SCD behavior validation tests
- Pipeline validation with SCD awareness
- Schema-configuration alignment tests
- Mixed SCD scenario testing
"""

import sys
import argparse
import subprocess
from pathlib import Path
import json
from datetime import datetime

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

try:
    from shared.scd_config import SCDConfigManager
    from shared.logger import get_logger
    SHARED_IMPORTS_AVAILABLE = True
except ImportError:
    SHARED_IMPORTS_AVAILABLE = False
    print("⚠️ Shared imports not available, running basic tests only")

logger = get_logger(__name__) if SHARED_IMPORTS_AVAILABLE else None


class SCDTestRunner:
    """Comprehensive SCD test runner."""
    
    def __init__(self, mode='full'):
        self.mode = mode
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'mode': mode,
            'tests': {},
            'overall_status': 'PENDING'
        }
        
        if SHARED_IMPORTS_AVAILABLE:
            self.scd_manager = SCDConfigManager()
        else:
            self.scd_manager = None

    def print_header(self, title):
        """Print formatted section header."""
        print(f"\n{'='*80}")
        print(f"🧪 {title}")
        print(f"{'='*80}")

    def print_step(self, step):
        """Print formatted step."""
        print(f"\n📋 {step}")
        print(f"{'-'*60}")

    def print_result(self, message, success=True):
        """Print formatted result."""
        icon = "✅" if success else "❌"
        print(f"{icon} {message}")

    def run_scd_behavior_tests(self):
        """Run SCD behavior validation tests."""
        self.print_step("Running SCD Behavior Tests")
        
        try:
            # Run pytest on the SCD behavior test file
            result = subprocess.run([
                sys.executable, '-m', 'pytest', 
                'tests/test_scd_behavior.py', 
                '-v', '--tb=short'
            ], capture_output=True, text=True, cwd=Path(__file__).parent.parent)
            
            success = result.returncode == 0
            
            if success:
                self.print_result("SCD behavior tests passed")
                passed_count = result.stdout.count(' PASSED')
                self.print_result(f"Passed {passed_count} SCD behavior tests")
            else:
                self.print_result("SCD behavior tests failed", False)
                print(f"STDOUT:\n{result.stdout}")
                print(f"STDERR:\n{result.stderr}")
            
            self.results['tests']['scd_behavior'] = {
                'status': 'PASSED' if success else 'FAILED',
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr
            }
            
            return success
            
        except Exception as e:
            self.print_result(f"Failed to run SCD behavior tests: {e}", False)
            self.results['tests']['scd_behavior'] = {
                'status': 'ERROR',
                'error': str(e)
            }
            return False

    def run_scd_config_tests(self):
        """Run SCD configuration tests."""
        self.print_step("Running SCD Configuration Tests")
        
        try:
            # Run pytest on the SCD config test file
            result = subprocess.run([
                sys.executable, '-m', 'pytest', 
                'tests/test_scd_config.py', 
                '-v', '--tb=short'
            ], capture_output=True, text=True, cwd=Path(__file__).parent.parent)
            
            success = result.returncode == 0
            
            if success:
                self.print_result("SCD configuration tests passed")
                passed_count = result.stdout.count(' PASSED')
                self.print_result(f"Passed {passed_count} SCD configuration tests")
            else:
                self.print_result("SCD configuration tests failed", False)
                print(f"STDOUT:\n{result.stdout}")
                print(f"STDERR:\n{result.stderr}")
            
            self.results['tests']['scd_config'] = {
                'status': 'PASSED' if success else 'FAILED',
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr
            }
            
            return success
            
        except Exception as e:
            self.print_result(f"Failed to run SCD configuration tests: {e}", False)
            self.results['tests']['scd_config'] = {
                'status': 'ERROR',
                'error': str(e)
            }
            return False

    def run_pipeline_validation_tests(self):
        """Run enhanced pipeline validation tests."""
        self.print_step("Running Enhanced Pipeline Validation")
        
        try:
            # Run the enhanced pipeline validation
            result = subprocess.run([
                sys.executable, 'tests/validate-pipeline.py', 
                '--mode', 'data'
            ], capture_output=True, text=True, cwd=Path(__file__).parent.parent)
            
            success = result.returncode == 0
            
            if success:
                self.print_result("Enhanced pipeline validation passed")
            else:
                self.print_result("Enhanced pipeline validation failed", False)
                print(f"STDOUT:\n{result.stdout}")
                print(f"STDERR:\n{result.stderr}")
            
            self.results['tests']['pipeline_validation'] = {
                'status': 'PASSED' if success else 'FAILED',
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr
            }
            
            return success
            
        except Exception as e:
            self.print_result(f"Failed to run pipeline validation: {e}", False)
            self.results['tests']['pipeline_validation'] = {
                'status': 'ERROR',
                'error': str(e)
            }
            return False

    def validate_scd_configuration_consistency(self):
        """Validate SCD configuration consistency across the system."""
        self.print_step("Validating SCD Configuration Consistency")
        
        if not self.scd_manager:
            self.print_result("SCD manager not available, skipping consistency check", False)
            return False
        
        try:
            # Expected SCD configuration based on business requirements
            expected_config = {
                'companies': 'type_1',    # Simple upsert for companies
                'contacts': 'type_1',     # Simple upsert for contacts  
                'tickets': 'type_2',      # Full historical tracking for tickets
                'time_entries': 'type_1'  # Simple upsert for time entries
            }
            
            all_consistent = True
            consistency_results = {}
            
            for table, expected_type in expected_config.items():
                try:
                    actual_type = self.scd_manager.get_scd_type(table)
                    consistent = actual_type == expected_type
                    
                    consistency_results[table] = {
                        'expected': expected_type,
                        'actual': actual_type,
                        'consistent': consistent
                    }
                    
                    if consistent:
                        self.print_result(f"{table}: {actual_type} (consistent)")
                    else:
                        self.print_result(f"{table}: expected {expected_type}, got {actual_type}", False)
                        all_consistent = False
                        
                except Exception as e:
                    self.print_result(f"{table}: error getting SCD type - {e}", False)
                    consistency_results[table] = {
                        'expected': expected_type,
                        'actual': None,
                        'consistent': False,
                        'error': str(e)
                    }
                    all_consistent = False
            
            self.results['tests']['scd_consistency'] = {
                'status': 'PASSED' if all_consistent else 'FAILED',
                'results': consistency_results
            }
            
            if all_consistent:
                self.print_result("SCD configuration is consistent across all tables")
            else:
                self.print_result("SCD configuration inconsistencies detected", False)
            
            return all_consistent
            
        except Exception as e:
            self.print_result(f"Failed to validate SCD consistency: {e}", False)
            self.results['tests']['scd_consistency'] = {
                'status': 'ERROR',
                'error': str(e)
            }
            return False

    def validate_schema_alignment(self):
        """Validate that table schemas align with their SCD configuration."""
        self.print_step("Validating Schema-Configuration Alignment")
        
        if not self.scd_manager:
            self.print_result("SCD manager not available, skipping schema alignment check", False)
            return False
        
        try:
            # This would ideally connect to ClickHouse and validate actual schemas
            # For now, we'll validate the configuration logic
            
            tables = ['companies', 'contacts', 'tickets', 'time_entries']
            alignment_results = {}
            all_aligned = True
            
            for table in tables:
                try:
                    scd_type = self.scd_manager.get_scd_type(table)
                    validation_result = self.scd_manager.validate_scd_configuration(table)
                    
                    aligned = validation_result.get('valid', False)
                    alignment_results[table] = {
                        'scd_type': scd_type,
                        'validation_result': validation_result,
                        'aligned': aligned
                    }
                    
                    if aligned:
                        self.print_result(f"{table}: schema aligned with {scd_type} configuration")
                    else:
                        self.print_result(f"{table}: schema alignment issues detected", False)
                        all_aligned = False
                        
                except Exception as e:
                    self.print_result(f"{table}: error validating alignment - {e}", False)
                    alignment_results[table] = {
                        'aligned': False,
                        'error': str(e)
                    }
                    all_aligned = False
            
            self.results['tests']['schema_alignment'] = {
                'status': 'PASSED' if all_aligned else 'FAILED',
                'results': alignment_results
            }
            
            if all_aligned:
                self.print_result("All table schemas are aligned with their SCD configuration")
            else:
                self.print_result("Schema alignment issues detected", False)
            
            return all_aligned
            
        except Exception as e:
            self.print_result(f"Failed to validate schema alignment: {e}", False)
            self.results['tests']['schema_alignment'] = {
                'status': 'ERROR',
                'error': str(e)
            }
            return False

    def generate_report(self):
        """Generate comprehensive test report."""
        self.print_header("SCD TEST EXECUTION SUMMARY")
        
        # Calculate overall status
        test_statuses = []
        for test_name, test_data in self.results['tests'].items():
            status = test_data.get('status', 'UNKNOWN')
            test_statuses.append(status)
        
        # Determine overall status
        if all(status == 'PASSED' for status in test_statuses):
            overall_status = 'ALL_PASSED'
        elif any(status == 'PASSED' for status in test_statuses):
            overall_status = 'PARTIAL'
        else:
            overall_status = 'ALL_FAILED'
        
        self.results['overall_status'] = overall_status
        
        # Print summary
        print(f"🎯 SCD TEST SUITE STATUS: {overall_status}")
        print(f"⏰ Test Time: {self.results['timestamp']}")
        print(f"🔧 Test Mode: {self.mode.upper()}")
        print()
        
        # Component Status
        print("🧪 TEST RESULTS:")
        for test_name, test_data in self.results['tests'].items():
            status = test_data.get('status', 'UNKNOWN')
            status_icon = "✅" if status == 'PASSED' else "❌" if status == 'FAILED' else "⚠️"
            test_display_name = test_name.replace('_', ' ').title()
            print(f"   {status_icon} {test_display_name}: {status}")
        
        # Save detailed report
        report_filename = f"scd_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_filename, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        print(f"\n📄 Detailed report saved to: {report_filename}")
        
        return overall_status == 'ALL_PASSED'

    def run_full_test_suite(self):
        """Run the complete SCD test suite."""
        self.print_header("SCD-AWARE TEST SUITE")
        print(f"⏰ Started at: {datetime.now()}")
        print(f"🔧 Mode: {self.mode.upper()}")
        
        success_count = 0
        total_tests = 0
        
        # Run all test categories
        test_methods = [
            ('SCD Configuration Tests', self.run_scd_config_tests),
            ('SCD Behavior Tests', self.run_scd_behavior_tests),
            ('SCD Consistency Validation', self.validate_scd_configuration_consistency),
            ('Schema Alignment Validation', self.validate_schema_alignment)
        ]
        
        if self.mode in ['full', 'pipeline']:
            test_methods.append(('Enhanced Pipeline Validation', self.run_pipeline_validation_tests))
        
        for test_name, test_method in test_methods:
            try:
                if test_method():
                    success_count += 1
                total_tests += 1
            except Exception as e:
                print(f"❌ {test_name} failed with error: {e}")
                total_tests += 1
        
        # Generate final report
        overall_success = self.generate_report()
        
        # Final status
        print(f"\n🎯 TEST SUMMARY: {success_count}/{total_tests} test categories passed")
        
        if overall_success:
            print("🎉 SCD TEST SUITE: SUCCESS")
            print("✅ All SCD-aware tests passed!")
        else:
            print("⚠️ SCD TEST SUITE: ISSUES DETECTED")
            print("❌ Some SCD tests need attention.")
        
        return overall_success


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='SCD-Aware Test Suite')
    parser.add_argument('--mode', choices=['full', 'behavior', 'config', 'pipeline', 'consistency'], 
                       default='full', help='Test mode (default: full)')
    
    args = parser.parse_args()
    
    runner = SCDTestRunner(mode=args.mode)
    
    if args.mode == 'full':
        success = runner.run_full_test_suite()
    elif args.mode == 'behavior':
        success = runner.run_scd_behavior_tests()
    elif args.mode == 'config':
        success = runner.run_scd_config_tests()
    elif args.mode == 'pipeline':
        success = runner.run_pipeline_validation_tests()
    elif args.mode == 'consistency':
        success = (runner.validate_scd_configuration_consistency() and 
                  runner.validate_schema_alignment())
        runner.generate_report()
    else:
        print(f"Unknown mode: {args.mode}")
        sys.exit(1)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()