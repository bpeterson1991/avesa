"""
Tests for standardized argument parser module.
"""
import pytest
import argparse
from unittest.mock import patch

# Add scripts/shared to path for testing
import sys
from pathlib import Path
scripts_shared_path = Path(__file__).parent.parent / 'scripts' / 'shared'
if str(scripts_shared_path) not in sys.path:
    sys.path.insert(0, str(scripts_shared_path))

from arg_parser import StandardArgumentParser


class TestStandardArgumentParser:
    """Test cases for StandardArgumentParser class."""
    
    def test_create_base_parser(self):
        """Test creation of base parser with standard arguments."""
        description = "Test script description"
        parser = StandardArgumentParser.create_base_parser(description)
        
        assert isinstance(parser, argparse.ArgumentParser)
        assert parser.description == description
        
        # Test that standard arguments are present
        args = parser.parse_args(['--credentials', 'env', '--save-report', '--verbose'])
        assert args.credentials == 'env'
        assert args.save_report is True
        assert args.verbose is True
    
    def test_base_parser_defaults(self):
        """Test default values for base parser arguments."""
        parser = StandardArgumentParser.create_base_parser("Test")
        args = parser.parse_args([])
        
        assert args.credentials == 'aws'
        assert args.save_report is False
        assert args.verbose is False
    
    def test_credentials_choices(self):
        """Test that credentials argument only accepts valid choices."""
        parser = StandardArgumentParser.create_base_parser("Test")
        
        # Valid choices should work
        args = parser.parse_args(['--credentials', 'aws'])
        assert args.credentials == 'aws'
        
        args = parser.parse_args(['--credentials', 'env'])
        assert args.credentials == 'env'
        
        # Invalid choice should raise error
        with pytest.raises(SystemExit):
            parser.parse_args(['--credentials', 'invalid'])
    
    def test_add_execution_args(self):
        """Test adding execution-related arguments."""
        parser = StandardArgumentParser.create_base_parser("Test")
        parser = StandardArgumentParser.add_execution_args(parser)
        
        # Test execution arguments
        args = parser.parse_args(['--execute', '--batch-size', '500'])
        assert args.execute is True
        assert args.batch_size == 500
        
        # Test defaults
        args = parser.parse_args([])
        assert args.execute is False
        assert args.batch_size == 1000
    
    def test_batch_size_type_validation(self):
        """Test that batch-size only accepts integers."""
        parser = StandardArgumentParser.create_base_parser("Test")
        parser = StandardArgumentParser.add_execution_args(parser)
        
        # Valid integer should work
        args = parser.parse_args(['--batch-size', '2000'])
        assert args.batch_size == 2000
        
        # Invalid type should raise error
        with pytest.raises(SystemExit):
            parser.parse_args(['--batch-size', 'not-a-number'])
    
    def test_add_aws_arguments(self):
        """Test adding AWS-related arguments."""
        parser = StandardArgumentParser.create_base_parser("Test")
        parser = StandardArgumentParser.add_aws_arguments(parser)
        
        # Test AWS arguments
        args = parser.parse_args(['--aws-profile', 'test-profile', '--aws-region', 'eu-west-1'])
        assert args.aws_profile == 'test-profile'
        assert args.aws_region == 'eu-west-1'
        
        # Test defaults
        args = parser.parse_args([])
        assert args.aws_profile is None
        assert args.aws_region == 'us-east-2'
    
    def test_add_clickhouse_arguments(self):
        """Test adding ClickHouse-related arguments."""
        parser = StandardArgumentParser.create_base_parser("Test")
        parser = StandardArgumentParser.add_clickhouse_arguments(parser)
        
        # Test ClickHouse arguments
        args = parser.parse_args(['--clickhouse-secret', 'custom-secret', '--table-prefix', 'test_'])
        assert args.clickhouse_secret == 'custom-secret'
        assert args.table_prefix == 'test_'
        
        # Test defaults
        args = parser.parse_args([])
        assert args.clickhouse_secret == 'clickhouse-credentials'
        assert args.table_prefix is None
    
    def test_create_investigation_parser(self):
        """Test creation of investigation parser with combined arguments."""
        description = "Investigation script"
        parser = StandardArgumentParser.create_investigation_parser(description)
        
        # Test that all argument types are present
        args = parser.parse_args([
            '--credentials', 'env',
            '--save-report',
            '--verbose',
            '--aws-profile', 'test-profile',
            '--aws-region', 'us-west-1',
            '--clickhouse-secret', 'test-secret',
            '--table-prefix', 'inv_'
        ])
        
        # Base arguments
        assert args.credentials == 'env'
        assert args.save_report is True
        assert args.verbose is True
        
        # AWS arguments
        assert args.aws_profile == 'test-profile'
        assert args.aws_region == 'us-west-1'
        
        # ClickHouse arguments
        assert args.clickhouse_secret == 'test-secret'
        assert args.table_prefix == 'inv_'
    
    def test_create_cleanup_parser(self):
        """Test creation of cleanup parser with all argument types."""
        description = "Cleanup script"
        parser = StandardArgumentParser.create_cleanup_parser(description)
        
        # Test that all argument types are present including execution
        args = parser.parse_args([
            '--credentials', 'aws',
            '--save-report',
            '--verbose',
            '--execute',
            '--batch-size', '250',
            '--aws-profile', 'cleanup-profile',
            '--aws-region', 'ap-southeast-1',
            '--clickhouse-secret', 'cleanup-secret',
            '--table-prefix', 'clean_'
        ])
        
        # Base arguments
        assert args.credentials == 'aws'
        assert args.save_report is True
        assert args.verbose is True
        
        # Execution arguments
        assert args.execute is True
        assert args.batch_size == 250
        
        # AWS arguments
        assert args.aws_profile == 'cleanup-profile'
        assert args.aws_region == 'ap-southeast-1'
        
        # ClickHouse arguments
        assert args.clickhouse_secret == 'cleanup-secret'
        assert args.table_prefix == 'clean_'
    
    def test_cleanup_parser_defaults(self):
        """Test default values for cleanup parser."""
        parser = StandardArgumentParser.create_cleanup_parser("Test cleanup")
        args = parser.parse_args([])
        
        # Base defaults
        assert args.credentials == 'aws'
        assert args.save_report is False
        assert args.verbose is False
        
        # Execution defaults
        assert args.execute is False
        assert args.batch_size == 1000
        
        # AWS defaults
        assert args.aws_profile is None
        assert args.aws_region == 'us-east-2'
        
        # ClickHouse defaults
        assert args.clickhouse_secret == 'clickhouse-credentials'
        assert args.table_prefix is None
    
    def test_parser_help_functionality(self):
        """Test that parsers provide help functionality."""
        parser = StandardArgumentParser.create_base_parser("Test script")
        
        # Help should be available and exit cleanly
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(['--help'])
        
        # Help should exit with code 0
        assert exc_info.value.code == 0
    
    def test_argument_conflicts(self):
        """Test that there are no argument conflicts in combined parsers."""
        # This test ensures that combining different argument sets doesn't create conflicts
        parser = StandardArgumentParser.create_cleanup_parser("Test all arguments")
        
        # Should be able to parse all arguments without conflicts
        all_args = [
            '--credentials', 'env',
            '--save-report',
            '--verbose',
            '--execute',
            '--batch-size', '100',
            '--aws-profile', 'test',
            '--aws-region', 'us-west-2',
            '--clickhouse-secret', 'secret',
            '--table-prefix', 'test_'
        ]
        
        # This should not raise any exceptions
        args = parser.parse_args(all_args)
        assert args is not None
    
    def test_formatter_class(self):
        """Test that the correct formatter class is used."""
        parser = StandardArgumentParser.create_base_parser("Test")
        assert parser._get_formatter().__class__.__name__ == 'RawDescriptionHelpFormatter'
    
    def test_description_preservation(self):
        """Test that custom descriptions are preserved in specialized parsers."""
        custom_description = "Custom investigation script description"
        parser = StandardArgumentParser.create_investigation_parser(custom_description)
        assert parser.description == custom_description
        
        custom_cleanup_description = "Custom cleanup script description"
        cleanup_parser = StandardArgumentParser.create_cleanup_parser(custom_cleanup_description)
        assert cleanup_parser.description == custom_cleanup_description