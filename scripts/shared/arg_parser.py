"""
Standardized argument parsing utilities for AVESA scripts.
Eliminates duplicate argparse logic across multiple scripts.
"""
import argparse
from typing import Optional

class StandardArgumentParser:
    """Centralized argument parsing for AVESA scripts."""
    
    @staticmethod
    def create_base_parser(description: str) -> argparse.ArgumentParser:
        """
        Create parser with standard arguments.
        
        Args:
            description: Script description
            
        Returns:
            ArgumentParser with standard arguments
        """
        parser = argparse.ArgumentParser(
            description=description,
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        
        # Standard credential options
        parser.add_argument(
            '--credentials',
            choices=['aws', 'env'],
            default='aws',
            help='Credential source (default: aws)'
        )
        
        # Standard reporting options
        parser.add_argument(
            '--save-report',
            action='store_true',
            help='Save detailed report to JSON file'
        )
        
        # Standard verbosity options
        parser.add_argument(
            '-v', '--verbose',
            action='store_true',
            help='Enable verbose output'
        )
        
        return parser
    
    @staticmethod
    def add_execution_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        """
        Add execution-related arguments.
        
        Args:
            parser: ArgumentParser to enhance
            
        Returns:
            Enhanced ArgumentParser
        """
        parser.add_argument(
            '--execute',
            action='store_true',
            help='Execute actual operations (default is dry run)'
        )
        
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Batch size for operations (default: 1000)'
        )
        
        return parser
    
    @staticmethod
    def add_aws_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        """
        Add standard AWS arguments.
        
        Args:
            parser: ArgumentParser to enhance
            
        Returns:
            Enhanced ArgumentParser
        """
        parser.add_argument(
            '--aws-profile',
            help='AWS profile to use'
        )
        
        parser.add_argument(
            '--aws-region',
            default='us-east-2',
            help='AWS region (default: us-east-2)'
        )
        
        return parser
    
    @staticmethod
    def add_clickhouse_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        """
        Add standard ClickHouse arguments.
        
        Args:
            parser: ArgumentParser to enhance
            
        Returns:
            Enhanced ArgumentParser
        """
        parser.add_argument(
            '--clickhouse-secret',
            default='clickhouse-credentials',
            help='ClickHouse secret name (default: clickhouse-credentials)'
        )
        
        parser.add_argument(
            '--table-prefix',
            help='Table prefix for ClickHouse operations'
        )
        
        return parser
    
    @staticmethod
    def create_investigation_parser(description: str) -> argparse.ArgumentParser:
        """
        Create parser for investigation scripts.
        
        Args:
            description: Script description
            
        Returns:
            ArgumentParser configured for investigation scripts
        """
        parser = StandardArgumentParser.create_base_parser(description)
        parser = StandardArgumentParser.add_aws_arguments(parser)
        parser = StandardArgumentParser.add_clickhouse_arguments(parser)
        
        return parser
    
    @staticmethod
    def create_cleanup_parser(description: str) -> argparse.ArgumentParser:
        """
        Create parser for cleanup scripts.
        
        Args:
            description: Script description
            
        Returns:
            ArgumentParser configured for cleanup scripts
        """
        parser = StandardArgumentParser.create_investigation_parser(description)
        parser = StandardArgumentParser.add_execution_args(parser)
        
        return parser