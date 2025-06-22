"""
Standardized CDK bundling utilities for Lambda functions.
Eliminates duplicate bundling configuration across CDK stacks.
"""
from aws_cdk import BundlingOptions
import aws_cdk.aws_lambda as _lambda
from typing import List, Optional

class BundlingOptionsFactory:
    """Factory for creating standardized CDK bundling options."""
    
    @staticmethod
    def get_python_bundling(requirements_file: str = "requirements.txt") -> BundlingOptions:
        """
        Standard Python Lambda bundling options.
        
        Args:
            requirements_file: Name of requirements file to install
            
        Returns:
            BundlingOptions configured for Python Lambda functions
        """
        return BundlingOptions(
            image=_lambda.Runtime.PYTHON_3_9.bundling_image,
            command=[
                "bash", "-c",
                f"pip install -r {requirements_file} -t /asset-output && "
                "cp -au . /asset-output && "
                "find /asset-output -name '*.pyc' -delete && "
                "find /asset-output -name '__pycache__' -type d -exec rm -rf {{}} + || true"
            ]
        )
    
    @staticmethod
    def get_optimized_bundling(
        requirements_file: str = "requirements.txt",
        exclude_patterns: Optional[List[str]] = None
    ) -> BundlingOptions:
        """
        Optimized bundling with size reduction and exclusions.
        
        Args:
            requirements_file: Name of requirements file to install
            exclude_patterns: List of patterns to exclude from bundle
            
        Returns:
            BundlingOptions configured for optimized Lambda functions
        """
        exclude_patterns = exclude_patterns or [
            "tests/", "*.pyc", "__pycache__/", "*.md", ".git/",
            "*.log", "*.tmp", ".pytest_cache/", ".coverage"
        ]
        
        exclude_commands = []
        for pattern in exclude_patterns:
            exclude_commands.append(f"find /asset-output -name '{pattern}' -exec rm -rf {{}} + || true")
        
        exclude_cmd = " && ".join(exclude_commands)
        
        return BundlingOptions(
            image=_lambda.Runtime.PYTHON_3_9.bundling_image,
            command=[
                "bash", "-c",
                f"pip install -r {requirements_file} -t /asset-output && "
                "cp -au . /asset-output && "
                f"{exclude_cmd}"
            ]
        )
    
    @staticmethod
    def get_lightweight_bundling(requirements_file: str = "requirements.txt") -> BundlingOptions:
        """
        Lightweight bundling for minimal Lambda functions.
        
        Args:
            requirements_file: Name of requirements file to install
            
        Returns:
            BundlingOptions configured for lightweight Lambda functions
        """
        return BundlingOptions(
            image=_lambda.Runtime.PYTHON_3_9.bundling_image,
            command=[
                "bash", "-c",
                f"pip install -r {requirements_file} -t /asset-output --no-deps && "
                "cp -au *.py /asset-output/ && "
                "find /asset-output -name '*.pyc' -delete && "
                "find /asset-output -name '__pycache__' -type d -exec rm -rf {{}} + || true && "
                "find /asset-output -name '*.dist-info' -type d -exec rm -rf {{}} + || true"
            ]
        )
    
    @staticmethod
    def get_shared_layer_bundling() -> BundlingOptions:
        """
        Bundling options for shared Lambda layers.
        
        Returns:
            BundlingOptions configured for Lambda layers
        """
        return BundlingOptions(
            image=_lambda.Runtime.PYTHON_3_9.bundling_image,
            command=[
                "bash", "-c",
                "pip install -r requirements.txt -t /asset-output/python && "
                "find /asset-output -name '*.pyc' -delete && "
                "find /asset-output -name '__pycache__' -type d -exec rm -rf {{}} + || true"
            ]
        )