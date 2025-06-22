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
        Standard Python Lambda bundling options with fallback handling.
        
        Args:
            requirements_file: Name of requirements file to install
            
        Returns:
            BundlingOptions configured for Python Lambda functions
        """
        return BundlingOptions(
            image=_lambda.Runtime.PYTHON_3_9.bundling_image,
            command=[
                "bash", "-c",
                f"if [ -f {requirements_file} ]; then "
                f"pip install -r {requirements_file} -t /asset-output; "
                "else "
                "echo 'No requirements.txt found, skipping pip install'; "
                "fi && "
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
                f"if [ -f {requirements_file} ]; then "
                f"pip install -r {requirements_file} -t /asset-output; "
                "else "
                "echo 'No requirements.txt found, skipping pip install'; "
                "fi && "
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
                f"if [ -f {requirements_file} ]; then "
                f"pip install -r {requirements_file} -t /asset-output --no-deps; "
                "else "
                "echo 'No requirements.txt found, skipping pip install'; "
                "fi && "
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
    
    @staticmethod
    def get_optimized_shared_bundling(requirements_file: str = "requirements.txt") -> BundlingOptions:
        """
        Optimized bundling that includes the root shared directory for optimized lambda functions.
        This eliminates the need for duplicated shared files in each lambda directory.
        
        Args:
            requirements_file: Name of requirements file to install
            
        Returns:
            BundlingOptions configured to include shared modules from root
        """
        return BundlingOptions(
            image=_lambda.Runtime.PYTHON_3_9.bundling_image,
            command=[
                "bash", "-c",
                # Install requirements if they exist
                f"if [ -f {requirements_file} ]; then "
                f"pip install -r {requirements_file} -t /asset-output; "
                "else "
                "echo 'No requirements.txt found, skipping pip install'; "
                "fi && "
                # Copy the lambda function code
                "cp -au . /asset-output && "
                # Copy the shared directory from the root src directory
                "if [ -d ../../shared ]; then "
                "cp -au ../../shared /asset-output/; "
                "echo 'Copied shared directory from ../../shared'; "
                "else "
                "echo 'Warning: shared directory not found at ../../shared'; "
                "fi && "
                # Remove any existing duplicated shared directories
                "rm -rf /asset-output/shared/shared 2>/dev/null || true && "
                # Clean up unnecessary files
                "find /asset-output -name '*.pyc' -delete && "
                "find /asset-output -name '__pycache__' -type d -exec rm -rf {{}} + || true && "
                "find /asset-output -name '*.md' -delete || true && "
                "find /asset-output -name '.git*' -exec rm -rf {{}} + || true && "
                "find /asset-output -name 'tests' -type d -exec rm -rf {{}} + || true"
            ]
        )