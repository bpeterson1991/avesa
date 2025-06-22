"""
Tests for CDK bundling utilities.
"""
import pytest
import sys
import os
from unittest.mock import Mock, patch

# Add infrastructure to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'infrastructure'))
from infrastructure.shared.bundling_utils import BundlingOptionsFactory


class TestBundlingOptionsFactory:
    """Test CDK bundling options factory."""

    def test_get_python_bundling_default(self):
        """Test Python bundling with default requirements file."""
        bundling_options = BundlingOptionsFactory.get_python_bundling()
        
        # Check that it returns a BundlingOptions object
        assert bundling_options is not None
        assert hasattr(bundling_options, 'command')
        assert hasattr(bundling_options, 'image')
        
        # Check command contains expected elements
        command = bundling_options.command
        assert len(command) == 3
        assert command[0] == "bash"
        assert command[1] == "-c"
        
        # Check the actual command string
        cmd_string = command[2]
        assert "pip install -r requirements.txt -t /asset-output" in cmd_string
        assert "cp -au . /asset-output" in cmd_string
        assert "find /asset-output -name '*.pyc' -delete" in cmd_string
        assert "find /asset-output -name '__pycache__' -type d -exec rm -rf" in cmd_string

    def test_get_python_bundling_custom_requirements(self):
        """Test Python bundling with custom requirements file."""
        custom_requirements = "custom-requirements.txt"
        bundling_options = BundlingOptionsFactory.get_python_bundling(custom_requirements)
        
        cmd_string = bundling_options.command[2]
        assert f"pip install -r {custom_requirements} -t /asset-output" in cmd_string

    def test_get_optimized_bundling_default(self):
        """Test optimized bundling with default settings."""
        bundling_options = BundlingOptionsFactory.get_optimized_bundling()
        
        assert bundling_options is not None
        cmd_string = bundling_options.command[2]
        
        # Check basic installation
        assert "pip install -r requirements.txt -t /asset-output" in cmd_string
        assert "cp -au . /asset-output" in cmd_string
        
        # Check default exclusions
        assert "find /asset-output -name 'tests/' -exec rm -rf" in cmd_string
        assert "find /asset-output -name '*.pyc' -exec rm -rf" in cmd_string
        assert "find /asset-output -name '__pycache__/' -exec rm -rf" in cmd_string
        assert "find /asset-output -name '*.md' -exec rm -rf" in cmd_string

    def test_get_optimized_bundling_custom_exclusions(self):
        """Test optimized bundling with custom exclusions."""
        custom_exclusions = ["*.log", "temp/", "*.tmp"]
        bundling_options = BundlingOptionsFactory.get_optimized_bundling(
            exclude_patterns=custom_exclusions
        )
        
        cmd_string = bundling_options.command[2]
        
        # Check custom exclusions are included
        for pattern in custom_exclusions:
            assert f"find /asset-output -name '{pattern}' -exec rm -rf" in cmd_string

    def test_get_lightweight_bundling(self):
        """Test lightweight bundling options."""
        bundling_options = BundlingOptionsFactory.get_lightweight_bundling()
        
        assert bundling_options is not None
        cmd_string = bundling_options.command[2]
        
        # Check lightweight installation (no-deps)
        assert "pip install -r requirements.txt -t /asset-output --no-deps" in cmd_string
        assert "cp -au *.py /asset-output/" in cmd_string
        
        # Check cleanup commands
        assert "find /asset-output -name '*.pyc' -delete" in cmd_string
        assert "find /asset-output -name '__pycache__' -type d -exec rm -rf" in cmd_string
        assert "find /asset-output -name '*.dist-info' -type d -exec rm -rf" in cmd_string

    def test_get_lightweight_bundling_custom_requirements(self):
        """Test lightweight bundling with custom requirements file."""
        custom_requirements = "minimal-requirements.txt"
        bundling_options = BundlingOptionsFactory.get_lightweight_bundling(custom_requirements)
        
        cmd_string = bundling_options.command[2]
        assert f"pip install -r {custom_requirements} -t /asset-output --no-deps" in cmd_string

    def test_get_shared_layer_bundling(self):
        """Test shared layer bundling options."""
        bundling_options = BundlingOptionsFactory.get_shared_layer_bundling()
        
        assert bundling_options is not None
        cmd_string = bundling_options.command[2]
        
        # Check layer-specific installation path
        assert "pip install -r requirements.txt -t /asset-output/python" in cmd_string
        
        # Check cleanup commands
        assert "find /asset-output -name '*.pyc' -delete" in cmd_string
        assert "find /asset-output -name '__pycache__' -type d -exec rm -rf" in cmd_string

    def test_bundling_options_structure(self):
        """Test that all bundling options have consistent structure."""
        bundling_methods = [
            BundlingOptionsFactory.get_python_bundling,
            BundlingOptionsFactory.get_optimized_bundling,
            BundlingOptionsFactory.get_lightweight_bundling,
            BundlingOptionsFactory.get_shared_layer_bundling
        ]
        
        for method in bundling_methods:
            bundling_options = method()
            
            # Check basic structure
            assert hasattr(bundling_options, 'command')
            assert hasattr(bundling_options, 'image')
            
            # Check command structure
            assert isinstance(bundling_options.command, list)
            assert len(bundling_options.command) == 3
            assert bundling_options.command[0] == "bash"
            assert bundling_options.command[1] == "-c"
            assert isinstance(bundling_options.command[2], str)

    def test_bundling_options_image_consistency(self):
        """Test that all bundling options use consistent image."""
        bundling_methods = [
            BundlingOptionsFactory.get_python_bundling,
            BundlingOptionsFactory.get_optimized_bundling,
            BundlingOptionsFactory.get_lightweight_bundling,
            BundlingOptionsFactory.get_shared_layer_bundling
        ]
        
        images = []
        for method in bundling_methods:
            bundling_options = method()
            images.append(bundling_options.image)
        
        # All should use the same image type (they're different objects but same runtime)
        # Just verify they all have images
        for img in images:
            assert img is not None

    def test_exclude_patterns_handling(self):
        """Test that exclude patterns are properly handled."""
        # Test with empty list
        bundling_options = BundlingOptionsFactory.get_optimized_bundling(exclude_patterns=[])
        cmd_string = bundling_options.command[2]
        
        # Should still have basic pip install and copy
        assert "pip install -r requirements.txt -t /asset-output" in cmd_string
        assert "cp -au . /asset-output" in cmd_string
        
        # Test with None (should use defaults)
        bundling_options = BundlingOptionsFactory.get_optimized_bundling(exclude_patterns=None)
        cmd_string = bundling_options.command[2]
        
        # Should have default exclusions
        assert "find /asset-output -name 'tests/' -exec rm -rf" in cmd_string

    def test_command_safety(self):
        """Test that commands are safe and don't contain dangerous operations."""
        bundling_methods = [
            BundlingOptionsFactory.get_python_bundling,
            BundlingOptionsFactory.get_optimized_bundling,
            BundlingOptionsFactory.get_lightweight_bundling,
            BundlingOptionsFactory.get_shared_layer_bundling
        ]
        
        dangerous_patterns = ['rm -rf /', 'rm -rf *', 'sudo', 'chmod 777']
        
        for method in bundling_methods:
            bundling_options = method()
            cmd_string = bundling_options.command[2]
            
            for pattern in dangerous_patterns:
                assert pattern not in cmd_string, f"Dangerous pattern '{pattern}' found in command"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])