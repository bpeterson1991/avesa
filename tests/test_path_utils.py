"""
Tests for path utilities module.
"""
import pytest
import sys
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

from shared.path_utils import PathManager


class TestPathManager:
    """Test cases for PathManager class."""
    
    def test_get_project_root_with_markers(self, tmp_path):
        """Test project root detection with infrastructure and src markers."""
        # Create a mock project structure
        project_root = tmp_path / "project"
        project_root.mkdir()
        (project_root / "infrastructure").mkdir()
        (project_root / "src").mkdir()
        
        # Create a nested file
        nested_dir = project_root / "src" / "shared"
        nested_dir.mkdir(parents=True)
        test_file = nested_dir / "test.py"
        test_file.write_text("# test file")
        
        # Test project root detection
        detected_root = PathManager.get_project_root(str(test_file))
        assert detected_root == project_root
    
    def test_get_project_root_fallback(self, tmp_path):
        """Test project root fallback when markers not found."""
        # Create a simple file structure without markers
        test_file = tmp_path / "subdir" / "test.py"
        test_file.parent.mkdir(parents=True)
        test_file.write_text("# test file")
        
        # Test fallback behavior
        detected_root = PathManager.get_project_root(str(test_file))
        assert detected_root == tmp_path  # Should be parent.parent
    
    def test_setup_src_path(self, tmp_path):
        """Test src path setup functionality."""
        # Create mock project structure
        project_root = tmp_path / "project"
        project_root.mkdir()
        (project_root / "infrastructure").mkdir()
        src_dir = project_root / "src"
        src_dir.mkdir()
        
        test_file = project_root / "scripts" / "test.py"
        test_file.parent.mkdir(parents=True)
        test_file.write_text("# test file")
        
        # Store original sys.path
        original_path = sys.path.copy()
        
        try:
            # Test setup_src_path
            PathManager.setup_src_path(str(test_file))
            
            # Verify src path was added
            assert str(src_dir) in sys.path
            assert sys.path.index(str(src_dir)) == 0  # Should be first
            
        finally:
            # Restore original sys.path
            sys.path[:] = original_path
    
    def test_setup_src_path_already_exists(self, tmp_path):
        """Test that setup_src_path doesn't duplicate existing paths."""
        # Create mock project structure
        project_root = tmp_path / "project"
        project_root.mkdir()
        (project_root / "infrastructure").mkdir()
        src_dir = project_root / "src"
        src_dir.mkdir()
        
        test_file = project_root / "scripts" / "test.py"
        test_file.parent.mkdir(parents=True)
        test_file.write_text("# test file")
        
        # Store original sys.path
        original_path = sys.path.copy()
        
        try:
            # Add src path manually first
            sys.path.insert(0, str(src_dir))
            original_count = sys.path.count(str(src_dir))
            
            # Test setup_src_path doesn't duplicate
            PathManager.setup_src_path(str(test_file))
            
            # Verify no duplication
            assert sys.path.count(str(src_dir)) == original_count
            
        finally:
            # Restore original sys.path
            sys.path[:] = original_path
    
    def test_setup_test_paths(self, tmp_path):
        """Test test path setup functionality."""
        # Create mock project structure
        project_root = tmp_path / "project"
        project_root.mkdir()
        (project_root / "infrastructure").mkdir()
        src_dir = project_root / "src"
        src_dir.mkdir()
        tests_dir = project_root / "tests"
        tests_dir.mkdir()
        
        test_file = tests_dir / "test_something.py"
        test_file.write_text("# test file")
        
        # Store original sys.path
        original_path = sys.path.copy()
        
        try:
            # Test setup_test_paths
            PathManager.setup_test_paths(str(test_file))
            
            # Verify both src and tests paths were added
            assert str(src_dir) in sys.path
            assert str(tests_dir) in sys.path
            
        finally:
            # Restore original sys.path
            sys.path[:] = original_path
    
    @patch('os.path.exists')
    def test_setup_lambda_paths(self, mock_exists):
        """Test Lambda path setup functionality."""
        # Mock that Lambda paths exist
        mock_exists.return_value = True
        
        # Store original sys.path
        original_path = sys.path.copy()
        
        try:
            # Test setup_lambda_paths
            PathManager.setup_lambda_paths()
            
            # Verify Lambda paths were added
            expected_paths = [
                '/opt/python',
                '/opt/python/lib/python3.9/site-packages',
                '/var/task'
            ]
            
            for path in expected_paths:
                assert path in sys.path
                
        finally:
            # Restore original sys.path
            sys.path[:] = original_path
    
    @patch('os.path.exists')
    def test_setup_lambda_paths_nonexistent(self, mock_exists):
        """Test Lambda path setup when paths don't exist."""
        # Mock that Lambda paths don't exist
        mock_exists.return_value = False
        
        # Store original sys.path
        original_path = sys.path.copy()
        
        try:
            # Test setup_lambda_paths
            PathManager.setup_lambda_paths()
            
            # Verify no Lambda paths were added
            lambda_paths = [
                '/opt/python',
                '/opt/python/lib/python3.9/site-packages',
                '/var/task'
            ]
            
            for path in lambda_paths:
                assert path not in sys.path
                
        finally:
            # Restore original sys.path
            sys.path[:] = original_path
    
    @patch('os.path.exists')
    def test_setup_lambda_paths_already_exists(self, mock_exists):
        """Test Lambda path setup doesn't duplicate existing paths."""
        # Mock that Lambda paths exist
        mock_exists.return_value = True
        
        # Store original sys.path
        original_path = sys.path.copy()
        
        try:
            # Add one Lambda path manually first
            test_path = '/opt/python'
            sys.path.insert(0, test_path)
            original_count = sys.path.count(test_path)
            
            # Test setup_lambda_paths
            PathManager.setup_lambda_paths()
            
            # Verify no duplication for the pre-existing path
            assert sys.path.count(test_path) == original_count
            
        finally:
            # Restore original sys.path
            sys.path[:] = original_path