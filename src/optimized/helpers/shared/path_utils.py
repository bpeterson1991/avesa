"""
Standardized path manipulation utilities for AVESA project.
Eliminates duplicate sys.path manipulation across 28+ files.
"""
import os
import sys
from pathlib import Path
from typing import Optional, List

class PathManager:
    """Centralized path management for consistent module imports."""
    
    @staticmethod
    def setup_src_path(script_file: str) -> None:
        """
        Standardized src path setup for scripts and Lambda functions.
        
        Args:
            script_file: __file__ from the calling script
        """
        project_root = PathManager.get_project_root(script_file)
        src_path = project_root / 'src'
        
        if str(src_path) not in sys.path:
            sys.path.insert(0, str(src_path))
    
    @staticmethod
    def get_project_root(current_file: str) -> Path:
        """
        Get project root from any file location.
        
        Args:
            current_file: __file__ from the calling script
            
        Returns:
            Path to project root directory
        """
        current_path = Path(current_file).resolve()
        
        # Look for project markers
        for parent in current_path.parents:
            if (parent / 'infrastructure').exists() and (parent / 'src').exists():
                return parent
        
        # Fallback: assume we're in a subdirectory of project root
        return current_path.parent.parent
    
    @staticmethod
    def setup_test_paths(test_file: str) -> None:
        """
        Standardized test path setup.
        
        Args:
            test_file: __file__ from the calling test
        """
        project_root = PathManager.get_project_root(test_file)
        paths_to_add = [
            project_root / 'src',
            project_root / 'tests'
        ]
        
        for path in paths_to_add:
            if str(path) not in sys.path:
                sys.path.insert(0, str(path))
    
    @staticmethod
    def setup_lambda_paths() -> None:
        """Standardized Lambda path setup for AWS environment."""
        lambda_paths = [
            '/opt/python',
            '/opt/python/lib/python3.9/site-packages',
            '/var/task'
        ]
        
        for path in lambda_paths:
            if os.path.exists(path) and path not in sys.path:
                sys.path.insert(0, path)