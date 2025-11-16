"""
Utilities for identifying and working with SQL files.
"""
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class SQLFileUtils:
    """Utilities for SQL file operations."""
    
    @staticmethod
    def is_source_file(sql_path: Path, project_root: Optional[Path] = None) -> bool:
        """
        Check if SQL file is in the source folder.
        
        Args:
            sql_path: Path to SQL file
            project_root: Project root directory (optional, auto-detected if not provided)
            
        Returns:
            True if file is in source folder, False otherwise
        """
        try:
            # Get absolute path
            sql_path_abs = sql_path.resolve()
            
            # Get relative path from src/sql
            if project_root is None:
                # Try to detect project root from common locations
                current = sql_path_abs
                for _ in range(10):  # Max 10 levels up
                    if (current / "src" / "sql").exists():
                        project_root = current
                        break
                    if current.parent == current:
                        break
                    current = current.parent
            
            if project_root is None:
                # Fallback: check by file name
                file_name = sql_path.stem.lower()
                return 'source' in file_name
            
            sql_base_path = (project_root / "src" / "sql").resolve()
            
            # Check if path is within src/sql
            try:
                relative_path = sql_path_abs.relative_to(sql_base_path)
                
                # Check if first part of path is 'source'
                if len(relative_path.parts) > 0:
                    return relative_path.parts[0] == 'source'
                return False
            except ValueError:
                # Path is not relative to src/sql, check by file name
                file_name = sql_path.stem.lower()
                return 'source' in file_name
        except (ValueError, AttributeError, Exception) as e:
            # If can't determine, check by file name
            file_name = sql_path.stem.lower()
            logger.debug(f"Error determining if source file ({e}), checking filename: {file_name}")
            return 'source' in file_name
    
    @staticmethod
    def is_export_file(sql_path: Path, project_root: Optional[Path] = None) -> bool:
        """
        Check if SQL file is in the export folder.
        
        Args:
            sql_path: Path to SQL file
            project_root: Project root directory (optional, auto-detected if not provided)
            
        Returns:
            True if file is in export folder, False otherwise
        """
        try:
            # Get absolute path
            sql_path_abs = sql_path.resolve()
            
            # Get relative path from src/sql
            if project_root is None:
                # Try to detect project root from common locations
                current = sql_path_abs
                for _ in range(10):  # Max 10 levels up
                    if (current / "src" / "sql").exists():
                        project_root = current
                        break
                    if current.parent == current:
                        break
                    current = current.parent
            
            if project_root is None:
                # Fallback: check by file name
                file_name = sql_path.stem.lower()
                return 'export' in file_name or 'final' in file_name
            
            sql_base_path = (project_root / "src" / "sql").resolve()
            
            # Check if path is within src/sql
            try:
                relative_path = sql_path_abs.relative_to(sql_base_path)
                
                # Check if first part of path is 'export'
                if len(relative_path.parts) > 0:
                    return relative_path.parts[0] == 'export'
                return False
            except ValueError:
                # Path is not relative to src/sql, check by file name
                file_name = sql_path.stem.lower()
                return 'export' in file_name or 'final' in file_name
        except (ValueError, AttributeError, Exception) as e:
            # If can't determine, check by file name
            file_name = sql_path.stem.lower()
            logger.debug(f"Error determining if export file ({e}), checking filename: {file_name}")
            return 'export' in file_name or 'final' in file_name

