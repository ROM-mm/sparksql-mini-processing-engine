"""
Module for processing SQL files (removing CREATE VIEW, processing templates, etc.).
"""
import re
import logging
from pathlib import Path
from typing import Dict, Optional
from src.modules.sql_template_processor import SQLTemplateProcessor
from src.modules.sql_file_utils import SQLFileUtils

logger = logging.getLogger(__name__)


class SQLFileProcessor:
    """Processes SQL files for execution."""
    
    def __init__(self, logger_instance: Optional[logging.Logger] = None):
        """
        Initialize SQLFileProcessor.
        
        Args:
            logger_instance: Logger instance (optional, uses module logger if not provided)
        """
        self.log = logger_instance or logger
        self.template_processor = SQLTemplateProcessor()
    
    def process_sql_file(
        self,
        sql_path: Path,
        sql_content: str,
        ref_map: Dict[str, str],
        project_root: Optional[Path] = None
    ) -> str:
        """
        Process SQL file: remove CREATE VIEW, add CREATE MATERIALIZED VIEW, process templates.
        
        Args:
            sql_path: Path to SQL file
            sql_content: Original SQL content
            ref_map: Mapping of ref names to view names
            project_root: Project root directory (optional)
            
        Returns:
            Processed SQL content
        """
        # Check if this is an export file
        is_exporting = SQLFileUtils.is_export_file(sql_path, project_root)
        
        # Remove CREATE VIEW and wrap with CREATE MATERIALIZED VIEW
        sql_content = self._remove_create_view_and_wrap_sql(
            sql_content,
            sql_path,
            is_exporting,
            project_root
        )
        
        # Process templates (dbt style)
        refs = self.template_processor.extract_refs(sql_content)
        
        if refs:
            self.log.info("\nProcessing dbt-style templates...")
            self.log.info(f"   Found {len(refs)} reference(s): {refs}")
            self.log.info(f"   ref_map provided: {bool(ref_map)}")
            if ref_map:
                self.log.info(f"   ref_map contents: {ref_map}")
            
            # If ref_map not provided, try to create a basic one
            if not ref_map:
                self.log.warning("   ref_map not provided, attempting to use ref names as view names")
                ref_map = {ref_name: ref_name for ref_name in refs}
                self.log.info(f"   Created basic ref_map: {ref_map}")
            
            for ref_name in refs:
                view_name = ref_map.get(ref_name, ref_name)
                self.log.info(f"   Mapping: {{ ref('{ref_name}') }} -> '{view_name}'")
            
            # Compile template: replace {{ ref('name') }} with view name
            sql_content = self.template_processor.compile_template(
                sql_content,
                ref_map=ref_map
            )
            self.log.info("Template compiled successfully")
            
            # Verify all templates were replaced
            remaining_refs = self.template_processor.extract_refs(sql_content)
            if remaining_refs:
                self.log.error(f"   ERROR: Templates not replaced: {remaining_refs}")
                self.log.error("   This indicates a problem with template processing!")
        else:
            self.log.debug("   No {{ ref() }} templates found in SQL")
        
        return sql_content
    
    def _remove_create_view_and_wrap_sql(
        self,
        sql_content: str,
        sql_path: Path,
        is_exporting: bool,
        project_root: Optional[Path] = None
    ) -> str:
        """
        Remove any CREATE VIEW statement from SQL and add automatically
        ensuring that the view name is always the file name.
        
        Uses TEMPORARY VIEW for intermediate transformations (transformation folder)
        and MATERIALIZED VIEW only for final exports (export folder).
        
        Args:
            sql_content: Original SQL content
            sql_path: Path to SQL file
            is_exporting: If True, is a final export view; if False, is intermediate view
            project_root: Project root directory (optional)
            
        Returns:
            SQL with CREATE VIEW added automatically using file name
        """
        # Check if is source file
        is_source = SQLFileUtils.is_source_file(sql_path, project_root)
        
        # Get view name (file name without extension)
        view_name = sql_path.stem
        
        # Determine view type based on file location
        # Note: We use MATERIALIZED VIEW for all views because each Airflow task
        # has its own SparkSession, so TEMPORARY VIEWs wouldn't work across tasks.
        # Intermediate views will be cleaned up after processing completes.
        if is_exporting:
            # Export files: use MATERIALIZED VIEW (persisted for export)
            view_type = "MATERIALIZED VIEW"
            drop_statement = f"DROP TABLE IF EXISTS {view_name};\n"
            self.log.info(f"   Detected export file - will create MATERIALIZED VIEW '{view_name}' (persisted)")
        elif is_source:
            # Source files: use MATERIALIZED VIEW (needed for other views to reference)
            view_type = "MATERIALIZED VIEW"
            drop_statement = f"DROP TABLE IF EXISTS {view_name};\n"
            self.log.info(f"   Detected source file - will create MATERIALIZED VIEW '{view_name}' (needed for references)")
        else:
            # Transformation files: use MATERIALIZED VIEW (will be cleaned up after processing)
            # We can't use TEMPORARY VIEW because each Airflow task has its own SparkSession
            view_type = "MATERIALIZED VIEW"
            drop_statement = f"DROP TABLE IF EXISTS {view_name};\n"
            self.log.info(f"   Detected transformation file - will create MATERIALIZED VIEW '{view_name}' (will be cleaned up after processing)")
        
        # Detect and remove any existing CREATE VIEW
        # Supports:
        # - Optional TEMPORARY and MATERIALIZED
        # - Optional IF NOT EXISTS
        # - View names with dots, dashes, underscores ([\w.-]+) or backticks (`[^`]+`)
        # - Optional USING and LOCATION
        create_view_pattern = re.compile(
            r'CREATE\s+(?:TEMPORARY\s+)?(?:MATERIALIZED\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[\w.-]+|`[^`]+`)\s*(?:USING\s+\w+\s*)?(?:LOCATION\s+[\'"][^\'"]+[\'"]\s*)?AS\s*',
            re.IGNORECASE | re.DOTALL
        )
        
        create_view_using_pattern = re.compile(
            r'CREATE\s+(?:TEMPORARY\s+)?(?:MATERIALIZED\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[\w.-]+|`[^`]+`)\s*(?:USING\s+\w+\s*)?(?:OPTIONS\s*\([^)]+\)\s*)?;',
            re.IGNORECASE | re.DOTALL
        )
        
        has_create_view = create_view_pattern.search(sql_content) or create_view_using_pattern.search(sql_content)
        
        if has_create_view:
            self.log.info(f"   Removing existing CREATE VIEW statement from SQL...")
            sql_content = create_view_pattern.sub('', sql_content)
            sql_content = create_view_using_pattern.sub('', sql_content)
            sql_content = re.sub(r'\n\s*\n\s*\n', '\n\n', sql_content)
            sql_content = sql_content.strip()
            self.log.info(f"   CREATE VIEW statement removed")
        
        # Check for USING, OPTIONS, LOCATION in SQL
        using_match = re.search(r'USING\s+(\w+)', sql_content, re.IGNORECASE)
        options_match = re.search(r'OPTIONS\s*\([^)]+\)', sql_content, re.IGNORECASE | re.DOTALL)
        location_match = re.search(r"LOCATION\s+['\"]([^'\"]+)['\"]", sql_content, re.IGNORECASE)
        
        # Build CREATE VIEW statement (TEMPORARY or MATERIALIZED based on file type)
        create_statement = f"CREATE {view_type} {view_name}"
        
        # If has USING and OPTIONS (special case for ingestion with specific format)
        if using_match and options_match:
            using_format = using_match.group(1)
            options = options_match.group(0)
            create_statement += f"\nUSING {using_format}\n{options}\n"
            sql_content = re.sub(r'USING\s+\w+\s*', '', sql_content, flags=re.IGNORECASE)
            sql_content = re.sub(r'OPTIONS\s*\([^)]+\)\s*', '', sql_content, flags=re.IGNORECASE | re.DOTALL)
            sql_content = sql_content.strip()
            if not sql_content or sql_content == ';':
                final_sql = drop_statement + create_statement.rstrip() + ';'
                return final_sql
            else:
                create_statement += "AS\n"
        else:
            # Default case: CREATE MATERIALIZED VIEW ... AS SELECT ...
            if using_match:
                using_format = using_match.group(1)
                create_statement += f" USING {using_format}"
                sql_content = re.sub(r'USING\s+\w+\s*', '', sql_content, flags=re.IGNORECASE)
            
            if location_match:
                location = location_match.group(1)
                create_statement += f" LOCATION '{location}'"
                sql_content = re.sub(r"LOCATION\s+['\"][^'\"]+['\"]\s*", '', sql_content, flags=re.IGNORECASE)
            
            create_statement += " AS\n"
        
        # Add DROP and CREATE at the beginning of SQL
        final_sql = drop_statement + create_statement + sql_content
        
        self.log.info(f"   Added DROP and '{view_type}' statement with name '{view_name}' (from file name)")
        
        return final_sql

