"""
Module for executing source Python modules to create MATERIALIZED VIEWs.
"""
import sys
import importlib
import importlib.util
import time
import logging
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class SourceModuleExecutor:
    """Executes source Python modules to create MATERIALIZED VIEWs."""
    
    def __init__(self, logger_instance: Optional[logging.Logger] = None):
        """
        Initialize SourceModuleExecutor.
        
        Args:
            logger_instance: Logger instance (optional, uses module logger if not provided)
        """
        self.log = logger_instance or logger
    
    def execute_source_module(self, spark: SparkSession, sql_path: Path, project_root: Optional[Path] = None) -> None:
        """
        Execute Python module to create MATERIALIZED VIEW for source files.
        
        For source files, the view is created by a Python module using the centralized
        materialized_view_utils module. This method imports and executes the module to ensure
        the view is created in the catalog before the SQL file tries to reference it.
        
        Args:
            spark: SparkSession instance
            sql_path: Path to SQL file (used to determine which module to execute)
            project_root: Project root directory (optional, auto-detected if not provided)
        """
        try:
            # Determine project root if not provided
            if project_root is None:
                # Try to detect from sql_path
                current = sql_path.resolve()
                for _ in range(10):  # Max 10 levels up
                    if (current / "src" / "modules").exists():
                        project_root = current
                        break
                    if current.parent == current:
                        break
                    current = current.parent
            
            if project_root is None:
                raise ValueError("Could not determine project root directory")
            
            modules_path = project_root / "src" / "modules"
            source_module_path = modules_path / "source.py"
            
            if not source_module_path.exists():
                raise FileNotFoundError(
                    f"Source module not found: {source_module_path}. "
                    f"Expected module at src/modules/source.py"
                )
            
            self.log.info(f"Loading source module: {source_module_path}")
            
            # Import module dynamically
            module_name = "src.modules.source"
            
            # Remove from sys.modules if already imported to force reload
            if module_name in sys.modules:
                self.log.info(f"Module {module_name} already imported, reloading...")
                module = importlib.reload(sys.modules[module_name])
            else:
                # Import module
                spec = importlib.util.spec_from_file_location(module_name, source_module_path)
                if spec is None or spec.loader is None:
                    raise ImportError(f"Could not load module spec from {source_module_path}")
                
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
                self.log.info(f"Module {module_name} imported successfully")
            
            # Get the create_source_materialized_view function from module
            if not hasattr(module, 'create_source_materialized_view'):
                raise AttributeError(
                    f"Module {module_name} does not have 'create_source_materialized_view' function. "
                    f"Expected function that creates MATERIALIZED VIEW using centralized materialized_view_utils"
                )
            
            create_view_function = getattr(module, 'create_source_materialized_view')
            self.log.info(f"Found create_source_materialized_view function: {create_view_function}")
            
            # Execute the function
            self.log.info("Executing create_source_materialized_view() to create MATERIALIZED VIEW...")
            try:
                df = create_view_function(spark)
                
                self.log.info(f"✓ create_source_materialized_view() executed successfully")
                self.log.info(f"  Schema: {df.schema}")
                record_count = df.count()
                self.log.info(f"  Records: {record_count}")
            except Exception as e:
                self.log.error(f"Error creating MATERIALIZED VIEW: {e}", exc_info=True)
                raise
            
            # Verify that the view was created in the catalog
            self._verify_view_in_catalog(spark, 'source_table')
            
            # Also create mv_source as an alias to source_table for compatibility
            # This ensures that {{ ref('mv_source') }} works in transformation SQL files
            # The source SQL file will also create mv_source, but this ensures it exists
            # even if the SQL file execution fails or is skipped
            try:
                source_table_df = spark.table('source_table')
                # Check if mv_source already exists
                tables = spark.catalog.listTables()
                mv_source_exists = any(t.name == 'mv_source' for t in tables)
                
                if not mv_source_exists:
                    self.log.info("Creating 'mv_source' as alias to 'source_table' for compatibility...")
                    from src.modules.materialized_view_utils import create_materialized_view_from_dataframe
                    create_materialized_view_from_dataframe(
                        spark=spark,
                        df=source_table_df,
                        view_name='mv_source',
                        if_not_exists=True,
                        verify=True
                    )
                    self.log.info("✓ 'mv_source' created successfully")
                else:
                    self.log.info("✓ 'mv_source' already exists in catalog")
            except Exception as alias_error:
                self.log.warning(f"Could not create mv_source alias: {alias_error}")
                self.log.info("The source SQL file will create mv_source instead")
            
            self.log.info("=" * 80)
            
        except Exception as e:
            self.log.error(f"Error executing source module: {e}", exc_info=True)
            raise RuntimeError(
                f"Failed to execute source module for {sql_path.name}. "
                f"The MATERIALIZED VIEW must be created before SQL can reference it. "
                f"Error: {e}"
            ) from e
    
    def _verify_view_in_catalog(self, spark: SparkSession, view_name: str, max_retries: int = 5, retry_delay: int = 1):
        """
        Verify that a view exists in the catalog.
        
        Args:
            spark: SparkSession instance
            view_name: Name of the view to verify
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
        """
        # SDP may register the view asynchronously, so we check multiple times
        view_found = False
        for attempt in range(max_retries):
            try:
                tables = spark.catalog.listTables()
                view_found = any(t.name == view_name for t in tables)
                
                if view_found:
                    self.log.info(f"✓ MATERIALIZED VIEW '{view_name}' found in catalog")
                    for table in tables:
                        if table.name == view_name:
                            self.log.info(f"  Type: {table.tableType}")
                            self.log.info(f"  Database: {table.database}")
                            break
                    break
                else:
                    if attempt < max_retries - 1:
                        self.log.info(f"  View not found yet, retrying in {retry_delay}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(retry_delay)
                    else:
                        self.log.warning(f"⚠ MATERIALIZED VIEW '{view_name}' not found in catalog after retries")
                        self.log.info("  Attempting to refresh catalog...")
                        # Try to refresh the catalog
                        try:
                            spark.catalog.refreshTable(view_name)
                            tables = spark.catalog.listTables()
                            view_found = any(t.name == view_name for t in tables)
                            if view_found:
                                self.log.info("✓ View found after catalog refresh")
                            else:
                                self.log.warning("  View still not found. It may be created lazily when first accessed.")
                        except Exception as refresh_error:
                            self.log.warning(f"  Could not refresh catalog: {refresh_error}")
            except Exception as catalog_error:
                self.log.warning(f"Could not verify view in catalog: {catalog_error}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        
        if not view_found:
            self.log.warning(f"⚠ MATERIALIZED VIEW '{view_name}' may not be registered yet")
            self.log.info("  The view will be created when first accessed by SQL")

