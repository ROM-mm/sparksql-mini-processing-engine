"""
Module for exporting Materialized Views to disk.
"""
import logging
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession
from src.modules.exporting import ExportingConfig

logger = logging.getLogger(__name__)


class DataExporter:
    """Exports Materialized Views to disk."""
    
    def __init__(self, exporting_config: ExportingConfig, logger_instance: Optional[logging.Logger] = None):
        """
        Initialize DataExporter.
        
        Args:
            exporting_config: ExportingConfig instance
            logger_instance: Logger instance (optional, uses module logger if not provided)
        """
        self.exporting_config = exporting_config
        self.log = logger_instance or logger
    
    def export_view(
        self,
        spark: SparkSession,
        view_name: str,
        sql_statement: str,
        sql_path: Path,
        project_root: Optional[Path] = None
    ) -> None:
        """
        Export a Materialized View to disk.
        
        Args:
            spark: SparkSession instance
            view_name: Name of the view to export
            sql_statement: SQL statement that created the view (for format detection)
            sql_path: Path to SQL file
            project_root: Project root directory (optional)
        """
        self.log.info("=" * 80)
        self.log.info(f"EXPORTING VIEW '{view_name}' TO DISK")
        self.log.info("=" * 80)
        self.log.info("   This is the final step - converting view to DataFrame and writing to disk")
        self.log.info(f"   View name: {view_name}")
        self.log.info(f"   SQL file path: {sql_path}")
        
        # Read view as DataFrame
        try:
            self.log.info("   Step 1: Reading view as DataFrame...")
            self.log.info(f"   View name to read: {view_name}")
            df = spark.table(view_name)
            self.log.info(f"   ✓ DataFrame read successfully")
            self.log.info(f"   DataFrame schema: {df.schema}")
            
            # Get export configuration from YAML
            self.log.info("   Step 2: Loading export configuration from pipeline.yml...")
            
            # Determine format from SQL or default to parquet
            format_from_sql = self._detect_format_from_sql(sql_statement)
            
            # Get configuration for the determined format
            write_config = self._get_write_config(format_from_sql)
            
            # Determine output path
            output_path = self._determine_output_path(view_name, write_config, project_root)
            
            # Log DataFrame info before writing
            self._log_dataframe_info(df)
            
            # Apply write configuration and save
            self._save_dataframe(df, output_path, write_config)
            
            # Verify files were created
            self._verify_export(output_path)
            
            self.log.info(f"   View '{view_name}' exported successfully to {output_path}!")
            
        except Exception as export_error:
            self.log.error(f"   Error exporting view: {export_error}")
            import traceback
            self.log.error(traceback.format_exc())
            raise RuntimeError(f"Failed to export view '{view_name}' to disk: {export_error}") from export_error
    
    def _detect_format_from_sql(self, sql_statement: str) -> str:
        """
        Detect format from SQL statement.
        
        Args:
            sql_statement: SQL statement
            
        Returns:
            Format name (parquet, csv, json, etc.)
        """
        statement_upper = sql_statement.upper()
        if 'USING PARQUET' in statement_upper:
            return 'parquet'
        elif 'USING CSV' in statement_upper:
            return 'csv'
        elif 'USING JSON' in statement_upper:
            return 'json'
        elif 'USING ORC' in statement_upper:
            return 'orc'
        elif 'USING DELTA' in statement_upper:
            return 'delta'
        else:
            return 'parquet'  # default
    
    def _get_write_config(self, format_type: str) -> dict:
        """
        Get write configuration for format.
        
        Args:
            format_type: Format name
            
        Returns:
            Write configuration dictionary
        """
        if format_type == 'parquet':
            return self.exporting_config.get_parquet_config()
        elif format_type == 'csv':
            return self.exporting_config.get_csv_config()
        elif format_type == 'json':
            return self.exporting_config.get_json_config()
        elif format_type == 'orc':
            return self.exporting_config.get_orc_config()
        elif format_type == 'delta':
            return self.exporting_config.get_delta_config()
        else:
            return self.exporting_config.get_parquet_config()
    
    def _determine_output_path(self, view_name: str, write_config: dict, project_root: Optional[Path] = None) -> str:
        """
        Determine output path for export.
        
        Args:
            view_name: Name of the view
            write_config: Write configuration
            project_root: Project root directory (optional)
            
        Returns:
            Absolute output path
        """
        view_specific_path = write_config.get('path', '')
        base_path = write_config.get('base_path', '')
        
        self.log.info(f"   Write config keys: {list(write_config.keys())}")
        self.log.info(f"   Config path from YAML: {view_specific_path}")
        self.log.info(f"   Base path from YAML: {base_path}")
        
        # If path in config is for a specific view, use it; otherwise construct path
        if view_specific_path:
            if view_name in view_specific_path or Path(view_specific_path).is_absolute():
                output_path = view_specific_path
                self.log.info(f"   Using configured path directly: {output_path}")
            else:
                if view_specific_path.endswith('/'):
                    output_path = f"{view_specific_path}{view_name}"
                else:
                    output_path = f"{view_specific_path}/{view_name}"
                self.log.info(f"   Constructed path from config: {output_path}")
        else:
            if not base_path:
                base_path = 'lakehouse/silver'
                self.log.warning(f"   No base_path in config, using default: {base_path}")
            if base_path.endswith('/'):
                output_path = f"{base_path}{view_name}"
            else:
                output_path = f"{base_path}/{view_name}"
            self.log.info(f"   Constructed path from base_path: {output_path}")
        
        # Resolve relative paths
        if not Path(output_path).is_absolute():
            if project_root is None:
                # Try to detect project root
                current = Path.cwd()
                for _ in range(10):
                    if (current / "src" / "sql").exists():
                        project_root = current
                        break
                    if current.parent == current:
                        break
                    current = current.parent
            
            if project_root:
                output_path = str(project_root / output_path)
            self.log.info(f"   Resolved relative path to: {output_path}")
        
        # Ensure parent directory exists
        output_path_obj = Path(output_path)
        output_path_obj.parent.mkdir(parents=True, exist_ok=True)
        self.log.info(f"   Ensured parent directory exists: {output_path_obj.parent}")
        
        self.log.info(f"   Final output path: {output_path}")
        self.log.info(f"   Format: {write_config.get('format', 'parquet')}")
        self.log.info(f"   Mode: {write_config.get('mode', 'overwrite')}")
        
        return output_path
    
    def _log_dataframe_info(self, df):
        """Log DataFrame information."""
        try:
            row_count = df.count()
            self.log.info(f"   DataFrame row count: {row_count}")
            self.log.info(f"   DataFrame schema:")
            for field in df.schema.fields:
                self.log.info(f"      - {field.name}: {field.dataType}")
        except Exception as df_info_error:
            self.log.warning(f"   Could not get DataFrame info: {df_info_error}")
    
    def _save_dataframe(self, df, output_path: str, write_config: dict):
        """Save DataFrame to disk."""
        self.log.info("   Applying write configuration and saving DataFrame...")
        self.log.info(f"   Output path to save: {output_path}")
        self.log.info(f"   Write config: {write_config}")
        try:
            writer = self.exporting_config.apply_write_config(df, write_config)
            self.log.info("   Writer configured successfully")
            self.log.info(f"   Writer format: {write_config.get('format', 'unknown')}")
            self.log.info(f"   Writer mode: {write_config.get('mode', 'unknown')}")
            self.log.info(f"   Writer options: {write_config.get('options', {})}")
            self.log.info("   Calling save()...")
            self.log.info(f"   Saving to: {output_path}")
            writer.save(output_path)
            self.log.info("   ✓ save() completed successfully")
            
            # Verify the file was actually created
            output_path_obj = Path(output_path)
            if output_path_obj.exists():
                self.log.info(f"   ✓ Output directory exists: {output_path_obj}")
                files = list(output_path_obj.rglob("*"))
                self.log.info(f"   ✓ Found {len(files)} file(s) in output directory")
                for f in files[:10]:  # Show first 10 files
                    self.log.info(f"      - {f.name} ({f.stat().st_size} bytes)")
            else:
                self.log.warning(f"   ⚠ Output directory does not exist: {output_path_obj}")
        except Exception as save_error:
            self.log.error(f"   ✗ Error during save(): {save_error}")
            import traceback
            self.log.error(traceback.format_exc())
            raise
    
    def _verify_export(self, output_path: str):
        """Verify that export files were created."""
        output_path_obj = Path(output_path)
        if output_path_obj.exists():
            parquet_files = list(output_path_obj.rglob("*.parquet"))
            if parquet_files:
                total_size = sum(f.stat().st_size for f in parquet_files)
                size_mb = total_size / (1024 * 1024)
                self.log.info(f"   Created {len(parquet_files)} parquet file(s) ({size_mb:.2f} MB)")
            else:
                csv_files = list(output_path_obj.rglob("*.csv"))
                json_files = list(output_path_obj.rglob("*.json"))
                if csv_files or json_files:
                    self.log.info(f"   Created {len(csv_files)} CSV file(s) and {len(json_files)} JSON file(s)")
                else:
                    self.log.warning(f"   No output files found in {output_path}")
        else:
            self.log.warning(f"   Output directory does not exist: {output_path}")

