"""
Module for validating views and tables in Spark catalog.
"""
import logging
import re
from pathlib import Path
from typing import Optional, List
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class ViewValidator:
    """Validates views and tables in Spark catalog."""
    
    def __init__(self, logger_instance: Optional[logging.Logger] = None):
        """
        Initialize ViewValidator.
        
        Args:
            logger_instance: Logger instance (optional, uses module logger if not provided)
        """
        self.log = logger_instance or logger
    
    def validate_all_views(self, spark: SparkSession, expected_views: Optional[List[str]] = None):
        """
        Validate all views/tables created in Spark catalog.
        
        Args:
            spark: SparkSession instance
            expected_views: List of expected view names (optional)
        """
        try:
            self.log.info("\n" + "=" * 80)
            self.log.info("FINAL VALIDATION: Views/Tables created in Spark")
            self.log.info("=" * 80)
            
            tables = spark.catalog.listTables()
            if tables:
                self.log.info(f"\nTotal views/tables in catalog: {len(tables)}")
                
                # Separate by type
                temp_views = []
                materialized_views = []
                other_tables = []
                
                for table in tables:
                    if table.tableType.lower() == 'view' or 'temp' in table.tableType.lower():
                        temp_views.append(table)
                    elif table.tableType.lower() in ['managed', 'external']:
                        materialized_views.append(table)
                    else:
                        other_tables.append(table)
                
                # Log Temporary Views
                if temp_views:
                    self.log.info(f"\nTemporary Views ({len(temp_views)}):")
                    for table in temp_views:
                        self.log.info(f"  {table.name} (type: {table.tableType})")
                
                # Log Materialized Views (persisted tables)
                if materialized_views:
                    self.log.info(f"\nMaterialized Views / Persisted Tables ({len(materialized_views)}):")
                    for table in materialized_views:
                        self._log_table_info(spark, table)
                
                # Log other tables
                if other_tables:
                    self.log.info(f"\nOther Tables ({len(other_tables)}):")
                    for table in other_tables:
                        self.log.info(f"  - {table.name} (type: {table.tableType})")
                
                # Check if expected Materialized Views exist
                if expected_views:
                    found_materialized = [t.name for t in materialized_views]
                    missing = [name for name in expected_views if name not in found_materialized]
                    
                    if missing:
                        self.log.warning(f"\nExpected Materialized Views not found: {missing}")
                        self.log.warning("   Check if CREATE MATERIALIZED VIEW was executed correctly.")
                    else:
                        self.log.info(f"\nAll expected Materialized Views were created!")
            else:
                self.log.warning("No tables/views found in catalog!")
                self.log.warning("   This may indicate that views were not created correctly.")
        except Exception as e:
            self.log.error(f"Error validating views/tables: {e}")
            import traceback
            self.log.error(traceback.format_exc())
    
    def _log_table_info(self, spark: SparkSession, table):
        """Log detailed information about a table."""
        try:
            self.log.info(f"  {table.name} (type: {table.tableType})")
            table_info = spark.catalog.getTable(table.name)
            if hasattr(table_info, 'storage') and table_info.storage.locationUri:
                location = table_info.storage.locationUri
                self.log.info(f"     Location: {location}")
                
                # Check if path exists
                try:
                    location_path = Path(location.replace('file:', ''))
                    if location_path.exists():
                        self.log.info(f"     Directory exists in file system")
                        parquet_files = list(location_path.rglob("*.parquet"))
                        if parquet_files:
                            total_size = sum(f.stat().st_size for f in parquet_files)
                            size_mb = total_size / (1024 * 1024)
                            self.log.info(f"     {len(parquet_files)} Parquet file(s) ({size_mb:.2f} MB)")
                        else:
                            self.log.warning(f"     No Parquet files found in {location_path}")
                    else:
                        self.log.warning(f"     Directory does not exist: {location_path}")
                except Exception as path_error:
                    self.log.debug(f"     Error checking path: {path_error}")
            
            # Check record count
            try:
                qualified = self._quote_table_identifier(table.name)
                count = spark.sql(f"SELECT COUNT(*) as cnt FROM {qualified}").collect()[0]['cnt']
                self.log.info(f"     Total records: {count}")
            except Exception as count_error:
                self.log.debug(f"     Error counting records: {count_error}")
        except Exception as table_info_error:
            self.log.debug(f"     Error getting information: {table_info_error}")

    @staticmethod
    def _quote_table_identifier(name: str) -> str:
        """
        Quote and validate a Spark SQL table identifier to avoid SQL injection / parse errors.

        Supports unqualified ("tbl") and qualified ("db.tbl") names composed of:
        letters, digits, underscore. Anything else is rejected.
        """
        if name is None:
            raise ValueError("Table name cannot be None")

        name = name.strip()
        if not name:
            raise ValueError("Table name cannot be empty")

        # Allow db.table (Spark catalog listTables often returns unqualified names, but be safe)
        parts = name.split(".")
        if len(parts) > 2:
            raise ValueError(f"Unsupported table identifier format: {name!r}")

        ident_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
        for part in parts:
            if not ident_re.match(part):
                raise ValueError(f"Unsafe table identifier component: {part!r}")

        return ".".join(f"`{p}`" for p in parts)
    
    def search_parquet_files(self, spark: SparkSession, project_root: Optional[Path] = None):
        """
        Search for parquet files in common locations.
        
        Args:
            spark: SparkSession instance
            project_root: Project root directory (optional)
        """
        try:
            self.log.info("\n" + "=" * 80)
            self.log.info("COMPLEMENTARY VERIFICATION: Searching for Parquet files in file system")
            self.log.info("=" * 80)
            
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
            
            if project_root is None:
                self.log.warning("Could not determine project root, skipping parquet file search")
                return
            
            # Common locations where Spark may save
            search_paths = [
                project_root / "lakehouse" / "silver",
                project_root / "lakehouse" / "gold",
                project_root / "lakehouse" / ".meta",
                project_root / "spark-warehouse",
                project_root / "spark-warehouse" / "default.db",
                project_root / "lakehouse" / ".meta" / "default.db",
            ]
            
            # Also check warehouse.dir
            try:
                warehouse_dir = spark.conf.get('spark.sql.warehouse.dir', '')
                if warehouse_dir:
                    warehouse_path = Path(warehouse_dir.replace('file:', ''))
                    if not warehouse_path.is_absolute():
                        warehouse_path = project_root / warehouse_dir
                    search_paths.insert(0, warehouse_path)
                    search_paths.insert(1, warehouse_path / "default.db")
                    
                    self.log.info(f"\nWarehouse Dir configured: {warehouse_dir}")
                    self.log.info(f"   Resolved path: {warehouse_path.resolve()}")
            except Exception as warehouse_error:
                self.log.debug(f"Error processing warehouse.dir: {warehouse_error}")
            
            found_parquet = False
            for search_path in search_paths:
                if search_path.exists():
                    parquet_files = list(search_path.rglob("*.parquet"))
                    if parquet_files:
                        found_parquet = True
                        self.log.info(f"\nPARQUET FILES FOUND in: {search_path.relative_to(project_root)}")
                        self.log.info(f"   Total: {len(parquet_files)} file(s)")
                        for pf in parquet_files[:10]:  # Show first 10
                            size = pf.stat().st_size if pf.exists() else 0
                            size_mb = size / (1024 * 1024)
                            self.log.info(f"   {pf.relative_to(project_root)} ({size_mb:.2f} MB)")
                        if len(parquet_files) > 10:
                            self.log.info(f"   ... and {len(parquet_files) - 10} more file(s)")
                else:
                    self.log.debug(f"   Directory does not exist: {search_path}")
            
            if not found_parquet:
                self.log.warning("\nNO PARQUET FILES FOUND in file system!")
                self.log.warning("   This may be normal if:")
                self.log.warning("   - Materialized Views were created but not yet materialized")
                self.log.warning("   - Spark is using a different format")
                self.log.warning("   - Data is in another location")
                self.log.warning("\n   Check the 'FINAL VALIDATION' section above to see if views")
                self.log.warning("      are in the Spark catalog (this is more important than physical files).")
            else:
                self.log.info("\nParquet files found in file system!")
                
        except Exception as search_error:
            self.log.warning(f"Error in complementary verification: {search_error}")
            import traceback
            self.log.debug(traceback.format_exc())

