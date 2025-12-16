"""
Module for loading and refreshing views from the Spark catalog.
Ensures views created in previous tasks are accessible in current session.
"""
import logging
import re
from typing import List, Optional
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class ViewLoader:
    """Loads and refreshes views from Spark catalog to ensure cross-session access."""
    
    def __init__(self, logger_instance: Optional[logging.Logger] = None):
        """
        Initialize ViewLoader.
        
        Args:
            logger_instance: Logger instance (optional, uses module logger if not provided)
        """
        self.log = logger_instance or logger
    
    def _get_available_tables(self, spark: SparkSession) -> tuple:
        """
        Get available tables from catalog with case-insensitive mapping.
        
        Args:
            spark: SparkSession instance
            
        Returns:
            Tuple of (list of table names, dict mapping uppercase names to actual names)
        """
        available_tables = [t.name for t in spark.catalog.listTables()]
        available_tables_upper = {at.upper(): at for at in available_tables}
        return available_tables, available_tables_upper
    
    def ensure_dependent_views_available(
        self,
        spark: SparkSession,
        sql_statement: str
    ) -> None:
        """
        Ensure that all views referenced in SQL statement are available in catalog.
        
        This method detects table/view references in SQL and ensures they are
        accessible in the current SparkSession, even if they were created in
        a different session (previous Airflow task).
        
        Args:
            spark: SparkSession instance
            sql_statement: SQL statement to analyze
        """
        # Extract table/view references from SQL
        referenced_tables = self._extract_table_references(sql_statement)
        
        if not referenced_tables:
            return
        
        self.log.info(f"Detected table references in SQL: {referenced_tables}")
        
        # List available tables in catalog (cached for this method call)
        try:
            available_tables, available_tables_upper = self._get_available_tables(spark)
            self.log.debug(f"Available tables in catalog: {available_tables[:10]}...")
            
            # Check which tables are missing (case-insensitive check)
            missing_tables = []
            found_tables = []
            
            for ref_table in referenced_tables:
                # Check exact match
                if ref_table in available_tables:
                    found_tables.append(ref_table)
                # Check case-insensitive match
                elif ref_table.upper() in available_tables_upper:
                    actual_name = available_tables_upper[ref_table.upper()]
                    found_tables.append(actual_name)
                    self.log.info(
                        f"Found table '{actual_name}' (case-insensitive match for '{ref_table}')"
                    )
                else:
                    missing_tables.append(ref_table)
            
            if found_tables:
                self.log.info(f"Found tables in catalog: {found_tables}")
            
            if missing_tables:
                self.log.warning(f"Tables not found in catalog: {missing_tables}")
                self.log.info("Attempting to load Materialized Views from warehouse.dir...")
                self._load_views_from_warehouse(spark, missing_tables)
                
                # Re-check after loading (refresh cache)
                available_tables_after, available_tables_upper_after = self._get_available_tables(spark)
                
                still_missing = []
                for ref_table in missing_tables:
                    if (ref_table not in available_tables_after and
                        ref_table.upper() not in available_tables_upper_after):
                        still_missing.append(ref_table)
                
                if still_missing:
                    # Try to create missing views from known sources as a fallback
                    self.log.warning(f"Tables still not found after loading attempts: {still_missing}")
                    self.log.info("Attempting to create missing views from known sources...")
                    self._create_missing_views_from_sources(spark, still_missing)
                    
                    # Force refresh catalog after creating views
                    try:
                        for table in still_missing:
                            try:
                                spark.catalog.refreshTable(table)
                                self.log.debug(f"Refreshed catalog for '{table}'")
                            except Exception:
                                pass  # Table might not exist yet, that's ok
                    except Exception as refresh_error:
                        self.log.debug(f"Could not refresh catalog: {refresh_error}")
                    
                    # Final re-check after fallback creation (refresh cache)
                    available_tables_final, available_tables_upper_final = self._get_available_tables(spark)
                    
                    final_missing = []
                    for ref_table in still_missing:
                        if (ref_table not in available_tables_final and
                            ref_table.upper() not in available_tables_upper_final):
                            final_missing.append(ref_table)
                    
                    if final_missing:
                        self.log.error(
                            f"CRITICAL: Tables still not found after all attempts: {final_missing}"
                        )
                        self.log.error(
                            "This may indicate that the views were not created in previous tasks, "
                            "or there is a problem with view persistence."
                        )
                        self.log.error(
                            "Please verify that the dependent tasks completed successfully."
                        )
                    else:
                        self.log.info("All tables are now available after fallback creation")
                else:
                    self.log.info("All tables are now available in catalog after loading")
            else:
                self.log.info("All referenced tables found in catalog")
                
        except Exception as list_error:
            self.log.debug(f"Could not list tables: {list_error}")
    
    def _extract_table_references(self, sql_statement: str) -> List[str]:
        """
        Extract table/view references from SQL statement.
        
        Args:
            sql_statement: SQL statement
            
        Returns:
            List of table/view names referenced
        """
        references = set()
        
        # Pattern to detect FROM table_name or JOIN table_name
        # Exclude subqueries and common keywords
        patterns = [
            r'\bFROM\s+(\w+)\b',
            r'\bJOIN\s+(\w+)\b',
            r'\bINNER\s+JOIN\s+(\w+)\b',
            r'\bLEFT\s+JOIN\s+(\w+)\b',
            r'\bRIGHT\s+JOIN\s+(\w+)\b',
            r'\bFULL\s+JOIN\s+(\w+)\b',
        ]
        
        excluded_keywords = {
            'select', 'where', 'group', 'order', 'having', 'limit',
            'union', 'except', 'intersect', 'with', 'as', 'on', 'using'
        }
        
        for pattern in patterns:
            matches = re.finditer(pattern, sql_statement, re.IGNORECASE)
            for match in matches:
                table_name = match.group(1).strip()
                # Exclude common SQL keywords and subquery indicators
                if (table_name and
                    table_name.lower() not in excluded_keywords and
                    not table_name.startswith('(') and
                    table_name.lower() not in ['csv', 'parquet', 'json', 'orc', 'delta']):
                    references.add(table_name)
        
        return sorted(list(references))
    
    def _load_views_from_warehouse(
        self,
        spark: SparkSession,
        table_names: List[str]
    ) -> None:
        """
        Attempt to load views/tables from warehouse directory.
        
        Args:
            spark: SparkSession instance
            table_names: List of table names to load
        """
        try:
            warehouse_dir = spark.conf.get('spark.sql.warehouse.dir', '')
            if not warehouse_dir:
                self.log.warning("spark.sql.warehouse.dir not configured, cannot load from warehouse")
                return
            
            from pathlib import Path
            
            # Resolve warehouse path
            warehouse_path = Path(warehouse_dir.replace('file:', ''))
            if not warehouse_path.is_absolute():
                # Try to resolve relative to current directory
                warehouse_path = Path.cwd() / warehouse_dir
            
            if not warehouse_path.exists():
                self.log.warning(f"Warehouse directory does not exist: {warehouse_path}")
                return
            
            self.log.info(f"Searching for tables in warehouse: {warehouse_path}")
            
            # Try to refresh catalog for each missing table
            for table_name in table_names:
                # Try multiple approaches to make the table available
                table_loaded = False
                
                # Approach 1: Try to refresh the catalog entry
                try:
                    spark.catalog.refreshTable(table_name)
                    self.log.info(f"   ✓ Refreshed catalog for '{table_name}'")
                    
                    # Verify it's now available
                    current_tables, _ = self._get_available_tables(spark)
                    if table_name in current_tables:
                        self.log.info(f"   ✓ Table '{table_name}' now available in catalog")
                        table_loaded = True
                        continue
                except Exception as refresh_error:
                    self.log.debug(f"   Could not refresh '{table_name}': {refresh_error}")
                
                # Approach 2: Try case-insensitive refresh
                if not table_loaded:
                    try:
                        current_tables, _ = self._get_available_tables(spark)
                        # Find case-insensitive match
                        for available_table in current_tables:
                            if available_table.upper() == table_name.upper():
                                self.log.info(
                                    f"   Found case variation: '{available_table}' "
                                    f"(requested '{table_name}')"
                                )
                                # Try to refresh the actual table name
                                spark.catalog.refreshTable(available_table)
                                table_loaded = True
                                self.log.info(
                                    f"   ✓ Using '{available_table}' instead of '{table_name}'"
                                )
                                break
                    except Exception as case_error:
                        self.log.debug(f"   Could not refresh with case-insensitive match: {case_error}")
                
                # If refresh didn't work, try to find and register the table manually
                try:
                    # Search for table directory in warehouse
                    # Tables are stored directly in warehouse root: warehouse/table_name/
                    # (No database subdirectories are used)
                    table_found = False
                    table_dir = None
                    
                    # Try direct structure: warehouse/table_name/
                    direct_table_dir = warehouse_path / table_name
                    if direct_table_dir.exists() and direct_table_dir.is_dir():
                        table_dir = direct_table_dir
                        table_found = True
                        self.log.info(f"   Found '{table_name}' directly in warehouse: {direct_table_dir}")
                    else:
                        # Try case-insensitive search in warehouse root
                        for subdir in warehouse_path.iterdir():
                            if subdir.is_dir() and subdir.name.upper() == table_name.upper():
                                table_dir = subdir
                                table_found = True
                                self.log.info(
                                    f"   Found '{subdir.name}' (case variation of '{table_name}') in warehouse: {subdir}"
                                )
                                break
                    
                    # If table was found, register it in the catalog
                    if table_found and table_dir:
                        self.log.info(f"   Found '{table_name}' in warehouse, registering...")
                        # Try to register as external table
                        # Use the actual directory name found
                        actual_table_name = table_dir.name
                        
                        # Resolve absolute path for LOCATION
                        table_dir_abs = table_dir.resolve()
                        
                        # Try to create table with absolute path
                        try:
                            spark.sql(f"""
                                CREATE TABLE IF NOT EXISTS {table_name}
                                USING PARQUET
                                LOCATION '{table_dir_abs}'
                            """)
                            self.log.info(f"   ✓ Registered '{table_name}' in catalog")
                            table_loaded = True
                            
                            # Verify it's now available
                            current_tables, _ = self._get_available_tables(spark)
                            if table_name in current_tables:
                                self.log.info(f"   ✓ Verified '{table_name}' is now in catalog")
                        except Exception as create_error:
                            self.log.warning(
                                f"   Could not create table '{table_name}': {create_error}"
                            )
                            # Try with case-insensitive name if different
                            if actual_table_name != table_name:
                                try:
                                    spark.sql(f"""
                                        CREATE TABLE IF NOT EXISTS {actual_table_name}
                                        USING PARQUET
                                        LOCATION '{table_dir_abs}'
                                    """)
                                    self.log.info(
                                        f"   ✓ Registered '{actual_table_name}' "
                                        f"(case variation) in catalog"
                                    )
                                    table_loaded = True
                                except Exception:
                                    pass
                    
                    if not table_found:
                        self.log.warning(
                            f"   Could not find '{table_name}' in warehouse directory structure"
                        )
                except Exception as register_error:
                    self.log.debug(f"   Could not register '{table_name}': {register_error}")
                    
        except Exception as warehouse_error:
            self.log.debug(f"Error loading from warehouse: {warehouse_error}")
    
    def _create_missing_views_from_sources(
        self,
        spark: SparkSession,
        missing_tables: List[str]
    ) -> None:
        """
        Attempt to create missing views from known source tables.
        
        This is a fallback mechanism when views are not found in the catalog
        or warehouse. For example, if 'mv_source' is missing but 'source_table'
        exists, we can create 'mv_source' as an alias.
        
        Args:
            spark: SparkSession instance
            missing_tables: List of table names that are missing
        """
        try:
            available_tables, available_tables_set = self._get_available_tables(spark)
            
            self.log.info(f"Available tables in catalog: {available_tables}")
            self.log.info(f"Missing tables to create: {missing_tables}")
            
            # Known mappings: if a view is missing, try to create it from a known source
            source_mappings = {
                'mv_source': 'source_table',  # mv_source can be created from source_table
            }
            
            for missing_table in missing_tables:
                # Check if there's a known source mapping
                source_table = source_mappings.get(missing_table.lower())
                
                if source_table:
                    self.log.info(
                        f"Found mapping for '{missing_table}' -> '{source_table}'"
                    )
                    # Check if source table exists (case-insensitive)
                    source_found = None
                    if source_table in available_tables:
                        source_found = source_table
                        self.log.info(f"Found exact match: '{source_table}'")
                    elif source_table.upper() in available_tables_set:
                        source_found = available_tables_set[source_table.upper()]
                        self.log.info(
                            f"Found case-insensitive match: '{source_found}' (requested '{source_table}')"
                        )
                    
                    if source_found:
                        self.log.info(
                            f"Creating '{missing_table}' from source table '{source_found}'..."
                        )
                        try:
                            # Create the missing view as a Materialized View from the source
                            from src.modules.materialized_view_utils import (
                                create_materialized_view_from_dataframe
                            )
                            
                            self.log.info(f"Reading data from '{source_found}'...")
                            source_df = spark.table(source_found)
                            self.log.info(f"DataFrame schema: {source_df.schema}")
                            
                            self.log.info(f"Creating Materialized View '{missing_table}'...")
                            create_materialized_view_from_dataframe(
                                spark=spark,
                                df=source_df,
                                view_name=missing_table,
                                if_not_exists=True,
                                verify=True
                            )
                            
                            # Verify it was created
                            tables_after, _ = self._get_available_tables(spark)
                            if missing_table in tables_after:
                                self.log.info(f"✓ Successfully created '{missing_table}' from '{source_found}'")
                            else:
                                self.log.warning(
                                    f"⚠ '{missing_table}' was created but not found in catalog. "
                                    f"Available tables: {tables_after}"
                                )
                        except Exception as create_error:
                            self.log.error(
                                f"Failed to create '{missing_table}' from '{source_found}': {create_error}",
                                exc_info=True
                            )
                    else:
                        self.log.warning(
                            f"Source table '{source_table}' not found in catalog for '{missing_table}'. "
                            f"Available tables: {available_tables}"
                        )
                else:
                    self.log.debug(
                        f"No known source mapping for '{missing_table}'"
                    )
        except Exception as fallback_error:
            self.log.error(
                f"Error in fallback view creation: {fallback_error}",
                exc_info=True
            )

