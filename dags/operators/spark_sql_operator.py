"""
Custom Airflow operator to execute SQL files in Spark.
Supports dbt-style templates with {{ ref('name') }}.
"""
import sys
import logging
import re
from pathlib import Path
from typing import Optional, Dict
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pyspark.sql import SparkSession

# Add project path to PYTHONPATH
dag_file = Path(__file__).resolve()
if 'airflow_home' in str(dag_file):
    project_root = dag_file.parent.parent.parent
else:
    project_root = dag_file.parent.parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.modules.config_loader import ConfigLoader
from src.modules.exporting import ExportingConfig
from src.modules.materialized_view_utils import execute_create_materialized_view_sql
from src.modules.spark_session_manager import SparkSessionManager
from src.modules.sql_file_processor import SQLFileProcessor
from src.modules.sql_file_utils import SQLFileUtils
from src.modules.source_module_executor import SourceModuleExecutor
from src.modules.catalog_manager import CatalogManager
from src.modules.data_exporter import DataExporter
from src.modules.dataframe_display import DataFrameDisplay
from src.modules.view_validator import ViewValidator
from src.modules.view_loader import ViewLoader

# Configure logging
logger = logging.getLogger(__name__)


class SparkSQLFileOperator(BaseOperator):
    """
    Operator that executes a SQL file in Spark.

    Args:
        sql_file_path: Path to SQL file
        spark_config: Dictionary with Spark configurations
        ref_map: Mapping of ref names to view names for template processing
        has_templates: Whether SQL file contains dbt-style templates
        task_id: Task ID (inherited from BaseOperator)
        **kwargs: Additional BaseOperator arguments
    """

    template_fields = ('sql_file_path',)

    @apply_defaults
    def __init__(
        self,
        sql_file_path: str,
        spark_config: Optional[dict] = None,
        ref_map: Optional[Dict[str, str]] = None,
        has_templates: bool = False,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql_file_path = sql_file_path
        self.spark_config = spark_config or {}
        self.ref_map = ref_map or {}
        self.has_templates = has_templates

        # Initialize configuration
        project_root = Path(__file__).parent.parent.parent
        self.config_loader = ConfigLoader(project_root)
        self.exporting_config = ExportingConfig(self.config_loader)

        # Initialize helper modules
        self.session_manager = SparkSessionManager(self.spark_config, self.log)
        self.sql_processor = SQLFileProcessor(self.log)
        self.source_executor = SourceModuleExecutor(self.log)
        self.catalog_manager = CatalogManager(self.config_loader, self.log)
        self.data_exporter = DataExporter(self.exporting_config, self.log)
        self.display = DataFrameDisplay(self.log)
        self.validator = ViewValidator(self.log)
        self.view_loader = ViewLoader(self.log)

    def execute(self, context):
        """
        Execute SQL file in Spark.

        Args:
            context: Airflow context
        """
        self.log.info("\n" + "=" * 80)
        self.log.info("STARTING SPARK SQL EXECUTION")
        self.log.info("=" * 80)
        self.log.info(f"Task ID: {self.task_id}")
        dag_run = context.get('dag_run')
        self.log.info(f"DAG Run ID: {dag_run.run_id if dag_run else 'N/A'}")
        self.log.info(f"Execution Date: {context.get('execution_date', 'N/A')}")

        # Resolve file path
        sql_path = Path(self.sql_file_path)
        if not sql_path.is_absolute():
            project_root = Path(__file__).parent.parent.parent
            sql_path = project_root / sql_path

        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")

        # Determine project root for utilities
        project_root = Path(__file__).parent.parent.parent

        # Check file type
        is_exporting = SQLFileUtils.is_export_file(sql_path, project_root)
        is_source = SQLFileUtils.is_source_file(sql_path, project_root)

        self.log.info(f"\nSQL File: {sql_path}")
        self.log.info(f"Absolute Path: {sql_path.resolve()}")
        file_type = 'EXPORT' if is_exporting else 'SOURCE' if is_source else 'TRANSFORMATION'
        self.log.info(f"File Type: {file_type}")

        # Setup Spark environment and create session
        self.log.info("\nConfiguring Spark environment...")
        self.session_manager.setup_environment()

        self.log.info("\nCreating SparkSession...")
        if self.spark_config:
            self.log.info(f"Spark Configurations ({len(self.spark_config)} configs):")
            for key, value in list(self.spark_config.items())[:5]:
                self.log.info(f"  {key}: {value}")
            if len(self.spark_config) > 5:
                remaining = len(self.spark_config) - 5
                self.log.info(f"  ... and {remaining} more configurations")

        spark = self.session_manager.create_session(context)
        self.session_manager.log_session_info(spark)

        # Ensure correct catalog and database
        self.catalog_manager.ensure_catalog_database(spark)

        # Execute source module if this is a source file
        if is_source:
            self.log.info("=" * 80)
            self.log.info("DETECTED SOURCE FILE - Executing Python module first")
            self.log.info("=" * 80)
            self.source_executor.execute_source_module(spark, sql_path, project_root)

        try:
            # Read and process SQL file
            self.log.info(f"\nReading SQL file: {sql_path}")
            with open(sql_path, 'r', encoding='utf-8') as f:
                original_sql_content = f.read()

            self.log.info(f"File size: {len(original_sql_content)} characters")

            # Process SQL: remove CREATE VIEW, add CREATE MATERIALIZED VIEW, process templates
            self.log.info(f"\nProcessing SQL file...")
            sql_content = self.sql_processor.process_sql_file(
                sql_path,
                original_sql_content,
                self.ref_map,
                project_root
            )

            # Split into statements
            statements = [
                s.strip()
                for s in sql_content.split(';')
                if s.strip() and not s.strip().startswith('--')
            ]

            self.log.info(f"Found {len(statements)} SQL statement(s) to execute\n")

            # Execute each statement
            for i, statement in enumerate(statements, 1):
                if statement:
                    self._execute_statement(
                        spark,
                        statement,
                        i,
                        len(statements),
                        is_exporting,
                        sql_path,
                        project_root
                    )

            self.log.info("\n" + "=" * 80)
            self.log.info(f"SQL FILE {sql_path.name} EXECUTED SUCCESSFULLY!")
            self.log.info("=" * 80)

            # For source files, verify that mv_source was created (if this is the source file)
            if is_source:
                view_name_from_file = sql_path.stem  # Should be 'mv_source'
                self.log.info(f"\nVerifying that '{view_name_from_file}' was created...")
                try:
                    tables = spark.catalog.listTables()
                    view_found = any(t.name == view_name_from_file for t in tables)
                    if view_found:
                        self.log.info(f"✓ '{view_name_from_file}' found in catalog")
                        # Try to access it to ensure it's working
                        try:
                            test_df = spark.table(view_name_from_file)
                            record_count = test_df.count()
                            self.log.info(f"✓ '{view_name_from_file}' is accessible ({record_count} records)")
                        except Exception as access_error:
                            self.log.warning(f"⚠ '{view_name_from_file}' exists but may not be accessible: {access_error}")
                    else:
                        self.log.warning(f"⚠ '{view_name_from_file}' not found in catalog after SQL execution")
                        self.log.info("  Attempting to refresh catalog...")
                        try:
                            spark.catalog.refreshTable(view_name_from_file)
                            tables = spark.catalog.listTables()
                            view_found = any(t.name == view_name_from_file for t in tables)
                            if view_found:
                                self.log.info(f"✓ '{view_name_from_file}' found after catalog refresh")
                            else:
                                self.log.warning(f"⚠ '{view_name_from_file}' still not found after refresh")
                        except Exception as refresh_error:
                            self.log.warning(f"  Could not refresh catalog: {refresh_error}")
                except Exception as verify_error:
                    self.log.warning(f"Could not verify '{view_name_from_file}': {verify_error}")

            # Final validation
            expected_views = ['table_customers_final']  # Can be configured
            self.validator.validate_all_views(spark, expected_views)
            self.validator.search_parquet_files(spark, project_root)

        except Exception as e:
            self.log.error("\n" + "=" * 80)
            self.log.error(f"CRITICAL ERROR EXECUTING SQL FILE")
            self.log.error("=" * 80)
            self.log.error(f"Error: {type(e).__name__}: {str(e)}")
            self.log.error(f"File: {sql_path}")
            self.log.error("=" * 80)
            raise
        finally:
            try:
                self.log.info("\nStopping SparkSession...")
                spark.stop()
                self.log.info("SparkSession stopped")
            except Exception as e:
                self.log.warning(f"Error stopping SparkSession: {e}")

    def _execute_statement(
        self,
        spark: SparkSession,
        statement: str,
        statement_num: int,
        total_statements: int,
        is_exporting: bool,
        sql_path: Path,
        project_root: Optional[Path]
    ):
        """
        Execute a single SQL statement.

        Args:
            spark: SparkSession instance
            statement: SQL statement to execute
            statement_num: Statement number (for logging)
            total_statements: Total number of statements
            is_exporting: Whether this is an export file
            sql_path: Path to SQL file
            project_root: Project root directory
        """
        self.log.info("=" * 80)
        self.log.info(f"EXECUTING STATEMENT {statement_num}/{total_statements}")
        self.log.info("=" * 80)

        # Detect operation type
        statement_upper = statement.upper().strip()
        if 'CREATE TEMPORARY VIEW' in statement_upper:
            self.log.info("Type: CREATE TEMPORARY VIEW")
        elif 'CREATE MATERIALIZED VIEW' in statement_upper:
            self.log.info("Type: CREATE MATERIALIZED VIEW")
        elif 'CREATE TABLE' in statement_upper:
            self.log.info("Type: CREATE TABLE")
        elif 'SELECT' in statement_upper:
            self.log.info("Type: SELECT")
        else:
            self.log.info("Type: Other SQL command")

        # Log SQL (with size limit)
        if len(statement) > 1000:
            self.log.info(f"SQL (first 500 chars):\n{statement[:500]}...")
            self.log.info(f"SQL (last 500 chars):\n...{statement[-500:]}")
        else:
            self.log.info(f"Complete SQL:\n{statement}")

        try:
            # Ensure dependent views are available before executing
            # This is critical for cross-session access in Airflow
            self.view_loader.ensure_dependent_views_available(spark, statement)
            
            # Check if this is a CREATE MATERIALIZED VIEW statement
            if 'CREATE MATERIALIZED VIEW' in statement_upper:
                # Extract view name
                view_match = re.search(
                    r'CREATE\s+MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)',
                    statement_upper
                )
                view_name = view_match.group(1) if view_match else None

                # Use utility function with automatic fallback
                execute_create_materialized_view_sql(spark, statement, view_name)

                # Get result DataFrame
                if view_name:
                    try:
                        result = spark.table(view_name)
                    except Exception:
                        result = spark.createDataFrame([], schema=[])
                else:
                    result = spark.createDataFrame([], schema=[])

                # Display preview and handle export if needed
                if view_name:
                    self.log.info(f"View '{view_name}' created successfully")
                    self.log.info(f"   is_exporting flag: {is_exporting}")

                    # Validate view in catalog
                    self._validate_view_in_catalog(spark, view_name)

                    # Display preview
                    view_df = spark.table(view_name)
                    self.display.display_preview(view_df, view_name=view_name, max_rows=10)

                    # Export if this is an export file
                    if is_exporting:
                        self.data_exporter.export_view(
                            spark,
                            view_name,
                            statement,
                            sql_path,
                            project_root
                        )
            else:
                # For other statements, execute normally
                result = spark.sql(statement)

                # Display preview for SELECT queries
                if 'SELECT' in statement_upper and 'CREATE' not in statement_upper:
                    self.log.info("SELECT query executed")
                    self.display.display_preview(
                        result,
                        view_name="SELECT query result",
                        max_rows=10
                    )

            self.log.info(f"Statement {statement_num} executed successfully!")

        except Exception as e:
            self.log.error("=" * 80)
            self.log.error(f"ERROR EXECUTING STATEMENT {statement_num}")
            self.log.error("=" * 80)
            self.log.error(f"Error: {type(e).__name__}: {str(e)}")
            self.log.error(f"\nSQL that caused the error:\n{statement}")
            self.log.error("=" * 80)
            raise

    def _validate_view_in_catalog(self, spark: SparkSession, view_name: str):
        """
        Validate that a view exists in the catalog and is accessible.

        Args:
            spark: SparkSession instance
            view_name: Name of the view to validate
        """
        self.log.info(f"\nValidating view '{view_name}' in Spark catalog...")

        try:
            tables = spark.catalog.listTables()
            view_found = False
            for table in tables:
                if table.name == view_name:
                    view_found = True
                    self.log.info(f"View '{view_name}' found in catalog!")
                    self.log.info(f"   Type: {table.tableType}")
                    self.log.info(f"   Database: {table.database}")
                    break

            if not view_found:
                self.log.warning(f"View '{view_name}' NOT found in catalog!")
                self.log.warning("   This may indicate that the view was not created correctly.")

            # Test access to view
            try:
                self.log.info(f"Testing access to view '{view_name}'...")
                test_query = spark.sql(f"SELECT * FROM {view_name} LIMIT 0")
                test_query.collect()
                self.log.info(f"View '{view_name}' verified and accessible")
            except Exception as access_error:
                self.log.warning(f"Could not access view '{view_name}': {access_error}")

        except Exception as catalog_error:
            self.log.warning(f"Error checking catalog: {catalog_error}")
