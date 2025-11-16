"""
Utility module for creating Materialized Views using Spark Declarative Pipelines (SDP).

This module provides reusable functions for creating Materialized Views throughout the project.
Materialized Views are automatically persisted by SDP and can be shared between Spark sessions.

This module centralizes the logic for Materialized View creation, ensuring consistency across:
- Source ingestion (from files)
- Transformations (from SQL queries)
- Export operations (final outputs)
"""
from pathlib import Path
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
import logging

logger = logging.getLogger(__name__)


def ensure_database(spark: SparkSession, database: str = "default") -> None:
    """
    Ensure that the correct database is used for Materialized Views.
    
    Args:
        spark: SparkSession instance
        database: Database name to use (default: "default")
    """
    if database != "default":
        try:
            spark.sql(f"USE {database}")
            logger.info(f"Using database: {database}")
        except Exception as e:
            logger.warning(f"Could not switch to database {database}: {e}")
            logger.info("Using default database")


def _cleanup_table_directory(spark: SparkSession, view_name: str) -> None:
    """
    Clean up table directory if it exists to prevent LOCATION_ALREADY_EXISTS errors.
    
    Args:
        spark: SparkSession instance
        view_name: Name of the table/view to clean up
    """
    try:
        warehouse_dir = spark.conf.get('spark.sql.warehouse.dir', '')
        if not warehouse_dir:
            return
        
        from pathlib import Path
        import shutil
        
        # Resolve warehouse directory path
        warehouse_dir_clean = warehouse_dir.replace('file:', '').strip()
        warehouse_path = Path(warehouse_dir_clean)
        
        if not warehouse_path.is_absolute():
            # Try multiple resolution strategies
            # 1. Relative to current working directory
            warehouse_path = Path.cwd() / warehouse_dir_clean
            
            # 2. If that doesn't exist, try relative to project root
            if not warehouse_path.exists():
                # Try to find project root (where pipeline.yml is)
                current_file = Path(__file__)
                project_root = current_file.parent.parent.parent
                warehouse_path = project_root / warehouse_dir_clean
        
        # Get current database
        try:
            current_db = spark.sql("SELECT current_database()").collect()[0][0]
        except Exception:
            current_db = "default"
        
        # Check for table directory in warehouse
        # Tables are stored as: warehouse_dir/database/table_name
        table_dir = warehouse_path / current_db / view_name
        
        if table_dir.exists():
            logger.warning(f"Table directory still exists at {table_dir}, removing it...")
            shutil.rmtree(table_dir)
            logger.info(f"✓ Removed existing table directory: {table_dir}")
        
        # Also check if table directory exists directly in warehouse (without database subdirectory)
        table_dir_direct = warehouse_path / view_name
        if table_dir_direct.exists():
            logger.warning(f"Table directory also exists at {table_dir_direct}, removing it...")
            shutil.rmtree(table_dir_direct)
            logger.info(f"✓ Removed existing table directory: {table_dir_direct}")
            
    except Exception as cleanup_error:
        logger.debug(f"Could not clean up table directory (may not exist): {cleanup_error}")


def create_materialized_view_from_dataframe(
    spark: SparkSession,
    df: DataFrame,
    view_name: str,
    database: Optional[str] = None,
    if_not_exists: bool = True,
    verify: bool = True
) -> DataFrame:
    """
    Create a MATERIALIZED VIEW from a DataFrame.
    
    This is the recommended way to create Materialized Views in SDP when working with DataFrames.
    The view will be automatically persisted by SDP and registered in the Spark catalog.
    
    Args:
        spark: SparkSession instance
        df: DataFrame to materialize
        view_name: Name of the Materialized View
        database: Database name (optional, uses current database if not provided)
        if_not_exists: If True, uses CREATE MATERIALIZED VIEW IF NOT EXISTS
        verify: If True, verifies that the view was created in the catalog
        
    Returns:
        DataFrame: DataFrame of the created Materialized View
        
    Example:
        >>> df = spark.read.csv("data.csv")
        >>> mv_df = create_materialized_view_from_dataframe(spark, df, "my_view")
    """
    # Ensure correct database
    if database:
        ensure_database(spark, database)
    
    # Create temporary view for Materialized View creation
    temp_view_name = f"temp_{view_name}"
    df.createOrReplaceTempView(temp_view_name)
    
    logger.info(f"Created temporary view '{temp_view_name}' for Materialized View creation")
    
    # Check if table/view already exists and drop it if needed
    # This prevents LOCATION_ALREADY_EXISTS errors
    try:
        tables = spark.catalog.listTables()
        table_exists = any(t.name == view_name for t in tables)
        
        if table_exists:
            logger.info(f"Table/view '{view_name}' already exists, dropping it first...")
            try:
                # Try dropping as MATERIALIZED VIEW first
                spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
            except Exception:
                # If that fails, try dropping as TABLE
                spark.sql(f"DROP TABLE IF EXISTS {view_name}")
            logger.info(f"✓ Existing table/view '{view_name}' dropped")
    except Exception as check_error:
        logger.debug(f"Could not check for existing table: {check_error}")
    
    # Try CREATE MATERIALIZED VIEW first (SDP syntax)
    # If that fails, fall back to CREATE TABLE AS SELECT (standard Spark)
    if_exists_clause = "IF NOT EXISTS" if if_not_exists else ""
    
    # First, try SDP syntax (CREATE MATERIALIZED VIEW)
    create_view_sql_sdp = f"""
        CREATE MATERIALIZED VIEW {if_exists_clause} {view_name} AS
        SELECT * FROM {temp_view_name}
    """
    
    logger.info(f"Attempting to create MATERIALIZED VIEW '{view_name}' using SDP syntax...")
    logger.debug(f"SQL: {create_view_sql_sdp}")
    
    try:
        # Try SDP syntax first
        spark.sql(create_view_sql_sdp)
        logger.info(f"✓ MATERIALIZED VIEW '{view_name}' created using SDP syntax")
    except Exception as sdp_error:
        # If SDP syntax fails, fall back to standard Spark CREATE TABLE AS SELECT
        logger.warning(f"SDP syntax not supported, falling back to CREATE TABLE AS SELECT: {sdp_error}")
        
        # Ensure table doesn't exist before creating (prevents LOCATION_ALREADY_EXISTS)
        # Drop table if it exists, including its location
        try:
            spark.sql(f"DROP TABLE IF EXISTS {view_name}")
            logger.debug(f"Dropped existing table '{view_name}' before creating new one")
        except Exception as drop_error:
            logger.debug(f"Could not drop existing table (may not exist): {drop_error}")
        
        # Also try to clean up any remaining directory if it exists
        # This handles cases where DROP TABLE didn't fully clean up
        _cleanup_table_directory(spark, view_name)
        
        # Use CREATE TABLE AS SELECT (standard Spark syntax)
        # This creates a managed table that persists data
        # Don't use IF NOT EXISTS here since we already dropped it
        create_table_sql = f"""
            CREATE TABLE {view_name} AS
            SELECT * FROM {temp_view_name}
        """
        
        logger.info(f"Creating table '{view_name}' using standard Spark syntax...")
        logger.debug(f"SQL: {create_table_sql}")
        
        spark.sql(create_table_sql)
        logger.info(f"✓ Table '{view_name}' created using standard Spark syntax (acts as Materialized View)")
    
    # Clean up temporary view
    spark.sql(f"DROP VIEW IF EXISTS {temp_view_name}")
    
    # Verify view/table creation if requested
    if verify:
        _verify_materialized_view(spark, view_name)
    
    # Return DataFrame of the created view
    return spark.table(view_name)


def create_materialized_view_from_sql(
    spark: SparkSession,
    sql_query: str,
    view_name: str,
    database: Optional[str] = None,
    if_not_exists: bool = True,
    verify: bool = True
) -> DataFrame:
    """
    Create a MATERIALIZED VIEW from a SQL query.
    
    This is useful when you have a SQL query and want to materialize its result.
    The SQL query should be a SELECT statement.
    
    Args:
        spark: SparkSession instance
        sql_query: SQL SELECT query to materialize
        view_name: Name of the Materialized View
        database: Database name (optional, uses current database if not provided)
        if_not_exists: If True, uses CREATE MATERIALIZED VIEW IF NOT EXISTS
        verify: If True, verifies that the view was created in the catalog
        
    Returns:
        DataFrame: DataFrame of the created Materialized View
        
    Example:
        >>> sql = "SELECT * FROM source_table WHERE status = 'active'"
        >>> mv_df = create_materialized_view_from_sql(spark, sql, "active_records")
    """
    # Ensure correct database
    if database:
        ensure_database(spark, database)
    
    # Check if table/view already exists and drop it if needed
    # This prevents LOCATION_ALREADY_EXISTS errors
    try:
        tables = spark.catalog.listTables()
        table_exists = any(t.name == view_name for t in tables)
        
        if table_exists:
            logger.info(f"Table/view '{view_name}' already exists, dropping it first...")
            try:
                # Try dropping as MATERIALIZED VIEW first
                spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
            except Exception:
                # If that fails, try dropping as TABLE
                spark.sql(f"DROP TABLE IF EXISTS {view_name}")
            logger.info(f"✓ Existing table/view '{view_name}' dropped")
            
            # Also clean up directory in case DROP didn't fully clean up
            _cleanup_table_directory(spark, view_name)
    except Exception as check_error:
        logger.debug(f"Could not check for existing table: {check_error}")
    
    # Try CREATE MATERIALIZED VIEW first (SDP syntax)
    # If that fails, fall back to CREATE TABLE AS SELECT (standard Spark)
    if_exists_clause = "IF NOT EXISTS" if if_not_exists else ""
    
    # First, try SDP syntax (CREATE MATERIALIZED VIEW)
    create_view_sql_sdp = f"""
        CREATE MATERIALIZED VIEW {if_exists_clause} {view_name} AS
        {sql_query}
    """
    
    logger.info(f"Attempting to create MATERIALIZED VIEW '{view_name}' using SDP syntax...")
    logger.debug(f"SQL: {create_view_sql_sdp}")
    
    try:
        # Try SDP syntax first
        spark.sql(create_view_sql_sdp)
        logger.info(f"✓ MATERIALIZED VIEW '{view_name}' created using SDP syntax")
    except Exception as sdp_error:
        # If SDP syntax fails, fall back to standard Spark CREATE TABLE AS SELECT
        logger.warning(f"SDP syntax not supported, falling back to CREATE TABLE AS SELECT: {sdp_error}")
        
        # Ensure table doesn't exist before creating (prevents LOCATION_ALREADY_EXISTS)
        # Drop table if it exists, including its location
        try:
            spark.sql(f"DROP TABLE IF EXISTS {view_name}")
            logger.debug(f"Dropped existing table '{view_name}' before creating new one")
        except Exception as drop_error:
            logger.debug(f"Could not drop existing table (may not exist): {drop_error}")
        
        # Also try to clean up any remaining directory if it exists
        # This handles cases where DROP TABLE didn't fully clean up
        _cleanup_table_directory(spark, view_name)
        
        # Use CREATE TABLE AS SELECT (standard Spark syntax)
        # This creates a managed table that persists data
        # Don't use IF NOT EXISTS here since we already dropped it
        create_table_sql = f"""
            CREATE TABLE {view_name} AS
            {sql_query}
        """
        
        logger.info(f"Creating table '{view_name}' using standard Spark syntax...")
        logger.debug(f"SQL: {create_table_sql}")
        
        spark.sql(create_table_sql)
        logger.info(f"✓ Table '{view_name}' created using standard Spark syntax (acts as Materialized View)")
    
    # Verify view/table creation if requested
    if verify:
        _verify_materialized_view(spark, view_name)
    
    # Return DataFrame of the created view
    return spark.table(view_name)


def create_materialized_view_from_file(
    spark: SparkSession,
    file_path: str,
    format_type: str = "csv",
    options: Optional[Dict[str, Any]] = None,
    view_name: Optional[str] = None,
    database: Optional[str] = None,
    if_not_exists: bool = True,
    verify: bool = True
) -> DataFrame:
    """
    Create a MATERIALIZED VIEW by reading from a file.
    
    This is useful for source ingestion from files (CSV, JSON, Parquet, etc.).
    The file is read into a DataFrame, then materialized as a view.
    
    Args:
        spark: SparkSession instance
        file_path: Path to the file or directory to read
        format_type: File format (csv, json, parquet, etc.)
        options: Dictionary of read options (e.g., {"header": "true", "inferSchema": "true"})
        view_name: Name of the Materialized View (defaults to file name if not provided)
        database: Database name (optional, uses current database if not provided)
        if_not_exists: If True, uses CREATE MATERIALIZED VIEW IF NOT EXISTS
        verify: If True, verifies that the view was created in the catalog
        
    Returns:
        DataFrame: DataFrame of the created Materialized View
        
    Example:
        >>> mv_df = create_materialized_view_from_file(
        ...     spark,
        ...     "lakehouse/stage/data.csv",
        ...     format_type="csv",
        ...     options={"header": "true", "inferSchema": "true"},
        ...     view_name="source_data"
        ... )
    """
    # Determine view name from file path if not provided
    if not view_name:
        view_name = Path(file_path).stem
    
    # Ensure correct database
    if database:
        ensure_database(spark, database)
    
    logger.info(f"Creating MATERIALIZED VIEW '{view_name}' from file: {file_path}")
    logger.info(f"Using format: {format_type}")
    logger.info(f"Options: {options or {}}")
    
    # Build reader with options
    reader = spark.read.format(format_type)
    
    # Apply all options
    if options:
        for key, value in options.items():
            reader = reader.option(key, value)
    
    # Load data into DataFrame
    temp_df = reader.load(file_path)
    
    logger.info(f"Data loaded. Schema: {temp_df.schema}")
    
    # Create Materialized View from DataFrame
    return create_materialized_view_from_dataframe(
        spark=spark,
        df=temp_df,
        view_name=view_name,
        database=database,
        if_not_exists=if_not_exists,
        verify=verify
    )


def _verify_materialized_view(spark: SparkSession, view_name: str) -> None:
    """
    Verify that a Materialized View was created in the catalog.
    
    Args:
        spark: SparkSession instance
        view_name: Name of the Materialized View to verify
    """
    try:
        tables = spark.catalog.listTables()
        view_found = any(t.name == view_name for t in tables)
        
        if view_found:
            logger.info(f"✓ View '{view_name}' found in catalog")
            for table in tables:
                if table.name == view_name:
                    logger.info(f"  Type: {table.tableType}")
                    logger.info(f"  Database: {table.database}")
                    break
        else:
            logger.warning(f"⚠ View '{view_name}' not found in catalog yet")
            logger.info("  The view may be created lazily when first accessed")
    except Exception as e:
        logger.warning(f"Could not verify view in catalog: {e}")


def execute_create_materialized_view_sql(
    spark: SparkSession,
    sql_statement: str,
    view_name: Optional[str] = None
) -> None:
    """
    Execute CREATE MATERIALIZED VIEW SQL with automatic fallback to CREATE TABLE.
    
    This function tries to execute the SQL statement as-is (assuming it's CREATE MATERIALIZED VIEW).
    If that fails, it automatically converts it to CREATE TABLE AS SELECT and retries.
    
    Args:
        spark: SparkSession instance
        sql_statement: SQL statement to execute (should be CREATE MATERIALIZED VIEW ...)
        view_name: Optional view name for logging (extracted from SQL if not provided)
        
    Example:
        >>> sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS my_view AS SELECT * FROM source"
        >>> execute_create_materialized_view_sql(spark, sql)
    """
    import re
    
    # Extract view name from SQL if not provided
    if not view_name:
        view_match = re.search(r'CREATE\s+(?:MATERIALIZED\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)', sql_statement, re.IGNORECASE)
        if view_match:
            view_name = view_match.group(1)
        else:
            view_name = "unknown"
    
    # Check if table/view already exists and drop it if needed
    # This prevents LOCATION_ALREADY_EXISTS errors
    try:
        tables = spark.catalog.listTables()
        table_exists = any(t.name == view_name for t in tables)
        
        if table_exists:
            logger.info(f"Table/view '{view_name}' already exists, dropping it first...")
            try:
                # Try dropping as MATERIALIZED VIEW first
                spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
            except Exception:
                # If that fails, try dropping as TABLE
                spark.sql(f"DROP TABLE IF EXISTS {view_name}")
            logger.info(f"✓ Existing table/view '{view_name}' dropped")
            
            # Also clean up directory in case DROP didn't fully clean up
            _cleanup_table_directory(spark, view_name)
    except Exception as check_error:
        logger.debug(f"Could not check for existing table: {check_error}")
    
    logger.info(f"Attempting to execute CREATE MATERIALIZED VIEW SQL for '{view_name}'...")
    logger.debug(f"SQL: {sql_statement}")
    
    try:
        # Try executing as-is (SDP syntax)
        spark.sql(sql_statement)
        logger.info(f"✓ MATERIALIZED VIEW '{view_name}' created using SDP syntax")
    except Exception as sdp_error:
        # If SDP syntax fails, convert to CREATE TABLE AS SELECT
        logger.warning(f"SDP syntax not supported, converting to CREATE TABLE AS SELECT: {sdp_error}")
        
        # Ensure table doesn't exist before creating (prevents LOCATION_ALREADY_EXISTS)
        # Drop table if it exists, including its location
        try:
            spark.sql(f"DROP TABLE IF EXISTS {view_name}")
            logger.debug(f"Dropped existing table '{view_name}' before creating new one")
        except Exception as drop_error:
            logger.debug(f"Could not drop existing table (may not exist): {drop_error}")
        
        # Also try to clean up any remaining directory if it exists
        # This handles cases where DROP TABLE didn't fully clean up
        _cleanup_table_directory(spark, view_name)
        
        # Extract the SELECT part from the SQL
        # Pattern: CREATE MATERIALIZED VIEW ... AS <SELECT>
        # Try multiple patterns to extract SELECT query
        select_match = None
        
        # Pattern 1: AS SELECT ... (most common)
        select_match = re.search(r'AS\s+(SELECT.*)', sql_statement, re.IGNORECASE | re.DOTALL)
        
        # Pattern 2: Just SELECT ... (if AS is missing or in different position)
        if not select_match:
            select_match = re.search(r'(SELECT.*)', sql_statement, re.IGNORECASE | re.DOTALL)
        
        if select_match:
            select_query = select_match.group(1).strip()
            
            # Remove trailing semicolon if present
            select_query = select_query.rstrip(';').strip()
            
            # Build CREATE TABLE AS SELECT (don't use IF NOT EXISTS since we dropped it)
            create_table_sql = f"CREATE TABLE {view_name} AS {select_query}"
            
            logger.info(f"Creating table '{view_name}' using standard Spark syntax...")
            logger.debug(f"SQL: {create_table_sql}")
            
            try:
                spark.sql(create_table_sql)
                logger.info(f"✓ Table '{view_name}' created using standard Spark syntax (acts as Materialized View)")
            except Exception as table_error:
                logger.error(f"Failed to create table '{view_name}': {table_error}")
                raise
        else:
            # If we can't extract SELECT, try to replace MATERIALIZED VIEW with TABLE
            # This handles cases where the SQL structure is different
            create_table_sql = re.sub(
                r'CREATE\s+MATERIALIZED\s+VIEW',
                'CREATE TABLE',
                sql_statement,
                flags=re.IGNORECASE
            )
            
            logger.info(f"Creating table '{view_name}' using standard Spark syntax (converted from MATERIALIZED VIEW)...")
            logger.debug(f"SQL: {create_table_sql}")
            
            try:
                spark.sql(create_table_sql)
                logger.info(f"✓ Table '{view_name}' created using standard Spark syntax (acts as Materialized View)")
            except Exception as table_error:
                logger.error(f"Failed to create table '{view_name}' with converted SQL: {table_error}")
                raise


def drop_materialized_view(
    spark: SparkSession,
    view_name: str,
    if_exists: bool = True
) -> None:
    """
    Drop a Materialized View or Table.
    
    Args:
        spark: SparkSession instance
        view_name: Name of the Materialized View/Table to drop
        if_exists: If True, uses DROP IF EXISTS
        
    Example:
        >>> drop_materialized_view(spark, "old_view")
    """
    if_exists_clause = "IF EXISTS" if if_exists else ""
    
    # Try DROP MATERIALIZED VIEW first, then fall back to DROP TABLE
    try:
        drop_sql = f"DROP MATERIALIZED VIEW {if_exists_clause} {view_name}"
        logger.info(f"Dropping MATERIALIZED VIEW '{view_name}'...")
        spark.sql(drop_sql)
        logger.info(f"✓ MATERIALIZED VIEW '{view_name}' dropped successfully")
    except Exception:
        # Fall back to DROP TABLE
        drop_sql = f"DROP TABLE {if_exists_clause} {view_name}"
        logger.info(f"Dropping table '{view_name}' (was created as table instead of view)...")
        spark.sql(drop_sql)
        logger.info(f"✓ Table '{view_name}' dropped successfully")

