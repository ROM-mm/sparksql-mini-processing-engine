"""
Module for managing Spark catalog and database configuration.
"""
import logging
from typing import Optional
from pyspark.sql import SparkSession
from src.modules.config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class CatalogManager:
    """Manages Spark catalog and database configuration."""
    
    def __init__(self, config_loader: ConfigLoader, logger_instance: Optional[logging.Logger] = None):
        """
        Initialize CatalogManager.
        
        Args:
            config_loader: ConfigLoader instance
            logger_instance: Logger instance (optional, uses module logger if not provided)
        """
        self.config_loader = config_loader
        self.log = logger_instance or logger
    
    def ensure_catalog_database(self, spark: SparkSession) -> None:
        """
        Ensure that the correct catalog and database are used for all MATERIALIZED VIEWs.
        This guarantees that all views are saved to the catalog and persist between sessions.
        
        Args:
            spark: SparkSession instance
        """
        try:
            # Load catalog and database configuration from pipeline.yml
            global_config = self.config_loader.load_global_config()
            catalog = global_config.get('catalog', 'spark_catalog')
            database = global_config.get('database', 'default')
            
            self.log.info("=" * 80)
            self.log.info("CATALOG AND DATABASE CONFIGURATION")
            self.log.info("=" * 80)
            self.log.info(f"Catalog: {catalog}")
            self.log.info(f"Database: {database}")
            
            # Set the database if it's not 'default'
            if database != 'default':
                try:
                    # Check if database exists, create if not
                    try:
                        spark.sql(f"DESCRIBE DATABASE {database}")
                        self.log.info(f"Database '{database}' already exists")
                    except Exception:
                        # Database doesn't exist, create it
                        self.log.info(f"Creating database '{database}'...")
                        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
                        self.log.info(f"Database '{database}' created successfully")
                    
                    # Use the database
                    spark.sql(f"USE {database}")
                    self.log.info(f"✓ Using database: {database}")
                except Exception as e:
                    self.log.warning(f"Could not switch to database '{database}': {e}")
                    self.log.info("Using default database")
            else:
                self.log.info("Using default database")
            
            # Verify current database
            try:
                current_db = spark.sql("SELECT current_database()").collect()[0][0]
                self.log.info(f"Current database: {current_db}")
            except Exception as e:
                self.log.debug(f"Could not get current database: {e}")
            
            self.log.info("=" * 80)
            self.log.info("All MATERIALIZED VIEWs will be saved to catalog for persistence between sessions")
            self.log.info(f"Catalog path: {catalog}.{database}.<view_name>")
            self.log.info("=" * 80)
            
        except Exception as e:
            self.log.warning(f"Error ensuring catalog/database configuration: {e}")
            self.log.info("Continuing with default catalog and database")

