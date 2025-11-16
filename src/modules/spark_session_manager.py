"""
Module for managing SparkSession creation and configuration.
"""
import os
import logging
from pathlib import Path
from typing import Optional, Dict
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class SparkSessionManager:
    """Manages SparkSession creation and configuration."""
    
    def __init__(self, spark_config: Optional[Dict] = None, logger_instance: Optional[logging.Logger] = None):
        """
        Initialize SparkSessionManager.
        
        Args:
            spark_config: Dictionary with Spark configurations
            logger_instance: Logger instance (optional, uses module logger if not provided)
        """
        self.spark_config = spark_config or {}
        self.log = logger_instance or logger
    
    def setup_environment(self):
        """
        Configure Spark environment for PySpark installed via pip.
        Remove invalid SPARK_HOME if it exists.
        """
        # For PySpark installed via pip, we don't need to configure SPARK_HOME
        # PySpark manages this automatically
        # Just ensure there are no conflicting configurations
        if "SPARK_HOME" in os.environ:
            spark_home = os.environ["SPARK_HOME"]
            # Check if SPARK_HOME points to a valid directory
            if not Path(spark_home).exists():
                self.log.warning(f"SPARK_HOME configured but directory does not exist: {spark_home}")
                self.log.info("Removing SPARK_HOME to use PySpark default configuration")
                del os.environ["SPARK_HOME"]
        
        # Try to use findspark if available (facilitates configuration)
        try:
            import findspark
            findspark.init()
            self.log.info("findspark detected and initialized")
        except ImportError:
            # findspark is not installed, but it's not required
            # For PySpark via pip, we don't need to configure SPARK_HOME
            pass
    
    def create_session(self, context: Optional[Dict] = None) -> SparkSession:
        """
        Create SparkSession with appropriate configuration.
        
        Args:
            context: Airflow context (optional, for app name)
            
        Returns:
            SparkSession instance
            
        Raises:
            RuntimeError: If SparkSession cannot be created after all attempts
        """
        # Check if PySpark is available
        try:
            import pyspark
            self.log.info(f"PySpark detected - Version: {pyspark.__version__}")
        except ImportError:
            error_msg = (
                "PySpark is not installed in the Airflow environment! "
                "Make sure PySpark is installed in the same venv where Airflow is running. "
                "Run: pip install pyspark==4.1.0.dev3"
            )
            self.log.error(error_msg)
            raise ImportError(error_msg)
        
        def _create_builder_with_config(master=None, extra_configs=None):
            """Helper to create a new builder with configurations."""
            # Use fixed appName for all tasks to try to share the same session
            dag_id = context.get('dag').dag_id if context and context.get('dag') else 'default'
            app_name = f"airflow_{dag_id}"
            
            builder = SparkSession.builder.appName(app_name)
            
            if master:
                builder = builder.master(master)
            
            # Apply custom configurations
            for key, value in self.spark_config.items():
                builder = builder.config(key, value)
            
            # Apply extra configurations
            if extra_configs:
                for key, value in extra_configs.items():
                    builder = builder.config(key, value)
            
            # Add pipelines extension if necessary
            if 'spark.sql.extensions' not in self.spark_config:
                builder = builder.config(
                    'spark.sql.extensions',
                    'org.apache.spark.sql.pipelines.PipelinesSessionExtension'
                )
            
            return builder
        
        # Try to get existing active session first (may not work between processes)
        try:
            active_session = SparkSession.getActiveSession()
            if active_session:
                self.log.info("Reusing existing active SparkSession")
                return active_session
            else:
                raise ValueError("No active session")
        except (ValueError, AttributeError):
            # No active session, create new one
            # Each Airflow task creates its own session
            self.log.info("Creating new SparkSession (each Airflow task runs in a separate process)")
            
            # Attempt 1: Default configuration (recommended for pip)
            try:
                self.log.info("Attempt 1: Creating SparkSession with default configuration...")
                spark = _create_builder_with_config().getOrCreate()
                self.log.info("SparkSession created successfully (default configuration)")
                return spark
            except Exception as e1:
                last_error = e1
                self.log.warning(f"Attempt 1 failed: {type(e1).__name__}: {str(e1)[:300]}")
            
            # Attempt 2: With master local[*]
            try:
                self.log.info("\nAttempt 2: Creating SparkSession with master local[*]...")
                spark = _create_builder_with_config(master="local[*]").getOrCreate()
                self.log.info("SparkSession created successfully (master local[*])")
                return spark
            except Exception as e2:
                last_error = e2
                self.log.warning(f"Attempt 2 failed: {type(e2).__name__}: {str(e2)[:300]}")
                
                # Attempt 3: With master local
                try:
                    self.log.info("\nAttempt 3: Creating SparkSession with master local...")
                    spark = _create_builder_with_config(master="local").getOrCreate()
                    self.log.info("SparkSession created successfully (master local)")
                    return spark
                except Exception as e3:
                    last_error = e3
                    self.log.warning(f"Attempt 3 failed: {type(e3).__name__}: {str(e3)[:300]}")
                    
                    # Attempt 4: With additional configurations
                    try:
                        self.log.info("\nAttempt 4: Creating SparkSession with additional configurations...")
                        extra_configs = {
                            "spark.master": "local[*]",
                            "spark.driver.host": "localhost",
                            "spark.driver.bindAddress": "127.0.0.1"
                        }
                        spark = _create_builder_with_config(extra_configs=extra_configs).getOrCreate()
                        self.log.info("SparkSession created successfully (additional configurations)")
                        return spark
                    except Exception as e4:
                        last_error = e4
                        self.log.error("\n" + "=" * 80)
                        self.log.error("ALL ATTEMPTS TO CREATE SPARKSESSION FAILED")
                        self.log.error("=" * 80)
                        self.log.error(f"Last error: {type(e4).__name__}: {str(e4)}")
                        self.log.error("\nTips to resolve:")
                        self.log.error("  1. Check if Java is installed: java -version")
                        self.log.error("  2. Check if JAVA_HOME is configured correctly")
                        self.log.error("  3. Check if PySpark is installed: pip list | grep pyspark")
                        self.log.error("=" * 80)
                        raise RuntimeError(
                            f"Could not create SparkSession after all attempts. "
                            f"Last error: {e4}. "
                            f"Check if Java is installed and accessible (java -version)."
                        ) from e4
        
        raise RuntimeError("Could not create SparkSession after all attempts")
    
    def log_session_info(self, spark: SparkSession):
        """
        Log SparkSession information.
        
        Args:
            spark: SparkSession instance
        """
        try:
            self.log.info("=" * 80)
            self.log.info("SPARKSESSION INFORMATION")
            self.log.info("=" * 80)
            self.log.info(f"Spark Version: {spark.version}")
            self.log.info(f"Spark App Name: {spark.sparkContext.appName}")
            self.log.info(f"Spark Master: {spark.sparkContext.master}")
            self.log.info(f"Spark Default Parallelism: {spark.sparkContext.defaultParallelism}")
            
            # Log important configurations
            self.log.info("\nSPARK CONFIGURATIONS:")
            important_configs = [
                'spark.sql.extensions',
                'spark.sql.warehouse.dir',
                'spark.sql.sources.default',
                'spark.sql.shuffle.partitions',
                'spark.sql.adaptive.enabled'
            ]
            for config_key in important_configs:
                try:
                    value = spark.conf.get(config_key, 'not configured')
                    self.log.info(f"  {config_key}: {value}")
                except:
                    pass
            
            self.log.info("=" * 80)
        except Exception as e:
            self.log.warning(f"Could not get SparkSession information: {e}")

