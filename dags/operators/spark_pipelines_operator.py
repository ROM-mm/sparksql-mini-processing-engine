"""
Airflow operator to execute pipelines using Spark Declarative Pipelines (SDP).
This operator uses spark-pipelines CLI or SDP API to execute the complete pipeline.
"""
import os
import subprocess
from pathlib import Path
from typing import Optional, Dict
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class SparkPipelinesOperator(BaseOperator):
    """
    Operator that executes pipeline using Spark Declarative Pipelines (SDP).
    
    This operator uses spark-pipelines CLI to execute the complete pipeline,
    allowing SDP to automatically manage:
    - Execution order based on dependencies
    - Materialized View persistence
    - Cache and optimizations
    
    Args:
        pipeline_spec_path: Path to pipeline.yml file
        spark_config: Dictionary with additional Spark configurations
        use_cli: If True, uses spark-pipelines CLI; if False, tries to use SDP API
        task_id: Task ID (inherited from BaseOperator)
        **kwargs: Additional BaseOperator arguments
    """
    
    template_fields = ('pipeline_spec_path',)
    
    @apply_defaults
    def __init__(
        self,
        pipeline_spec_path: str = "pipeline.yml",
        spark_config: Optional[Dict] = None,
        use_cli: bool = True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.pipeline_spec_path = pipeline_spec_path
        self.spark_config = spark_config or {}
        self.use_cli = use_cli
    
    def _find_project_root(self) -> Path:
        """Finds project root directory."""
        # If operator is in dags/operators/, go up 2 levels
        # If it's in airflow_home/dags/operators/, go up 3 levels
        current_file = Path(__file__).resolve()
        
        if 'airflow_home' in str(current_file):
            project_root = current_file.parent.parent.parent.parent
        else:
            project_root = current_file.parent.parent.parent
        
        return project_root
    
    def _execute_with_cli(self, project_root: Path, pipeline_path: Path) -> None:
        """
        Executes pipeline using spark-pipelines CLI.
        
        Args:
            project_root: Project root directory
            pipeline_path: Path to pipeline.yml
        """
        # Check if spark-pipelines is available
        try:
            result = subprocess.run(
                ["spark-pipelines", "--version"],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode != 0:
                raise FileNotFoundError("spark-pipelines not found")
            self.log.info(f"spark-pipelines found: {result.stdout.strip()}")
        except (FileNotFoundError, subprocess.TimeoutExpired) as e:
            error_msg = (
                "spark-pipelines CLI is not available. "
                "Install PySpark 4.1.0+ with pipelines support: "
                "pip install 'pyspark[pipelines]>=4.1.0'"
            )
            self.log.error(error_msg)
            raise AirflowException(error_msg) from e
        
        # Build command
        cmd = ["spark-pipelines", "run", "--spec", str(pipeline_path)]
        
        # Add Spark configurations if provided
        if self.spark_config:
            for key, value in self.spark_config.items():
                cmd.extend(["--conf", f"{key}={value}"])
        
        self.log.info(f"Executing: {' '.join(cmd)}")
        self.log.info(f"Working directory: {project_root}")
        
        # Execute command
        try:
            result = subprocess.run(
                cmd,
                cwd=str(project_root),
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            # Log output
            if result.stdout:
                self.log.info("spark-pipelines output:")
                for line in result.stdout.split('\n'):
                    if line.strip():
                        self.log.info(f"   {line}")
            
            if result.stderr:
                self.log.warning("Errors/Warnings from spark-pipelines:")
                for line in result.stderr.split('\n'):
                    if line.strip():
                        self.log.warning(f"   {line}")
            
            if result.returncode != 0:
                error_msg = (
                    f"SDP Pipeline failed with code {result.returncode}. "
                    f"Error: {result.stderr[:500]}"
                )
                self.log.error(error_msg)
                raise AirflowException(error_msg)
            
            self.log.info("SDP Pipeline executed successfully!")
            
        except subprocess.TimeoutExpired:
            error_msg = "SDP Pipeline exceeded 1 hour timeout"
            self.log.error(error_msg)
            raise AirflowException(error_msg)
        except Exception as e:
            error_msg = f"Error executing spark-pipelines: {e}"
            self.log.error(error_msg)
            raise AirflowException(error_msg) from e
    
    def _execute_with_api(self, project_root: Path, pipeline_path: Path) -> None:
        """
        Executes pipeline using SDP API via PySpark.
        
        This is an alternative when CLI is not available.
        Note: This implementation is basic and may need adjustments.
        
        Args:
            project_root: Project root directory
            pipeline_path: Path to pipeline.yml
        """
        self.log.warning(
            "Executing via SDP API (basic implementation). "
            "It is recommended to use spark-pipelines CLI for full functionality."
        )
        
        try:
            from pyspark.sql import SparkSession
            import yaml
            
            # Load pipeline configuration
            with open(pipeline_path, 'r') as f:
                pipeline_config = yaml.safe_load(f)
            
            # Create SparkSession with SDP extension
            builder = SparkSession.builder.appName(f"airflow_{self.task_id}")
            
            # Configure SDP extension
            builder = builder.config(
                "spark.sql.extensions",
                "org.apache.spark.sql.pipelines.PipelinesSessionExtension"
            )
            
            # Apply pipeline configurations
            spark_config = pipeline_config.get('configuration', {})
            for key, value in spark_config.items():
                builder = builder.config(key, str(value))
            
            # Apply additional configurations
            for key, value in self.spark_config.items():
                builder = builder.config(key, str(value))
            
            spark = builder.getOrCreate()
            self.log.info("SparkSession created with SDP extension")
            
            # Load and execute SQL files
            definitions = pipeline_config.get('definitions', [])
            sql_files = []
            
            for definition in definitions:
                if 'glob' in definition:
                    glob_pattern = definition['glob'].get('include', '')
                    # Convert glob pattern to path
                    if 'src/sql' in glob_pattern:
                        sql_base = project_root / "src" / "sql"
                        sql_files.extend(sql_base.glob("*.sql"))
            
            # Remove duplicates and sort
            sql_files = sorted(set(sql_files))
            
            self.log.info(f"Found {len(sql_files)} SQL files")
            
            # Execute SQL files
            # Note: In a complete SDP implementation, the framework
            # would automatically manage order and dependencies
            for sql_file in sql_files:
                self.log.info(f"Executing: {sql_file.name}")
                with open(sql_file, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                
                # Split into statements
                statements = [
                    s.strip() 
                    for s in sql_content.split(';') 
                    if s.strip() and not s.strip().startswith('--')
                ]
                
                for statement in statements:
                    if statement:
                        try:
                            spark.sql(statement)
                            self.log.info(f"Statement executed: {sql_file.name}")
                        except Exception as e:
                            self.log.error(f"Error executing statement in {sql_file.name}: {e}")
                            raise
            
            self.log.info("Pipeline executed via SDP API")
            
        except ImportError as e:
            error_msg = (
                "PySpark is not installed or does not have SDP support. "
                "Install: pip install 'pyspark[pipelines]>=4.1.0'"
            )
            self.log.error(error_msg)
            raise AirflowException(error_msg) from e
        except Exception as e:
            error_msg = f"Error executing pipeline via SDP API: {e}"
            self.log.error(error_msg)
            raise AirflowException(error_msg) from e
        finally:
            if 'spark' in locals():
                spark.stop()
    
    def execute(self, context):
        """
        Executes SDP pipeline.
        
        Args:
            context: Airflow context
        """
        # Find project root directory
        project_root = self._find_project_root()
        
        # Resolve pipeline.yml path
        pipeline_path = Path(self.pipeline_spec_path)
        if not pipeline_path.is_absolute():
            pipeline_path = project_root / pipeline_path
        
        if not pipeline_path.exists():
            raise AirflowException(f"pipeline.yml file not found: {pipeline_path}")
        
        self.log.info(f"Pipeline spec: {pipeline_path}")
        self.log.info(f"Project root: {project_root}")
        
        # Execute using CLI or API
        if self.use_cli:
            try:
                self._execute_with_cli(project_root, pipeline_path)
            except AirflowException:
                # If CLI fails, try API as fallback
                self.log.warning("CLI failed, trying SDP API as fallback...")
                self._execute_with_api(project_root, pipeline_path)
        else:
            self._execute_with_api(project_root, pipeline_path)
