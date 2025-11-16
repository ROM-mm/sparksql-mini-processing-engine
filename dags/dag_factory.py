"""
DAG Factory to create dynamic DAGs based on SQL files.
Similar to dbt concept, where order and dependencies are automatically detected.
"""
import re
import sys
import os
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Configure logging
logger = logging.getLogger(__name__)

# Configure PYTHONPATH before importing project modules
# Determine project root path
dag_file = Path(__file__).resolve()
if 'airflow_home' in str(dag_file):
    # File is in airflow_home/dags/
    project_root = dag_file.parent.parent.parent
else:
    # File is in dags/ (development)
    project_root = dag_file.parent.parent

# Add to PYTHONPATH
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Also configure environment variable to ensure
os.environ['PYTHONPATH'] = f"{project_root}:{os.environ.get('PYTHONPATH', '')}"

from src.modules.config_loader import ConfigLoader
from src.modules.sql_template_processor import SQLTemplateProcessor, build_ref_map_from_sql_files
from dags.operators.spark_sql_operator import SparkSQLFileOperator


class SQLDependencyParser:
    """Parser to detect dependencies in SQL files."""
    
    # Patterns to detect dependencies in SQL
    FROM_PATTERN = re.compile(r'FROM\s+(\w+)', re.IGNORECASE)
    JOIN_PATTERN = re.compile(r'JOIN\s+(\w+)', re.IGNORECASE)
    CREATE_VIEW_PATTERN = re.compile(r'CREATE\s+(?:TEMPORARY\s+)?(?:MATERIALIZED\s+)?VIEW\s+(\w+)', re.IGNORECASE)
    
    # Pattern to detect {{ ref('name') }} dbt style
    REF_PATTERN = re.compile(r'\{\{\s*ref\s*\(\s*[\'"](\w+)[\'"]\s*\)\s*\}\}', re.IGNORECASE)
    
    def __init__(self):
        """Initialize parser with template processor."""
        self.template_processor = SQLTemplateProcessor()
    
    @staticmethod
    def extract_view_name(sql_content: str) -> Optional[str]:
        """Extracts the name of the view created in SQL."""
        match = SQLDependencyParser.CREATE_VIEW_PATTERN.search(sql_content)
        if match:
            return match.group(1)
        return None
    
    def extract_dependencies(self, sql_content: str) -> List[str]:
        """
        Extracts dependencies (tables/views used) from SQL.
        Prioritizes {{ ref('name') }} over FROM/JOIN.
        """
        dependencies = set()
        
        # FIRST: Detect {{ ref('name') }} (dbt style) - HIGH PRIORITY
        refs = self.template_processor.extract_refs(sql_content)
        for ref_name in refs:
            dependencies.add(ref_name)
        
        # SECOND: Detect FROM and JOIN (fallback for traditional SQL)
        # But ignore if already detected via ref()
        for match in SQLDependencyParser.FROM_PATTERN.finditer(sql_content):
            table = match.group(1).strip()
            if table and not table.startswith('('):  # Ignore subqueries
                # Ignore if it's a known file format
                if table.lower() not in ['csv', 'parquet', 'json', 'orc', 'delta']:
                    dependencies.add(table)
        
        for match in SQLDependencyParser.JOIN_PATTERN.finditer(sql_content):
            table = match.group(1).strip()
            if table:
                dependencies.add(table)
        
        return sorted(list(dependencies))


class DynamicDAGFactory:
    """Factory to create dynamic DAGs based on SQL files."""
    
    def __init__(self, project_root: Path, config_loader: ConfigLoader):
        self.project_root = project_root
        self.config_loader = config_loader
        self.sql_base_path = project_root / "src" / "sql"
        self.spark_config = config_loader.get_spark_config()
        self.parser = SQLDependencyParser()
        self.template_processor = SQLTemplateProcessor()
    
    def discover_sql_files(self) -> List[Path]:
        """Discovers all SQL files in directory and subdirectories."""
        if not self.sql_base_path.exists():
            return []
        
        # Search recursively in all subfolders
        sql_files = sorted(self.sql_base_path.rglob("*.sql"))
        return sql_files
    
    def parse_sql_file(self, sql_file: Path) -> Dict[str, any]:
        """
        Parse a SQL file and extract information.
        Detects {{ ref('name') }} for dbt-style dependencies.
        """
        with open(sql_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Task name based on file name (without extension)
        task_name = sql_file.stem
        
        # IMPORTANTE: Como garantimos que a view sempre tem o nome do arquivo,
        # sempre usar task_name como view_name
        # Tentar extrair do SQL apenas para compatibilidade, mas usar task_name como padrão
        extracted_view_name = self.parser.extract_view_name(content)
        view_name = extracted_view_name if extracted_view_name else task_name
        
        # Se o view_name extraído for diferente do task_name, usar task_name (garantia)
        # Isso garante consistência já que removemos CREATE VIEW do SQL
        if view_name != task_name:
            logger.debug(f"File {sql_file.name}: extracted view_name '{extracted_view_name}' != task_name '{task_name}', using task_name")
            view_name = task_name
        
        # Extract dependencies (prioritizes {{ ref() }} over FROM/JOIN)
        dependencies = self.parser.extract_dependencies(content)
        
        # Extract refs explicitly (for later use)
        refs = self.template_processor.extract_refs(content)
        
        return {
            'file_path': sql_file,
            'task_name': task_name,
            'view_name': view_name,  # Sempre igual a task_name
            'dependencies': dependencies,
            'refs': refs,  # {{ ref() }} references found
            'content': content,
            'has_templates': len(refs) > 0  # Flag indicating if uses templates
        }
    
    def build_dependency_graph(self, sql_files_info: List[Dict]) -> Dict[str, List[str]]:
        """Builds dependency graph based on views created and used."""
        # Map view_name -> task_name
        view_to_task = {}
        # Also map task_name -> view_name for reverse lookup
        task_to_view = {}
        
        for info in sql_files_info:
            if info['view_name']:
                view_to_task[info['view_name']] = info['task_name']
                task_to_view[info['task_name']] = info['view_name']
        
        # Build dependency graph
        dependency_graph = {}
        for info in sql_files_info:
            task_name = info['task_name']
            task_deps = []
            
            for dep in info['dependencies']:
                # If dependency is a view created by another SQL file
                if dep in view_to_task:
                    dep_task = view_to_task[dep]
                    # Avoid circular dependencies
                    if dep_task != task_name:
                        task_deps.append(dep_task)
            
            dependency_graph[task_name] = task_deps
        
        return dependency_graph
    
    def get_execution_order_from_config(self) -> List[str]:
        """Gets execution order from pipeline.yml."""
        config = self.config_loader.load_pipeline_config()
        execution_order = config.get('execution_order', [])
        return execution_order
    
    def group_tasks_by_stage(self, sql_files_info: List[Dict]) -> Dict[str, List[Dict]]:
        """Groups tasks by stage based on folder structure (source, transformation, export)."""
        groups = {}
        
        for info in sql_files_info:
            file_path = info['file_path']
            
            # Determine group based on folder where file is located
            # Example: src/sql/source/file.sql -> 'source'
            #          src/sql/transformation/file.sql -> 'transformation'
            #          src/sql/export/file.sql -> 'export'
            
            # Get relative path to sql_base_path
            try:
                relative_path = file_path.relative_to(self.sql_base_path)
                # If file is in a subfolder, use subfolder name
                if len(relative_path.parts) > 1:
                    # First part is folder name (source, transformation, export)
                    folder_name = relative_path.parts[0]
                    # Normalize folder name to ensure consistency
                    if folder_name in ['source', 'transformation', 'export']:
                        group = folder_name
                    else:
                        # If folder is not one of expected, use it as group anyway
                        group = folder_name
                else:
                    # File is in root of src/sql, use default group
                    group = 'transformation'  # Default for files in root
            except ValueError:
                # If can't get relative path, use name analysis as fallback
                task_name = info['task_name']
                task_lower = task_name.lower()
                
                if task_name.startswith('00_') or 'source' in task_lower:
                    group = 'source'
                elif 'final' in task_lower or 'export' in task_lower:
                    group = 'export'
                else:
                    group = 'transformation'
            
            if group not in groups:
                groups[group] = []
            groups[group].append(info)
        
        return groups
    
    def create_dynamic_dag(
        self,
        dag_id: str,
        description: str,
        schedule_interval,
        start_date,
        default_args: Dict,
        tags: List[str] = None,
        use_dependency_detection: bool = True
    ) -> DAG:
        """
        Creates a dynamic DAG based on found SQL files.
        
        Args:
            dag_id: DAG ID
            description: DAG description
            schedule_interval: Schedule interval
            start_date: Start date
            default_args: Default DAG arguments
            tags: DAG tags
            use_dependency_detection: If True, uses dependency detection, 
                                    otherwise uses only YAML order
        """
        # Create DAG
        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            description=description,
            schedule_interval=schedule_interval,
            start_date=start_date,
            catchup=False,
            tags=tags or [],
        )
        
        # Discover SQL files
        sql_files = self.discover_sql_files()
        if not sql_files:
            raise ValueError(f"No SQL files found in {self.sql_base_path}")
        
        # Parse all SQL files
        sql_files_info = [self.parse_sql_file(f) for f in sql_files]
        
        # Get execution order from config (optional, only used if dependency detection is disabled)
        execution_order = self.get_execution_order_from_config()
        
        # Build dependency graph if enabled
        # This detects dependencies automatically from {{ ref() }} in SQL files
        dependency_graph = {}
        if use_dependency_detection:
            dependency_graph = self.build_dependency_graph(sql_files_info)
            logger.info(f"Built dependency graph with {len(dependency_graph)} tasks")
            if execution_order:
                logger.info(
                    f"Note: execution_order in pipeline.yml is ignored when use_dependency_detection=True. "
                    f"Dependencies are detected automatically from {{ ref() }} in SQL files."
                )
        elif execution_order:
            # If dependency detection is disabled, use execution_order to sort tasks
            task_order = {}
            for idx, task_name in enumerate(execution_order):
                task_order[task_name] = idx
            
            sql_files_info.sort(key=lambda x: task_order.get(x['task_name'], 999))
            logger.info(f"Using execution_order from pipeline.yml (dependency detection disabled)")
        
        # Build ref mapping for templates (dbt style)
        # Maps ref_name -> view_name for substitution
        ref_map = build_ref_map_from_sql_files(sql_files_info)
        
        # Add source_table to ref_map (created by Python module, not a SQL file)
        # This ensures {{ ref('source_table') }} works correctly in source SQL files
        try:
            import yaml
            project_root = Path(__file__).parent.parent
            pipeline_path = project_root / "pipeline.yml"
            if pipeline_path.exists():
                with open(pipeline_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f) or {}
                    source_config = config.get('source', {})
                    source_view_name = source_config.get('view_name', 'source_table')
                    ref_map['source_table'] = source_view_name
                    logger.info(f"Added source_table to ref_map: ref('source_table') -> '{source_view_name}'")
        except Exception as e:
            # If can't load config, use default
            ref_map['source_table'] = 'source_table'
            logger.debug(f"Could not load source config, using default: {e}")
        
        # Log ref_map para debug
        logger.info(f"Built ref_map with {len(ref_map)} entries:")
        for ref_name, view_name in ref_map.items():
            logger.info(f"  ref('{ref_name}') -> '{view_name}'")
        
        # Create view_to_task_map for template processor
        # IMPORTANTE: Como garantimos que view_name = task_name, usar task_name sempre
        view_to_task_map = {}
        for info in sql_files_info:
            task_name = info['task_name']
            # A view sempre tem o nome do arquivo (task_name)
            view_to_task_map[task_name] = task_name
            
            # Se view_name existe e é diferente, também mapear (para compatibilidade)
            view_name = info.get('view_name')
            if view_name and view_name != task_name:
                view_to_task_map[view_name] = task_name
        
        # Update template processor with mapping
        self.template_processor.view_to_task_map = view_to_task_map
        
        # Group tasks by stage
        task_groups = self.group_tasks_by_stage(sql_files_info)
        
        # Create initial validation task (checks if SQL files use {{ ref() }})
        def validate_sql_refs(**context):
            """Validates if all SQL files (except source) use {{ ref() }}."""
            import logging
            log = logging.getLogger(__name__)
            
            errors = []
            warnings = []
            
            for info in sql_files_info:
                file_path = info['file_path']
                task_name = info['task_name']
                has_refs = info.get('has_templates', False)
                refs = info.get('refs', [])
                
                # Determine if it's a source file
                is_source = False
                try:
                    relative_path = file_path.relative_to(self.sql_base_path)
                    if len(relative_path.parts) > 1:
                        folder_name = relative_path.parts[0]
                        is_source = (folder_name == 'source')
                except ValueError:
                    # If can't determine, check by name
                    is_source = task_name.startswith('00_') or 'source' in task_name.lower()
                
                # Source files don't need refs (they are the source)
                if is_source:
                    continue
                
                # Check if has refs
                if not has_refs or len(refs) == 0:
                    error_msg = (
                        f"SQL file does not use {{ ref() }}: {file_path.relative_to(self.project_root)}\n"
                        f"   Task: {task_name}\n"
                        f"   Transformation/export files must use {{ ref('view_name') }} to reference other views."
                    )
                    errors.append(error_msg)
            
            # Log results
            log.info("=" * 80)
            log.info("INITIAL VALIDATION: Checking {{ ref() }} in SQL files")
            log.info("=" * 80)
            log.info(f"\nTotal files analyzed: {len(sql_files_info)}")
            
            if errors:
                log.error(f"\nERRORS FOUND: {len(errors)} file(s) without {{ ref() }}")
                log.error("\n" + "=" * 80)
                for error in errors:
                    log.error(error)
                log.error("=" * 80)
                log.error("\nVALIDATION FAILED: All next steps will be skipped.")
                raise ValueError(
                    f"Validation failed: {len(errors)} SQL file(s) do not use {{ ref() }}. "
                    f"Transformation/export files must use {{ ref('view_name') }}."
                )
            else:
                log.info("\nVALIDATION PASSED: All SQL files use {{ ref() }} correctly!")
                log.info("\nSummary:")
                for info in sql_files_info:
                    file_path = info['file_path']
                    task_name = info['task_name']
                    refs = info.get('refs', [])
                    
                    # Determine if it's a source file
                    is_source = False
                    try:
                        relative_path = file_path.relative_to(self.sql_base_path)
                        if len(relative_path.parts) > 1:
                            folder_name = relative_path.parts[0]
                            is_source = (folder_name == 'source')
                    except ValueError:
                        is_source = task_name.startswith('00_') or 'source' in task_name.lower()
                    
                    if is_source:
                        log.info(f"  {task_name} (source - does not need ref)")
                    elif refs:
                        log.info(f"  {task_name} - uses {{ ref() }}: {refs}")
            
            log.info("\n" + "=" * 80)
            return "Validation passed successfully!"
        
        # Create initial validation task
        validate_refs_task = PythonOperator(
            task_id='validate_sql_refs',
            python_callable=validate_sql_refs,
            dag=dag,
        )
        
        # Create TaskGroups and tasks
        created_tasks = {}
        created_groups = {}
        
        # Create TaskGroups
        for group_name, group_tasks in task_groups.items():
            with TaskGroup(
                group_id=group_name,
                dag=dag,
                tooltip=f'Group: {group_name}'
            ) as task_group:
                # Create tasks within group
                for task_info in group_tasks:
                    task_name = task_info['task_name']
                    
                    # Prepare information for operator
                    # SEMPRE passar ref_map para permitir processamento de templates
                    task_kwargs = {
                        'task_id': task_name,
                        'sql_file_path': str(task_info['file_path']),
                        'spark_config': self.spark_config,
                        'dag': dag,
                        'ref_map': ref_map,  # Sempre passar ref_map
                    }
                    
                    # Se usa templates {{ ref() }}, marcar explicitamente
                    if task_info.get('has_templates', False):
                        task_kwargs['has_templates'] = True
                    
                    task = SparkSQLFileOperator(**task_kwargs)
                    
                    created_tasks[task_name] = task
            
            created_groups[group_name] = task_group
        
        # SEMPRE garantir ordem fixa: source → transformation → export
        # Ordem fixa dos grupos (independente de dependências)
        group_order = ['source', 'transformation', 'export']
        
        # Organizar grupos na ordem fixa
        ordered_groups = []
        for group_name in group_order:
            if group_name in created_groups:
                ordered_groups.append((group_name, created_groups[group_name]))
        
        # Adicionar grupos que não estão na ordem padrão (se houver)
        for group_name, group in created_groups.items():
            if not any(g[0] == group_name for g in ordered_groups):
                ordered_groups.append((group_name, group))
        
        # Criar dependências entre grupos na ordem fixa
        # Isso garante que source sempre executa antes de transformation,
        # e transformation sempre executa antes de export
        for i in range(len(ordered_groups) - 1):
            _, current_group = ordered_groups[i]
            _, next_group = ordered_groups[i+1]
            current_group >> next_group
        
        # Criar dependências entre tasks
        if use_dependency_detection and dependency_graph:
            # Usar detecção automática de dependências via {{ ref() }}
            logger.info("Using automatic dependency detection from {{ ref() }}")
            for task_name, deps in dependency_graph.items():
                if task_name in created_tasks:
                    for dep_task in deps:
                        if dep_task in created_tasks:
                            created_tasks[dep_task] >> created_tasks[task_name]
                            logger.debug(f"Created dependency from graph: {dep_task} >> {task_name}")
        else:
            # Usar execution_order como fallback (modo manual)
            logger.info("Using execution_order from pipeline.yml (dependency detection disabled)")
            if execution_order:
                # Criar mapeamento de task_name para índice no execution_order
                order_map = {task: idx for idx, task in enumerate(execution_order)}
                
                # Criar dependências sequenciais baseadas na ordem do YAML
                for i in range(len(execution_order) - 1):
                    current_task = execution_order[i]
                    next_task = execution_order[i + 1]
                    
                    if current_task in created_tasks and next_task in created_tasks:
                        created_tasks[current_task] >> created_tasks[next_task]
                        logger.debug(f"Created dependency from execution_order: {current_task} >> {next_task}")
            else:
                logger.warning("No execution_order defined and dependency detection disabled. Tasks may execute in undefined order.")
        
        # Add cleanup task to remove warehouse files after processing
        with TaskGroup(group_id='cleanup', dag=dag, tooltip='Cleanup catalog files') as cleanup_group:
            cleanup_task = BashOperator(
                task_id='catalog',
                bash_command=f"""
                echo "Cleaning up warehouse directory (lakehouse/.meta)..."
                echo ""
                if [ -d "{self.project_root}/lakehouse/.meta" ]; then
                    echo "Removing warehouse files..."
                    rm -rf "{self.project_root}/lakehouse/.meta"/*
                    echo "Warehouse cleaned (Materialized Views removed from catalog)"
                else
                    echo "Warehouse directory does not exist"
                fi
                echo ""
                echo "Cleanup completed"
                """,
                dag=dag,
            )
        
        # Connect initial validation as prerequisite of ALL tasks
        # If validation fails, all tasks will be skipped
        for task in created_tasks.values():
            validate_refs_task >> task
        
        # Connect cleanup to end of pipeline
        # Ordem: tasks -> cleanup
        # Cleanup removes all files from lakehouse/.meta (warehouse directory)
        if ordered_groups:
            _, last_group = ordered_groups[-1]
            last_group >> cleanup_group
        else:
            # Fallback: se não houver grupos, conectar todas as tasks
            for task in created_tasks.values():
                task >> cleanup_group
        
        return dag

