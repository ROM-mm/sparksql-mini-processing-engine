"""
DAG unificado do Airflow para orquestrar o pipeline SparkSQL Mini Processing Engine.

Este DAG suporta dois modos de execução:

1. **Modo SDP (Padrão/Recomendado)**: 
   - Usa SparkPipelinesOperator que executa via spark-pipelines CLI
   - SDP gerencia automaticamente ordem de execução, dependências e cache
   - Mais simples e eficiente
   - Ativado quando use_sdp_mode=True (padrão)

2. **Modo Dinâmico**:
   - Usa DAG Factory para criar tasks dinamicamente baseado em arquivos SQL
   - Detecta dependências automaticamente via parsing
   - Mais controle granular sobre cada task
   - Ativado quando use_sdp_mode=False

Para escolher o modo, defina a variável de ambiente:
- AIRFLOW_VAR_USE_SDP_MODE=true (padrão) - usa modo SDP
- AIRFLOW_VAR_USE_SDP_MODE=false - usa modo dinâmico

Ou modifique diretamente a variável use_sdp_mode abaixo.
"""
from datetime import timedelta, datetime
from pathlib import Path
from airflow.utils.dates import days_ago
from airflow import DAG

# Importar módulos do projeto
import sys
import os

# Determinar o caminho raiz do projeto
dag_file = Path(__file__).resolve()
if 'airflow_home' in str(dag_file):
    project_root = dag_file.parent.parent.parent
else:
    project_root = dag_file.parent.parent

# Adicionar ao PYTHONPATH
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

os.environ['PYTHONPATH'] = f"{project_root}:{os.environ.get('PYTHONPATH', '')}"

from src.modules.config_loader import ConfigLoader

# Carregar configurações
config_loader = ConfigLoader(project_root)
pipeline_config = config_loader.load_pipeline_config()

# Helper functions para parsear configurações do YAML
def parse_timedelta(config: dict) -> timedelta:
    """Parse timedelta from YAML config."""
    if isinstance(config, dict) and config.get('type') == 'timedelta':
        if 'days' in config:
            return timedelta(days=config['days'])
        elif 'hours' in config:
            return timedelta(hours=config['hours'])
        elif 'minutes' in config:
            return timedelta(minutes=config['minutes'])
        elif 'seconds' in config:
            return timedelta(seconds=config['seconds'])
    return timedelta(days=1)  # default

def parse_start_date(config: dict) -> datetime:
    """Parse start_date from YAML config."""
    if isinstance(config, dict) and config.get('type') == 'days_ago':
        days = config.get('days', 1)
        return days_ago(days)
    return days_ago(1)  # default

# Carregar configurações do DAG do pipeline.yml
dag_config = pipeline_config.get('dag', {})
default_args_config = dag_config.get('default_args', {})
default_args = {
    'owner': default_args_config.get('owner', 'data-engineering'),
    'depends_on_past': default_args_config.get('depends_on_past', False),
    'email_on_failure': default_args_config.get('email_on_failure', False),
    'email_on_retry': default_args_config.get('email_on_retry', False),
    'retries': default_args_config.get('retries', 1),
    'retry_delay': parse_timedelta(default_args_config.get('retry_delay', {'type': 'timedelta', 'minutes': 5})),
}

# Determinar modo de execução do pipeline.yml
dag_mode = dag_config.get('mode', 'dynamic').lower()
use_sdp_mode = dag_mode == 'sdp'

if use_sdp_mode:
    # ==========================================
    # MODO SDP (Recomendado)
    # ==========================================
    from dags.operators.spark_pipelines_operator import SparkPipelinesOperator
    from airflow.operators.bash import BashOperator
    
    spark_config = config_loader.get_spark_config()
    
    # Criar DAG com configurações do pipeline.yml
    dag = DAG(
        dag_id=dag_config.get('dag_id', 'sparksql_mini_processing_engine'),
        description=dag_config.get('description', 'Pipeline SparkSQL usando Spark Declarative Pipelines (SDP) - Modo otimizado'),
        schedule_interval=parse_timedelta(dag_config.get('schedule_interval', {'type': 'timedelta', 'days': 1})),
        start_date=parse_start_date(dag_config.get('start_date', {'type': 'days_ago', 'days': 1})),
        default_args=default_args,
        catchup=dag_config.get('catchup', False),
        tags=dag_config.get('tags', ['spark', 'sql', 'data-pipeline', 'sdp', 'declarative', 'materialized-views']),
    )
    
    # Task única que executa todo o pipeline via SDP
    # O SDP gerencia automaticamente:
    # - Ordem de execução baseada em dependências
    # - Persistência de Materialized Views
    # - Cache e otimizações
    tasks_config = dag_config.get('tasks', {})
    run_pipeline_config = tasks_config.get('run_spark_pipelines', {})
    run_pipeline = SparkPipelinesOperator(
        task_id=run_pipeline_config.get('task_id', 'run_spark_pipelines'),
        pipeline_spec_path='pipeline.yml',
        spark_config=spark_config,
        use_cli=run_pipeline_config.get('use_cli', True),
        dag=dag,
    )
    
    # Task de validação
    validate_output_config = tasks_config.get('validate_output', {})
    validate_output = BashOperator(
        task_id=validate_output_config.get('task_id', 'validate_output'),
        bash_command=f"""
        echo "🔍 Validando dados processados pelo SDP..."
        echo ""
        if [ -d "{project_root}/lakehouse/silver" ]; then
            echo "✅ Diretório silver existe"
            echo "📊 Conteúdo:"
            ls -lh "{project_root}/lakehouse/silver/" || echo "   (vazio)"
        else
            echo "⚠️  Diretório silver ainda não existe"
        fi
        if [ -d "{project_root}/lakehouse/gold" ]; then
            echo "✅ Diretório gold existe"
            echo "📊 Conteúdo:"
            ls -lh "{project_root}/lakehouse/gold/" || echo "   (vazio)"
        fi
        echo ""
        echo "✅ Validação concluída"
        """,
        dag=dag,
    )
    
    # Definir dependências
    run_pipeline >> validate_output

else:
    # ==========================================
    # MODO DINÂMICO (DAG Factory)
    # ==========================================
    from dags.dag_factory import DynamicDAGFactory
    
    # Criar factory de DAG
    dag_factory = DynamicDAGFactory(
        project_root=project_root,
        config_loader=config_loader
    )
    
    # Criar DAG dinâmico
    # use_dependency_detection é lido do pipeline.yml (transformation_settings.use_dependency_detection)
    # Se True: detecta dependências automaticamente via {{ ref() }} nas queries SQL
    # Se False: usa apenas a ordem definida em execution_order do pipeline.yml
    transformation_settings = pipeline_config.get('transformation_settings', {})
    use_dependency_detection = transformation_settings.get('use_dependency_detection', True)
    
    dag = dag_factory.create_dynamic_dag(
        dag_id=dag_config.get('dag_id', 'sparksql_mini_processing_engine'),
        description=dag_config.get('description', 'Pipeline SparkSQL dinâmico: detecta arquivos SQL automaticamente e cria linhagem'),
        schedule_interval=parse_timedelta(dag_config.get('schedule_interval', {'type': 'timedelta', 'days': 1})),
        start_date=parse_start_date(dag_config.get('start_date', {'type': 'days_ago', 'days': 1})),
        default_args=default_args,
        tags=dag_config.get('tags', ['spark', 'sql', 'data-pipeline', 'transformations', 'lineage', 'dynamic', 'dbt-like']),
        use_dependency_detection=use_dependency_detection,
    )
