"""
Módulo de source (origem dos dados) usando Spark Declarative Pipelines (SDP).
Esta implementação segue a documentação oficial do SDP.

Conforme documentação: https://spark.apache.org/docs/4.1.0-preview1/declarative-pipelines-programming-guide.html

A leitura da origem é feita usando o módulo utilitário materialized_view_utils,
que centraliza a lógica de criação de Materialized Views em todo o projeto.
"""
from pathlib import Path
import yaml
import logging
from pyspark.sql import SparkSession

from src.modules.materialized_view_utils import (
    create_materialized_view_from_file,
    ensure_database
)

logger = logging.getLogger(__name__)


def _load_source_config() -> dict:
    """
    Carrega configurações de source (origem dos dados) do pipeline.yml.
    
    Returns:
        Dicionário com configurações de source
    """
    project_root = Path(__file__).parent.parent.parent
    pipeline_path = project_root / "pipeline.yml"
    
    if not pipeline_path.exists():
        raise FileNotFoundError(f"Arquivo pipeline.yml não encontrado em {pipeline_path}")
    
    with open(pipeline_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f) or {}
    
    source_config = config.get('source', {})
    
    if not source_config:
        raise ValueError(
            "Configuração de source não encontrada em pipeline.yml. "
            "Verifique a seção 'source'"
        )
    
    return source_config


def _load_catalog_config() -> dict:
    """
    Carrega configurações de catalog e database do pipeline.yml.
    
    Returns:
        Dicionário com catalog e database
    """
    project_root = Path(__file__).parent.parent.parent
    pipeline_path = project_root / "pipeline.yml"
    
    if not pipeline_path.exists():
        logger.warning(f"pipeline.yml não encontrado, usando valores padrão")
        return {"catalog": "spark_catalog", "database": "default"}
    
    with open(pipeline_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f) or {}
    
    catalog = config.get('catalog', 'spark_catalog')
    database = config.get('database', 'default')
    
    return {"catalog": catalog, "database": database}


def create_source_materialized_view(spark: SparkSession):
    """
    Cria uma MATERIALIZED VIEW usando o módulo utilitário centralizado.
    
    Esta função usa o módulo materialized_view_utils para criar a MATERIALIZED VIEW,
    garantindo consistência com o resto do projeto.
    
    Args:
        spark: SparkSession instance
        
    Returns:
        DataFrame: DataFrame da MATERIALIZED VIEW criada
    """
    # Carregar configurações
    config = _load_source_config()
    catalog_config = _load_catalog_config()
    
    format_type = config.get("format", "csv")
    path = config.get("path")
    options = config.get("options", {})
    view_name = config.get("view_name", "source_table")
    
    if not path:
        raise ValueError("Caminho (path) não especificado na configuração de source")
    
    logger.info(f"Creating MATERIALIZED VIEW '{view_name}' from source using centralized utility")
    logger.info(f"Catalog: {catalog_config['catalog']}, Database: {catalog_config['database']}")
    
    # Usar o módulo utilitário centralizado para criar a Materialized View
    # Isso garante consistência com o resto do projeto
    df = create_materialized_view_from_file(
        spark=spark,
        file_path=path,
        format_type=format_type,
        options=options,
        view_name=view_name,
        database=catalog_config['database'],
        if_not_exists=True,
        verify=True
    )
    
    logger.info(f"Source reading completed. Schema: {df.schema}")
    try:
        record_count = df.count()
        logger.info(f"Number of records: {record_count}")
    except Exception as e:
        logger.warning(f"Could not count records: {e}")
    
    return df

