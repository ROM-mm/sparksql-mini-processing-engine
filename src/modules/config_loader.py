"""
Módulo para carregar e combinar configurações dos arquivos YAML.
"""
import yaml
from pathlib import Path
from typing import Dict, Any, Optional


class ConfigLoader:
    """Carrega configurações do pipeline.yml unificado."""
    
    def __init__(self, base_path: Optional[Path] = None):
        """
        Inicializa o carregador de configuração.
        
        Args:
            base_path: Caminho base do projeto (default: diretório atual)
        """
        if base_path is None:
            base_path = Path(__file__).parent.parent.parent
        self.base_path = Path(base_path)
        self._config_cache: Optional[Dict[str, Any]] = None
    
    def _load_pipeline_yml(self) -> Dict[str, Any]:
        """
        Carrega o arquivo pipeline.yml.
        
        Returns:
            Dicionário com todas as configurações
        """
        if self._config_cache is not None:
            return self._config_cache
        
        pipeline_path = self.base_path / "pipeline.yml"
        
        if not pipeline_path.exists():
            raise FileNotFoundError(f"Arquivo pipeline.yml não encontrado em {pipeline_path}")
        
        with open(pipeline_path, 'r', encoding='utf-8') as f:
            self._config_cache = yaml.safe_load(f) or {}
        
        return self._config_cache
    
    def load_definitions(self) -> Dict[str, Any]:
        """
        Carrega as definições do pipeline.yml.
        
        Returns:
            Dicionário com as definições das transformações
        """
        config = self._load_pipeline_yml()
        return config
    
    def load_global_config(self) -> Dict[str, Any]:
        """
        Carrega a configuração global do pipeline.yml.
        
        Returns:
            Dicionário com a configuração global
        """
        config = self._load_pipeline_yml()
        return config
    
    def load_pipeline_config(self) -> Dict[str, Any]:
        """
        Carrega o arquivo pipeline.yml completo.
        
        Returns:
            Dicionário completo com todas as configurações
        """
        return self._load_pipeline_yml()
    
    def get_spark_config(self) -> Dict[str, str]:
        """
        Extrai apenas as configurações do Spark.
        
        Returns:
            Dicionário com configurações do Spark (chave: valor como string)
        """
        config = self._load_pipeline_yml()
        spark_config = config.get('configuration', {})
        
        # Converter todos os valores para string (requisito do Spark)
        return {k: str(v) for k, v in spark_config.items()}
    
    def get_datalake_zones(self) -> Dict[str, str]:
        """
        Retorna mapeamento de zonas do datalake.
        
        Returns:
            Dicionário com nome da zona: caminho
        """
        config = self._load_pipeline_yml()
        zones_config = config.get('lakehouse_zones', {})
        zones = zones_config.get('zones', [])
        
        return {zone['name']: zone['path'] for zone in zones}
    
    def get_execution_order(self) -> list:
        """
        Retorna a ordem de execução das transformações.
        
        Returns:
            Lista com nomes das transformações em ordem
        """
        config = self._load_pipeline_yml()
        return config.get('execution_order', [])


# Função helper para uso rápido
def load_config() -> Dict[str, Any]:
    """
    Carrega todas as configurações combinadas.
    
    Returns:
        Dicionário completo com todas as configurações
    """
    loader = ConfigLoader()
    return loader.load_pipeline_config()

