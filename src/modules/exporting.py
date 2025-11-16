"""
Módulo para exportação de dados no pipeline SDP.
No SDP, a exportação é feita através de CREATE MATERIALIZED VIEW que persiste
automaticamente os dados. Este módulo lê configurações do pipeline.yml.

Suporta todas as principais configurações do Spark DataFrame.write():
- path/location: onde salvar os dados
- format: formato de saída (parquet, csv, json, orc, delta)
- mode: modo de escrita (overwrite, append, error, ignore)
- partitionBy: particionamento por colunas
- bucketBy: bucketing por colunas
- sortBy: ordenação antes de salvar
- options: opções específicas por formato
"""
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
import sys

# Adicionar caminho do projeto ao PYTHONPATH se necessário
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.modules.config_loader import ConfigLoader


class ExportingConfig:
    """Configurações para exportação de dados, carregadas do pipeline.yml."""
    
    def __init__(self, config_loader: Optional[ConfigLoader] = None):
        """
        Inicializa o carregador de configurações de exportação.
        
        Args:
            config_loader: Instância do ConfigLoader (opcional, cria nova se não fornecido)
        """
        if config_loader is None:
            config_loader = ConfigLoader(project_root)
        self.config_loader = config_loader
    
    def _get_exporting_config(self) -> Dict[str, Any]:
        """
        Carrega configurações de export do pipeline.yml.
        
        Returns:
            Dicionário com configurações de export
        """
        definitions = self.config_loader.load_definitions()
        return definitions.get('export', {})
    
    def _get_general_config(self) -> Dict[str, Any]:
        """Retorna configurações gerais de exportação."""
        exporting_config = self._get_exporting_config()
        return exporting_config.get('general', {})
    
    def _build_write_config(
        self,
        format_name: str,
        format_config: Dict[str, Any],
        path: Optional[str] = None,
        mode: Optional[str] = None,
        partition_by: Optional[List[str]] = None,
        sort_by: Optional[List[str]] = None,
        bucket_by: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Constrói configuração completa de escrita a partir do YAML e parâmetros.
        
        Args:
            format_name: Nome do formato (parquet, csv, json, etc)
            format_config: Configurações do formato do YAML
            path: Caminho onde salvar (sobrescreve YAML se fornecido)
            mode: Modo de escrita (sobrescreve YAML se fornecido)
            partition_by: Colunas para particionamento (sobrescreve YAML se fornecido)
            sort_by: Colunas para ordenação (sobrescreve YAML se fornecido)
            bucket_by: Configuração de bucketing (sobrescreve YAML se fornecido)
            
        Returns:
            Dicionário completo com todas as configurações de escrita
        """
        general_config = self._get_general_config()
        
        # Construir configuração base
        # Garantir que options é um dicionário (não None)
        format_options = format_config.get('options')
        if format_options is None:
            format_options = {}
        elif isinstance(format_options, dict):
            format_options = format_options.copy()
        else:
            format_options = {}
        
        config = {
            "format": format_name,
            "path": path or format_config.get('path') or general_config.get('base_path', ''),
            "mode": mode or format_config.get('mode') or general_config.get('mode', 'overwrite'),
            "options": format_options
        }
        
        # Adicionar particionamento
        partition_cols = partition_by or format_config.get('partitionBy', [])
        if partition_cols:
            config["partitionBy"] = partition_cols
        
        # Adicionar ordenação
        sort_cols = sort_by or format_config.get('sortBy', [])
        if sort_cols:
            config["sortBy"] = sort_cols
        
        # Adicionar bucketing
        bucket_config = bucket_by or format_config.get('bucketBy', {})
        if bucket_config and bucket_config.get('numBuckets') and bucket_config.get('columns'):
            config["bucketBy"] = {
                "numBuckets": bucket_config['numBuckets'],
                "columns": bucket_config['columns']
            }
        
        # Adicionar coalesce/rep partition
        if general_config.get('coalesce') is not None:
            config["coalesce"] = general_config['coalesce']
        if general_config.get('repartition') is not None:
            config["repartition"] = general_config['repartition']
        
        # Sempre incluir base_path no config para uso como fallback
        if 'base_path' not in config:
            config["base_path"] = general_config.get('base_path', '')
        
        return config
    
    def get_parquet_config(
        self,
        path: Optional[str] = None,
        mode: Optional[str] = None,
        partition_by: Optional[List[str]] = None,
        sort_by: Optional[List[str]] = None,
        bucket_by: Optional[Dict[str, Any]] = None,
        compression: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Retorna configurações completas para escrita Parquet, carregadas do YAML.
        
        Args:
            path: Caminho onde salvar (sobrescreve YAML)
            mode: Modo de escrita (sobrescreve YAML)
            partition_by: Colunas para particionamento (sobrescreve YAML)
            sort_by: Colunas para ordenação (sobrescreve YAML)
            bucket_by: Configuração de bucketing (sobrescreve YAML)
            compression: Tipo de compressão (sobrescreve YAML)
            
        Returns:
            Dicionário completo com opções de escrita Parquet
        """
        exporting_config = self._get_exporting_config()
        parquet_config = exporting_config.get('parquet', {})
        
        # Garantir que options é um dicionário (não None)
        if 'options' not in parquet_config or parquet_config.get('options') is None:
            parquet_config['options'] = {}
        
        # Sobrescrever compressão se fornecida
        if compression:
            parquet_config['options']['compression'] = compression
        elif 'compression' in parquet_config:
            parquet_config['options']['compression'] = parquet_config['compression']
        
        # Adicionar mergeSchema e writeLegacyFormat às options
        if parquet_config.get('mergeSchema', False):
            parquet_config['options']['mergeSchema'] = "true"
        if parquet_config.get('writeLegacyFormat', False):
            parquet_config['options']['writeLegacyFormat'] = "true"
        
        return self._build_write_config(
            "parquet",
            parquet_config,
            path=path,
            mode=mode,
            partition_by=partition_by,
            sort_by=sort_by,
            bucket_by=bucket_by
        )
    
    def get_csv_config(
        self,
        path: Optional[str] = None,
        mode: Optional[str] = None,
        partition_by: Optional[List[str]] = None,
        sort_by: Optional[List[str]] = None,
        header: Optional[bool] = None,
        delimiter: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Retorna configurações completas para escrita CSV, carregadas do YAML.
        
        Args:
            path: Caminho onde salvar (sobrescreve YAML)
            mode: Modo de escrita (sobrescreve YAML)
            partition_by: Colunas para particionamento (sobrescreve YAML)
            sort_by: Colunas para ordenação (sobrescreve YAML)
            header: Incluir header (sobrescreve YAML)
            delimiter: Delimitador (sobrescreve YAML)
            
        Returns:
            Dicionário completo com opções de escrita CSV
        """
        exporting_config = self._get_exporting_config()
        csv_config = exporting_config.get('csv', {})
        
        # Garantir que options é um dicionário (não None)
        if 'options' not in csv_config or csv_config.get('options') is None:
            csv_config['options'] = {}
        
        if header is not None:
            csv_config['options']['header'] = str(header).lower()
        elif 'header' in csv_config:
            csv_config['options']['header'] = str(csv_config['header']).lower()
        
        if delimiter:
            csv_config['options']['delimiter'] = delimiter
        elif 'delimiter' in csv_config:
            csv_config['options']['delimiter'] = csv_config['delimiter']
        
        # Adicionar outras opções do CSV
        for opt in ['quote', 'escape', 'encoding', 'nullValue', 'emptyValue', 'lineSep']:
            if opt in csv_config:
                csv_config['options'][opt] = csv_config[opt]
        
        return self._build_write_config(
            "csv",
            csv_config,
            path=path,
            mode=mode,
            partition_by=partition_by,
            sort_by=sort_by
        )
    
    def get_json_config(
        self,
        path: Optional[str] = None,
        mode: Optional[str] = None,
        partition_by: Optional[List[str]] = None,
        sort_by: Optional[List[str]] = None,
        date_format: Optional[str] = None,
        timestamp_format: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Retorna configurações completas para escrita JSON, carregadas do YAML.
        
        Args:
            path: Caminho onde salvar (sobrescreve YAML)
            mode: Modo de escrita (sobrescreve YAML)
            partition_by: Colunas para particionamento (sobrescreve YAML)
            sort_by: Colunas para ordenação (sobrescreve YAML)
            date_format: Formato de data (sobrescreve YAML)
            timestamp_format: Formato de timestamp (sobrescreve YAML)
            
        Returns:
            Dicionário completo com opções de escrita JSON
        """
        exporting_config = self._get_exporting_config()
        json_config = exporting_config.get('json', {})
        
        # Garantir que options é um dicionário (não None)
        if 'options' not in json_config or json_config.get('options') is None:
            json_config['options'] = {}
        
        if date_format:
            json_config['options']['dateFormat'] = date_format
        elif 'dateFormat' in json_config:
            json_config['options']['dateFormat'] = json_config['dateFormat']
        
        if timestamp_format:
            json_config['options']['timestampFormat'] = timestamp_format
        elif 'timestampFormat' in json_config:
            json_config['options']['timestampFormat'] = json_config['timestampFormat']
        
        # Adicionar outros formatos de timestamp se existirem
        for opt in ['timestampFormatInRead', 'timestampFormatInWrite']:
            if opt in json_config:
                json_config['options'][opt] = json_config[opt]
        
        return self._build_write_config(
            "json",
            json_config,
            path=path,
            mode=mode,
            partition_by=partition_by,
            sort_by=sort_by
        )
    
    def get_orc_config(
        self,
        path: Optional[str] = None,
        mode: Optional[str] = None,
        partition_by: Optional[List[str]] = None,
        sort_by: Optional[List[str]] = None,
        bucket_by: Optional[Dict[str, Any]] = None,
        compression: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Retorna configurações completas para escrita ORC, carregadas do YAML.
        
        Args:
            path: Caminho onde salvar (sobrescreve YAML)
            mode: Modo de escrita (sobrescreve YAML)
            partition_by: Colunas para particionamento (sobrescreve YAML)
            sort_by: Colunas para ordenação (sobrescreve YAML)
            bucket_by: Configuração de bucketing (sobrescreve YAML)
            compression: Tipo de compressão (sobrescreve YAML)
            
        Returns:
            Dicionário completo com opções de escrita ORC
        """
        exporting_config = self._get_exporting_config()
        orc_config = exporting_config.get('orc', {})
        
        # Garantir que options é um dicionário (não None)
        if 'options' not in orc_config or orc_config.get('options') is None:
            orc_config['options'] = {}
        
        if compression:
            orc_config['options']['compression'] = compression
        elif 'compression' in orc_config:
            orc_config['options']['compression'] = orc_config['compression']
        
        return self._build_write_config(
            "orc",
            orc_config,
            path=path,
            mode=mode,
            partition_by=partition_by,
            sort_by=sort_by,
            bucket_by=bucket_by
        )
    
    def get_delta_config(
        self,
        path: Optional[str] = None,
        mode: Optional[str] = None,
        partition_by: Optional[List[str]] = None,
        sort_by: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Retorna configurações completas para escrita Delta, carregadas do YAML.
        
        Args:
            path: Caminho onde salvar (sobrescreve YAML)
            mode: Modo de escrita (sobrescreve YAML)
            partition_by: Colunas para particionamento (sobrescreve YAML)
            sort_by: Colunas para ordenação (sobrescreve YAML)
            
        Returns:
            Dicionário completo com opções de escrita Delta
        """
        exporting_config = self._get_exporting_config()
        delta_config = exporting_config.get('delta', {})
        
        # Garantir que options é um dicionário (não None)
        if 'options' not in delta_config or delta_config.get('options') is None:
            delta_config['options'] = {}
        
        return self._build_write_config(
            "delta",
            delta_config,
            path=path,
            mode=mode,
            partition_by=partition_by,
            sort_by=sort_by
        )
    
    def apply_write_config(self, df, config: Dict[str, Any]):
        """
        Aplica configurações de escrita a um DataFrame do Spark.
        
        Args:
            df: DataFrame do Spark
            config: Dicionário com configurações de escrita
            
        Returns:
            DataFrameWriter configurado
        """
        # Aplicar transformações no DataFrame ANTES de criar o writer
        # Ordem importante: sortBy -> coalesce/repartition
        
        # 1. Aplicar ordenação primeiro (se especificado)
        if 'sortBy' in config and config['sortBy']:
            df = df.sort(*config['sortBy'])
        
        # 2. Aplicar coalesce ou repartition (coalesce tem prioridade se ambos estiverem presentes)
        if 'coalesce' in config and config['coalesce'] is not None:
            df = df.coalesce(config['coalesce'])
        elif 'repartition' in config and config['repartition'] is not None:
            if 'partitionBy' in config and config['partitionBy']:
                # Se há partitionBy, usar repartition com colunas de particionamento
                df = df.repartition(config['repartition'], *config['partitionBy'])
            else:
                df = df.repartition(config['repartition'])
        
        # 3. Criar o writer com o DataFrame transformado
        writer = df.write.format(config['format'])
        
        # 4. Aplicar modo
        writer = writer.mode(config['mode'])
        
        # 5. Aplicar opções
        if 'options' in config:
            for key, value in config['options'].items():
                writer = writer.option(key, value)
        
        # 6. Aplicar particionamento (apenas se não foi usado no repartition acima)
        if 'partitionBy' in config and config['partitionBy']:
            # Só aplicar partitionBy no writer se não foi usado no repartition
            if 'repartition' not in config or config.get('repartition') is None:
                writer = writer.partitionBy(*config['partitionBy'])
        
        # 7. Aplicar bucketing
        if 'bucketBy' in config:
            bucket_config = config['bucketBy']
            if bucket_config.get('numBuckets') and bucket_config.get('columns'):
                writer = writer.bucketBy(
                    bucket_config['numBuckets'],
                    *bucket_config['columns']
                )
        
        return writer
    
    # Métodos estáticos para compatibilidade com código legado
    @staticmethod
    def get_parquet_config_static(
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Método estático para compatibilidade."""
        config = {
            "format": "parquet",
            "mode": mode,
            "options": {"compression": "snappy"}
        }
        if partition_by:
            config["partitionBy"] = partition_by
        return config
    
    @staticmethod
    def get_csv_config_static(
        mode: str = "overwrite",
        header: bool = True
    ) -> Dict[str, Any]:
        """Método estático para compatibilidade."""
        return {
            "format": "csv",
            "mode": mode,
            "options": {
                "header": str(header).lower(),
                "delimiter": ","
            }
        }
