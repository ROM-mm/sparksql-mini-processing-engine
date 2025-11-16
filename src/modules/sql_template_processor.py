"""
SQL Template Processor dbt style.
Supports {{ ref('view_name') }} to reference other views and build dependencies.
"""
import re
from typing import Dict, List, Optional, Set
from pathlib import Path


class SQLTemplateProcessor:
    """
    Processes dbt-style SQL templates, replacing {{ ref('name') }} with real view names.
    
    Features:
    - Detects {{ ref('view_name') }} to build dependency graph
    - Replaces {{ ref('view_name') }} with view_name before execution
    - Validates if all references exist
    - Supports multiple references in the same file
    """
    
    # Pattern to detect {{ ref('name') }}
    REF_PATTERN = re.compile(r'\{\{\s*ref\s*\(\s*[\'"](\w+)[\'"]\s*\)\s*\}\}', re.IGNORECASE)
    
    # Pattern to detect CREATE VIEW
    CREATE_VIEW_PATTERN = re.compile(
        r'CREATE\s+(?:TEMPORARY\s+)?(?:MATERIALIZED\s+)?VIEW\s+(\w+)', 
        re.IGNORECASE
    )
    
    def __init__(self, view_to_task_map: Optional[Dict[str, str]] = None):
        """
        Initialize template processor.
        
        Args:
            view_to_task_map: Mapping of view_name -> task_name (optional)
        """
        self.view_to_task_map = view_to_task_map or {}
    
    def extract_refs(self, sql_content: str) -> List[str]:
        """
        Extracts all {{ ref('name') }} references from SQL.
        
        Args:
            sql_content: SQL file content
            
        Returns:
            List of referenced view names
        """
        refs = []
        for match in self.REF_PATTERN.finditer(sql_content):
            view_name = match.group(1)
            refs.append(view_name)
        return list(set(refs))  # Remove duplicates
    
    def extract_view_name(self, sql_content: str) -> Optional[str]:
        """
        Extracts the name of the view created in SQL.
        
        Args:
            sql_content: SQL file content
            
        Returns:
            Name of created view or None
        """
        match = self.CREATE_VIEW_PATTERN.search(sql_content)
        if match:
            return match.group(1)
        return None
    
    def compile_template(self, sql_content: str, ref_map: Optional[Dict[str, str]] = None) -> str:
        """
        Compiles SQL template, replacing {{ ref('name') }} with real names.
        
        Args:
            sql_content: SQL file content with templates
            ref_map: Custom mapping of ref_name -> view_name (optional)
                    If not provided, uses ref name as view name
            
        Returns:
            Compiled SQL (without templates)
        """
        compiled_sql = sql_content
        
        # If ref_map provided, use it
        if ref_map:
            # Encontrar todos os templates primeiro
            all_matches = list(self.REF_PATTERN.finditer(compiled_sql))
            
            if not all_matches:
                # Não há templates para substituir
                return compiled_sql
            
            # Processar de trás para frente para não alterar índices durante substituição
            for match in reversed(all_matches):
                ref_name = match.group(1)
                full_match = match.group(0)
                
                # Obter view_name do ref_map
                view_name = ref_map.get(ref_name, ref_name)
                
                # Validação: garantir que view_name é válido
                if not view_name or not isinstance(view_name, str):
                    raise ValueError(
                        f"Invalid view_name for ref('{ref_name}'): {view_name}. "
                        f"ref_map: {ref_map}"
                    )
                
                # Validar que view_name não contém caracteres especiais problemáticos
                if any(ord(c) > 127 for c in view_name):
                    raise ValueError(
                        f"view_name contains non-ASCII characters: {view_name}. "
                        f"This may cause SQL parsing errors."
                    )
                
                # Substituir o match completo
                start, end = match.span()
                before = compiled_sql[:start]
                after = compiled_sql[end:]
                compiled_sql = before + view_name + after
                
                # Verificar se a substituição foi bem-sucedida
                if compiled_sql[start:start+len(view_name)] != view_name:
                    raise RuntimeError(
                        f"Failed to replace template {{ ref('{ref_name}') }} with '{view_name}'. "
                        f"Match was at position {start}-{end}, full_match: '{full_match}'"
                    )
        else:
            # Replace all refs with ref name (default: ref('name') -> name)
            # Processar de trás para frente
            all_matches = list(self.REF_PATTERN.finditer(compiled_sql))
            for match in reversed(all_matches):
                ref_name = match.group(1)
                start, end = match.span()
                compiled_sql = compiled_sql[:start] + ref_name + compiled_sql[end:]
        
        return compiled_sql
    
    def validate_refs(
        self, 
        sql_content: str, 
        available_views: Set[str],
        task_name: Optional[str] = None
    ) -> tuple[bool, List[str]]:
        """
        Validates if all {{ ref('name') }} references exist.
        
        Args:
            sql_content: SQL file content
            available_views: Set of available view names
            task_name: Task name (for error messages)
            
        Returns:
            Tuple (is_valid, list_of_errors)
        """
        refs = self.extract_refs(sql_content)
        errors = []
        
        for ref_name in refs:
            # Check if ref exists in mapping or available views
            if ref_name not in available_views and ref_name not in self.view_to_task_map:
                error_msg = (
                    f"Reference not found: {{ ref('{ref_name}') }}"
                )
                if task_name:
                    error_msg += f" (in {task_name})"
                errors.append(error_msg)
        
        return len(errors) == 0, errors
    
    def process_file(
        self, 
        sql_file_path: Path,
        ref_map: Optional[Dict[str, str]] = None,
        validate: bool = True,
        available_views: Optional[Set[str]] = None
    ) -> Dict[str, any]:
        """
        Processes a complete SQL file: reads, validates and compiles.
        
        Args:
            sql_file_path: Path to SQL file
            ref_map: Custom mapping of ref_name -> view_name
            validate: If True, validates references before compiling
            available_views: Set of available views (for validation)
            
        Returns:
            Dictionary with:
            - original_content: Original SQL
            - compiled_content: Compiled SQL
            - refs: List of found references
            - view_name: Name of created view
            - is_valid: If all references are valid
            - errors: List of errors (if any)
        """
        # Read file
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
        
        # Extract information
        refs = self.extract_refs(original_content)
        view_name = self.extract_view_name(original_content)
        
        # Validate if requested
        is_valid = True
        errors = []
        
        if validate and available_views is not None:
            is_valid, errors = self.validate_refs(
                original_content, 
                available_views,
                task_name=sql_file_path.stem
            )
        
        # Compile template
        compiled_content = self.compile_template(original_content, ref_map)
        
        return {
            'original_content': original_content,
            'compiled_content': compiled_content,
            'refs': refs,
            'view_name': view_name,
            'is_valid': is_valid,
            'errors': errors,
            'file_path': sql_file_path
        }


def build_ref_map_from_sql_files(sql_files_info: List[Dict]) -> Dict[str, str]:
    """
    Builds mapping of ref_name -> table_name based on SQL files.
    
    IMPORTANTE: Como garantimos que a view/tabela sempre tem o nome do arquivo,
    sempre usamos task_name como table_name.
    
    Agora usamos tabelas persistentes em vez de views temporárias globais,
    então não precisamos de prefixo global_temp. As tabelas são registradas
    no catálogo do Spark e podem ser acessadas diretamente pelo nome.
    
    Args:
        sql_files_info: List of dictionaries with SQL file information
                      Each dict should have 'task_name', 'view_name', and 'file_path'
    
    Returns:
        Dictionary mapping ref_name -> table_name (sem prefixo, tabelas persistentes)
    """
    ref_map = {}
    
    # Create mapping: file name (without extension) -> table name
    # SEMPRE usar task_name como table_name, já que garantimos que são iguais
    # Isso se aplica a TODOS os arquivos, incluindo source files
    # (arquivos source criam views com o nome do arquivo, não com o nome da view source_table)
    for info in sql_files_info:
        task_name = info.get('task_name')
        
        if task_name:
            # Agora todas as views são convertidas em tabelas persistentes
            # Então: ref('task_name') -> task_name (tabela persistente no catálogo)
            # Isso inclui arquivos source: ref('mv_source') -> mv_source
            ref_map[task_name] = task_name
            
            # Também mapear view_name se existir e for diferente (para compatibilidade)
            extracted_view_name = info.get('view_name')
            if extracted_view_name and extracted_view_name != task_name:
                # Se view_name existe e é diferente, mapear ambos
                ref_map[extracted_view_name] = task_name  # view_name também aponta para task_name
    
    return ref_map
