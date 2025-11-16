"""
Módulos auxiliares para o pipeline SDP.
"""

# Export Materialized View utilities for use throughout the project
from src.modules.materialized_view_utils import (
    create_materialized_view_from_dataframe,
    create_materialized_view_from_sql,
    create_materialized_view_from_file,
    execute_create_materialized_view_sql,
    drop_materialized_view,
    ensure_database,
)

__all__ = [
    'create_materialized_view_from_dataframe',
    'create_materialized_view_from_sql',
    'create_materialized_view_from_file',
    'execute_create_materialized_view_sql',
    'drop_materialized_view',
    'ensure_database',
]

