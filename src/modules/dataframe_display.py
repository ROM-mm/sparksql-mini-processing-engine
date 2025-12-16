"""
Module for displaying DataFrame previews in logs.
"""
import logging
from typing import Optional
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class DataFrameDisplay:
    """Utility class for displaying DataFrame previews."""
    
    def __init__(self, logger_instance: Optional[logging.Logger] = None):
        """
        Initialize DataFrameDisplay.
        
        Args:
            logger_instance: Logger instance (optional, uses module logger if not provided)
        """
        self.log = logger_instance or logger
    
    def display_preview(
        self,
        df: DataFrame,
        view_name: str = "DataFrame",
        max_rows: int = 10,
        show_total_count: bool = False
    ):
        """
        Display DataFrame preview in logs.
        This method formats and logs the data instead of using show() which may not appear in logs.
        
        Args:
            df: Spark DataFrame to display
            view_name: Name of the view/DataFrame for logging
            max_rows: Maximum number of rows to display
            show_total_count: If True, runs df.count() (can be expensive on large datasets)
        """
        try:
            # Get schema
            schema = df.schema
            columns = [field.name for field in schema.fields]
            
            # Collect limited rows
            rows = df.limit(max_rows).collect()
            
            # Log header
            self.log.info(f"\n{'=' * 80}")
            self.log.info(f"PREVIEW: First {len(rows)} rows from '{view_name}'")
            self.log.info(f"{'=' * 80}")
            
            if len(rows) == 0:
                self.log.info("   (No rows to display)")
                self.log.info(f"{'=' * 80}\n")
                return
            
            # Calculate column widths
            col_widths = {}
            for col in columns:
                col_widths[col] = max(len(str(col)), 15)  # Minimum width 15
            
            # Adjust widths based on data
            for row in rows:
                for i, col in enumerate(columns):
                    value = str(row[i]) if row[i] is not None else "NULL"
                    col_widths[col] = max(col_widths[col], min(len(value), 50))  # Max width 50
            
            # Print header
            header = " | ".join([col.ljust(col_widths[col]) for col in columns])
            self.log.info(header)
            self.log.info("-" * len(header))
            
            # Print rows
            for row in rows:
                row_str = " | ".join([
                    (str(row[i]) if row[i] is not None else "NULL")[:col_widths[col]].ljust(col_widths[col])
                    for i, col in enumerate(columns)
                ])
                self.log.info(row_str)
            
            # Log total count only if explicitly requested (df.count() triggers a full scan)
            if show_total_count:
                try:
                    total_count = df.count()
                    self.log.info(f"\nTotal rows in '{view_name}': {total_count}")
                except Exception as count_error:
                    self.log.debug(f"Could not count total rows for '{view_name}': {count_error}")
            
            self.log.info(f"{'=' * 80}\n")
            
        except Exception as e:
            self.log.warning(f"Could not display preview of '{view_name}': {e}")
            import traceback
            self.log.debug(traceback.format_exc())

