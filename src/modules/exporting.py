from pyspark.sql import DataFrame, SparkSession
from src.modules.read_yaml import SparkReadYAML

from datetime import datetime

# Initialize modules
config_reader = SparkReadYAML(config_file="definitions.yaml")
config = config_reader.load_config()

pipeline_properties = config.get('pipeline_properties', {})
datalake_zones = config_reader.load_config().get('datalake_zones', {})
write_properties = pipeline_properties.get('write', {})
export_options = write_properties.get('options', {})

format = write_properties.get("format")
mode = write_properties.get("mode")
partition = write_properties.get("partitionBy")
path = datalake_zones.get('path')
zones = datalake_zones.get('zones')[3]
path_files = f'{path}/{zones}'

class SparkSQLExport:
    def __init__(self, spark: SparkSession):
        """
        Initializes the DataProcessor class.

        Parameters:
            model_list (list): List of model names (SQL files) to execute.
            sql_path_models (str): Path where the SQL files are stored.
            spark (SparkSession): The Spark session to run the queries.
        """
        self.spark = spark

    def write(self, dataframe: DataFrame):
        """
        Exports a PySpark DataFrame to the specified format and path.

        Parameters:
            dataframe (DataFrame): The PySpark DataFrame to export.
            format (str): The format to save the data (e.g., 'csv', 'parquet', 'json').
            mode (str): The write mode ('overwrite', 'append', 'ignore', 'error').
            partitioning (list): A list of columns to partition by (optional).
            path (str): The destination path to save the file.
            options (dict): Additional options for writing the data (optional).

        Returns:
            None
        """
        if not isinstance(dataframe, DataFrame):
            raise TypeError("The 'dataframe' parameter must be a PySpark DataFrame.")

        if not format:
            raise ValueError("The 'format' parameter is required.")

        if not mode:
            raise ValueError("The 'mode' parameter is required.")

        # Default to an empty dictionary if no options are provided
        options = export_options or {}

        writer = dataframe.coalesce(5).write.format(format).options(**export_options).mode(mode)

        # Add partitioning if provided
        if partition:
            writer = writer.partitionBy(*partition)

        # Save the DataFrame to the specified path
        writer.save(path_files)
