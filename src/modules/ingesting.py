import os
import shutil

from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date, year, month, dayofmonth
from src.modules.read_yaml import SparkReadYAML

# Initialize modules
config_reader = SparkReadYAML(config_file="definitions.yaml")
config = config_reader.load_config()

# Extract configurations
pipeline_settings = config_reader.load_config().get('pipeline_settings', {})
datalake_zones = config_reader.load_config().get('datalake_zones', {})
pipeline_properties = config.get('pipeline_properties', {})
read_properties = pipeline_properties.get('read', {})
ingestion_options = read_properties.get('options', {})

# Pipeline settings
stages = pipeline_settings.get('print_dataframe', False)
print_plan_execution = pipeline_settings.get('print_plan_execution', False)
path = datalake_zones.get('path')
zones = datalake_zones.get('zones')[2]

def get_local_timestamp():
    # Pega o timestamp local
    current_timestamp = datetime.now()

    # Extrai o ano, mês e dia
    year = current_timestamp.year
    month = current_timestamp.month
    day = current_timestamp.day

    # Retorna as três variáveis
    return year, month, day

# Teste da função
year, month, day = get_local_timestamp()

def move_files(source_folder, destination_folder):
    # Verifica se as pastas de origem e destino existem
    if not os.path.exists(source_folder):
        print(f"A pasta de origem {source_folder} não existe.")
        return
    if not os.path.exists(destination_folder):
        print(f"A pasta de destino {destination_folder} não existe.")
        return

    # Itera sobre todos os arquivos na pasta de origem
    for filename in os.listdir(source_folder):
        # Constrói o caminho completo dos arquivos
        source_file = os.path.join(source_folder, filename)
        destination_file = os.path.join(destination_folder, filename)

        # Verifica se é um arquivo e não uma subpasta
        if os.path.isfile(source_file):
            # Move o arquivo para a pasta de destino
            shutil.move(source_file, destination_file)
            print(f"Arquivo {filename} movido para {destination_folder}")

class SparkSQLIngestion:
    def __init__(self, spark: SparkSession):
        """
        Initialize the DataIngestion class with a Spark session.

        Parameters:
            spark (SparkSession): The active Spark session.
        """
        self.spark = spark

    def load(self) -> DataFrame:
        """
        Load data into a DataFrame based on the specified format and options.

        Parameters:
            format (str): The format of the input data (e.g., 'csv', 'json', 'parquet').
            options (dict): The options for reading the data (e.g., header, delimiter).

        Returns:
            DataFrame: A PySpark DataFrame containing the loaded data.
        """
        try:
            # Load the data into a DataFrame using the specified format and options
            dataframe = self.spark.read.format(read_properties.get("format")).options(**ingestion_options).load().withColumn('_date', current_date())
            dataframe = dataframe.selectExpr('*','cast(year(current_date) AS string) AS _year','cast(month(current_date) AS string) AS _month','cast(day(current_date) AS string) AS _day')
            if stages:
                dataframe.show(10, truncate=False)
            dataframe.write.format('parquet').partitionBy('_year','_month','_day').mode('append').save(f'{path}/{zones}')

            dataframe = dataframe.drop('_date','_year','_month','_day')
        except Exception as e:
            print(f"Error loading data: {e}")
            raise

        return dataframe
