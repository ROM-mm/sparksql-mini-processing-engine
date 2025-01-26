#.----------------------------------------------------------------------------------------------------------------------.
#|░█▀▄░█▀▀░█░█░█▀▀░█░░░█▀█░█▀█░█▄█░█▀▀░█▀█░▀█▀░░░█▀▄░█░█░░░█▀▄░█▀█░█▄█░█▀▀░█▀▄░▀█▀░▀█▀░█▀█░░░█▄█░█▀█░█▀▄░█▀█░▀█▀░█▀▀    |
#|░█░█░█▀▀░▀▄▀░█▀▀░█░░░█░█░█▀▀░█░█░█▀▀░█░█░░█░░░░█▀▄░░█░░░░█▀▄░█░█░█░█░█▀▀░█▀▄░░█░░░█░░█░█░░░█░█░█░█░█▀▄░█▀█░░█░░▀▀█    |
#|░▀▀░░▀▀▀░░▀░░▀▀▀░▀▀▀░▀▀▀░▀░░░▀░▀░▀▀▀░▀░▀░░▀░░░░▀▀░░░▀░░░░▀░▀░▀▀▀░▀░▀░▀▀▀░▀░▀░▀▀▀░░▀░░▀▀▀░░░▀░▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀    |
#|https://www.linkedin.com/in/romeritomorais/                                                                           |
#'----------------------------------------------------------------------------------------------------------------------'

import os
from pyspark.sql import SparkSession
from src.modules.exporting import SparkSQLExport
from src.modules.ingesting import SparkSQLIngestion
from src.modules.processing import SparkSQLProcessing
from src.modules.udf_loader import PandasUDFRegistry

# Define base directories for datasets, SQL files, and export locations
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# default options
app_name = BASE_DIR.split('/')[::-1][0]

# Initialize Spark session
spark = SparkSession.builder.appName(app_name).getOrCreate()

ingestion = SparkSQLIngestion(spark=spark)
processor = SparkSQLProcessing(spark=spark)
exporting = SparkSQLExport(spark=spark)

# Instantiate the UDF registrar
#udf_registry = PandasUDFRegistry(spark, base_udfs)

# Register all UDFs
#udf_registry.register_udfs()

def data_pipeline():
    """
    Executes the complete data pipeline: Ingests data, processes it, and exports the result.
    """
    # Step 1: Ingest the data
    ingestion_dataframe = ingestion.load()

    # Step 2: Process the data using the model pipeline
    processed_dataframe = processor.processing(dataframe=ingestion_dataframe)

    # Step 3: Export the processed DataFrame to a file
    exporting.write(dataframe=processed_dataframe)

    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    # Run the data pipeline
    data_pipeline()
