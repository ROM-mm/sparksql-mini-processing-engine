import os
import time
from pathlib import Path

from graphviz import Digraph
from pyspark.sql import DataFrame, SparkSession

from src.modules.read_yaml import SparkReadYAML

# Obtém o diretório raiz assumindo que o arquivo que está rodando está na raiz do projeto
root_directory = Path(__file__).resolve().parent
# Caso você queira subir para um nível superior (exemplo: encontrar a raiz do projeto)
root_directory = root_directory.parent

base_sqlfiles = os.path.join(root_directory, "sql")
base_graphplan = os.path.join(root_directory, "graph_plan")

# Initialize modules
config_reader = SparkReadYAML(config_file="definitions.yaml")
config = config_reader.load_config()

# Pipeline settings
pipeline_settings = config_reader.load_config().get("pipeline_settings", {})
stages = pipeline_settings.get("print_dataframe", False)
plan = pipeline_settings.get("print_plan_execution", False)
model_list = config_reader.load_config().get("execution_sql_transfomation")
# print_plan_execution = pipeline_settings.get('print_plan_execution', False)


class SparkSQLProcessing:
    def __init__(self, spark: SparkSession):
        """
        Initializes the ModelProcessor class.

        Parameters:
            model_list (list): List of model names (SQL files) to execute.
            sql_path_models (str): Path where the SQL files are stored.
            spark (SparkSession): The Spark session to run the queries.
            base_graphplan (str): Directory path to save lineage graphs.
        """
        # self.model_list = model_list
        # self.sql_path_models = sql_path_models
        self.spark = spark
        # self.base_graphplan = base_graphplan

    def create_views(self, dataframe: DataFrame, sql_name_model: str):
        """
        Creates a temporary view from the DataFrame for SQL execution.
        """
        dataframe.createOrReplaceTempView(sql_name_model)

    def plot_dataframe_lineage(
        self, sql_name_model: str, dataframe: DataFrame, title="DataFrame Lineage"
    ):
        """
        Visualizes the lineage of a PySpark DataFrame as a graph using Graphviz.

        Parameters:
            sql_name_model (str): The name of the SQL model.
            dataframe (DataFrame): The DataFrame after transformation.
            title (str): Title of the lineage graph.

        Returns:
            None
        """
        # Get the logical plan of the DataFrame
        logical_plan = dataframe._jdf.queryExecution().toString()

        # Create the graph using Graphviz
        dot = Digraph(comment=title)
        nodes = []

        # Process each line in the logical plan
        for i, line in enumerate(logical_plan.split("\n")):
            node_id = f"node_{i}"
            dot.node(node_id, line.strip())
            if i > 0:
                dot.edge(f"node_{i - 1}", node_id)
            nodes.append(node_id)

        # Render the graph
        dot.render(
            f"{base_graphplan}/plan_graph_{sql_name_model}".lower(),
            format="png",
            cleanup=True,
        )
        print(
            f"Plan execution graph saved as '{base_graphplan}/plan_graph_{sql_name_model}.png'".lower()
        )

    def execute_models(self, model_name: str, dataframe: DataFrame) -> DataFrame:
        """
        Executes a model by reading the corresponding SQL file and applying it to the DataFrame.

        Parameters:
            model_name (str): The name of the model to execute.
            dataframe (DataFrame): The DataFrame to apply the model on.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        sql_file_path = f"{base_sqlfiles}/{model_name}.sql"

        try:
            # Read the SQL file
            with open(sql_file_path, "r") as file:
                sql_query = file.read()

            # Create a view and execute the SQL
            self.create_views(dataframe, model_name)
            dataframe = self.spark.sql(sql_query)

            return dataframe

        except FileNotFoundError:
            print(f"Error: SQL file not found: {sql_file_path}")
        except Exception as e:
            print(f"Error while processing model '{model_name}': {e}")

    def processing(self, dataframe: DataFrame) -> DataFrame:
        """
        Executes a pipeline of models on the DataFrame.

        Parameters:
            dataframe (DataFrame): The initial DataFrame to process.

        Returns:
            DataFrame: The resulting DataFrame after applying all models.
        """
        dataframe = dataframe

        count = 1

        for sql_file in model_list:
            try:
                print(
                    f" {count} - Executing query: ./src/sql/{sql_file}.sql in temporary view {sql_file}"
                )
                dataframe = self.execute_models(sql_file, dataframe)
                if stages:
                    dataframe.show(10, truncate=False)
                if plan:
                    self.plot_dataframe_lineage(sql_file, dataframe)
                time.sleep(5)  # Wait 5 seconds before executing the next model
                count += 1
            except Exception as e:
                print(f"Error while executing model '{sql_file}': {e}")

        return dataframe
