import importlib.util
import os

from pyspark.sql import SparkSession


class PandasUDFRegistry:
    def __init__(self, spark: SparkSession, udf_directory: str):
        """
        Initializes the class with a Spark instance and a directory of UDFs.

        Args:
            spark (SparkSession): Instance of SparkSession.
            udf_directory (str): Path to the directory where UDF files are located.
        """
        self.spark = spark
        self.udf_directory = udf_directory

    def _load_udf_from_file(self, file_path: str):
        """
        Loads all UDF functions from a Python file.

        Args:
            file_path (str): Path to the Python file containing UDFs.

        Returns:
            dict: A dictionary with the function name as the key and the UDF function as the value.
        """
        spec = importlib.util.spec_from_file_location("udf_module", file_path)
        udf_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(udf_module)

        # Filters functions decorated with @pandas_udf (verification by return type or signature)
        udfs = {
            name: func
            for name, func in vars(udf_module).items()
            if callable(func) and hasattr(func, "returnType")
        }

        return udfs

    def register_udfs(self):
        """
        Registers all UDF functions found in the directory files.
        """
        if not os.path.isdir(self.udf_directory):
            raise NotADirectoryError(f"The path '{self.udf_directory}' is not a valid directory.")

        for file_name in os.listdir(self.udf_directory):
            # Process only .py files
            if file_name.endswith(".py"):
                file_path = os.path.join(self.udf_directory, file_name)
                udfs = self._load_udf_from_file(file_path)

                for udf_name, udf_function in udfs.items():
                    self.spark.udf.register(udf_name, udf_function)
                    print(f"UDF {udf_name}() successfully registered from {file_name}.")
