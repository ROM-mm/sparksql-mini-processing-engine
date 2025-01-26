import yaml


class SparkReadYAML:
    def __init__(self, config_file="definitions.yaml"):
        """
        Initializes the SparkReadYAML instance with the path to the YAML config file.

        Args:
            config_file (str): Path to the YAML configuration file.
        """
        self.config_file = config_file
        self.config = self.load_config()

    def load_config(self):
        """
        Loads and parses the YAML configuration file.

        Returns:
            dict: Parsed configuration data.

        Raises:
            FileNotFoundError: If the configuration file does not exist.
            ValueError: If there is an error in parsing the YAML file.
        """
        try:
            with open(self.config_file, "r") as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file '{self.config_file}' not found.")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file: {e}")

    def get_properties(self):
        """
        Retrieves the properties for ingestion and export from the loaded configuration.

        Returns:
            tuple: A tuple containing the properties for read and write operations
                   and their corresponding options (ingestion and export).

        Raises:
            KeyError: If required keys are missing in the configuration file.
        """
        try:
            # Retrieve properties related to data ingestion and export
            properties_read = self.config.get('properties_read', {})
            options_ingestion = properties_read.get('options', {})

            properties_write = self.config.get('properties_write', {})
            options_export = properties_write.get('options', {})

            order_models = self.config.get('order_execution_models', {})

            return properties_read, options_ingestion, properties_write, options_export, order_models

        except KeyError as e:
            raise KeyError(f"Missing key in configuration: {e}")

    def get_read_properties(self):
        """
        Retrieves the read properties (format and options) for ingestion from the configuration.

        Returns:
            dict: Properties for ingestion (including file format and options).
        """
        properties_read, options_ingestion, order_models, _, _ = self.get_properties()
        return properties_read, options_ingestion, order_models

    def get_write_properties(self):
        """
        Retrieves the write properties (format and options) for export from the configuration.

        Returns:
            dict: Properties for export (including file format and options).
        """
        _, _, properties_write, options_export, order_models = self.get_properties()
        return properties_write, options_export, order_models
