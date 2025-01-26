# sparksql mini processing engine

```None
.----------------------------------------------------------------------------------------------------------------------.
|░█▀▄░█▀▀░█░█░█▀▀░█░░░█▀█░█▀█░█▄█░█▀▀░█▀█░▀█▀░░░█▀▄░█░█░░░█▀▄░█▀█░█▄█░█▀▀░█▀▄░▀█▀░▀█▀░█▀█░░░█▄█░█▀█░█▀▄░█▀█░▀█▀░█▀▀    |
|░█░█░█▀▀░▀▄▀░█▀▀░█░░░█░█░█▀▀░█░█░█▀▀░█░█░░█░░░░█▀▄░░█░░░░█▀▄░█░█░█░█░█▀▀░█▀▄░░█░░░█░░█░█░░░█░█░█░█░█▀▄░█▀█░░█░░▀▀█    |
|░▀▀░░▀▀▀░░▀░░▀▀▀░▀▀▀░▀▀▀░▀░░░▀░▀░▀▀▀░▀░▀░░▀░░░░▀▀░░░▀░░░░▀░▀░▀▀▀░▀░▀░▀▀▀░▀░▀░▀▀▀░░▀░░▀▀▀░░░▀░▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀    |
|https://www.linkedin.com/in/romeritomorais/                                                                           |
'----------------------------------------------------------------------------------------------------------------------'
```

This project aims to perform all data ingestion and transformation using only SQL script files.  
The entire process is executed via SparkSQL, utilizing temporary views.
It only requires a few configurations, such as:

difinitions.yaml:  

```yaml
pipeline_properties:
  read:
    format: json
    options:
      header: false
      inferSchema: false
      path: "/Users/romeritomorais/Documents/Develop/Projects/datasets/stage/*"
      delimiter: ","
      recursiveFileLookup: true

  write:
    format: parquet
    mode: overwrite
    partitionBy:
      - date_of_week
    options:
      header: true
      delimiter: ","

pipeline_settings:
  print_dataframe: true
  print_plan_execution: false
  engine:
    name: pyspark
    version: 3.5.4

execution_sql_transfomation:
  - table_aviation_select_structure
  - table_aviation_extract_date
  - table_aviation_filter
  - table_aviation_definite_data

datalake_zones:
  path: "/Users/romeritomorais/Documents/Develop/Projects/datasets/"
  zones:
  - stage
  - landing
  - raw
  - trusted
```
### Additional configurations for file formats:
- JSON: https://spark.apache.org/docs/3.5.4/sql-data-sources-json.html
- CSV https://spark.apache.org/docs/3.5.4/sql-data-sources-csv.html
- PARQUET: https://spark.apache.org/docs/3.5.4/sql-data-sources-parquet.html

## Environment Configurations:  
- Python = 3.10
- Dendencies:
    ```bash
    pip3 install -r ./requirements.txt
    ```

## Folder Structure  

```None
src
├── datasets
├── graph_plan
├── modules
│    __init__.py
│   ├── exporting.py
│   ├── ingesting.py
│   └── processing.py
│   └── read_yaml.py
│   └── udf_loader.py
└── sql
│   ├── table_aviation_definite_data.sql
│   ├── table_aviation_extract_date.sql
│   ├── table_aviation_filter.sql
│   └── table_aviation_select_structure.sql
├── udfs
```
`datasets:` Directory where the files to be processed should be stored.  
`graph_plan:` Directory containing images with the Spark execution plan for each executed model.  
`modules:` Directory containing reusable Python code with classes for ingestion, processing, export and read_yaml.  
`sql:` Directory containing .sql files with transformation commands.  
`udfs:` The directory contains functions written in python to be created in the spark session and used in .sql files.  
 - sample:
    ```sql
    select sample_udf_lower(column) from table
    ```  

## Description of Data Transformation Models

table_aviation_select_structure.sql:  
- Selects data from the column json_table<structure>.
- Renames columns to follow the snake_case standard.
- Splits the crew column into crew_fatalities and crew_occupants.
- Splits the passengers column into passengers_fatalities and passengers_occupants.
- Splits the departure_airport column into departure_airport_name and departure_airport_country.

table_aviation_extract_date.sql:
- Splits the date field into the following fields:
  - date_of_week
  - date_of_day
  - date_of_month
  - date_of_year

table_aviation_filter.sql:
- Filters data where crew_fatalities > 0  
- Filters data by months of the year  
- Filters the departure_airport field where NOT IN ('?', '-')  

table_aviation_definite_data.sql:  
- Converts months of the year to numerical values  
- Concatenates fields into the date field: CONCAT_WS('-', date_of_year, date_of_month1, date_of_day1)  

## Execution models:  
Dataset:
    # https://www.kaggle.com/datasets/donat1/aviationsafety-database-1919-2019  
In the definitions.yaml file define the execution order 
```yaml
execution_sql_transfomation:
  - "table_aviation_select_structure"
  - "table_aviation_extract_date"
  - "table_aviation_filter"
  - "table_aviation_definite_data"
```
The order is important, as a transformation calls another transformation using in-memory views in Spark   
