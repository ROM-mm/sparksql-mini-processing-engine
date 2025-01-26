from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

@pandas_udf(StringType())
def lowwer2(column):
    return column.lower()


@pandas_udf(StringType())
def upper2(column):
    return column.upper()