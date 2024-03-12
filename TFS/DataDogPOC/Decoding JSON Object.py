# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# COMMAND ----------

volume_path = '/Volumes/kbarlow/tfs_datadog_poc/json_data/'

file_path = '/Workspace/Repos/kevin.barlow@databricks.com/e2-demo-fe/TFS/DataDog POC/DistributionPackage.json'

# COMMAND ----------

dbutils.fs.ls(volume_path + 'DistributionPackage.json')

# COMMAND ----------

json_schema = (spark
    .read
    .json(volume_path + 'DistributionPackage.json', multiLine=True)
    .schema
)

json_data = (
  spark
  .read
  .schema(json_schema)
  .option("header", "true")  # Treat the first line as the header
  .json(volume_path + 'DistributionPackage.json', multiLine=True)
)

display(json_data)

# COMMAND ----------

json_string = (json_data.select('SerializedJsonObject').collect()[0][0])

json_string

# COMMAND ----------

def parseJSONCols(df, json_col, sanitize=True):
    """Auto infer the schema of a json column and parse into a struct.

    rdd-based schema inference works if you have well-formatted JSON,
    like ``{"key": "value", ...}``, but breaks if your 'JSON' is just a
    string (``"data"``) or is an array (``[1, 2, 3]``). In those cases you
    can fix everything by wrapping the data in another JSON object
    (``{"key": [1, 2, 3]}``). The ``sanitize`` option (default True)
    automatically performs the wrapping and unwrapping.

    The schema inference is based on this
    `SO Post <https://stackoverflow.com/a/45880574)/>`_.

    Parameters
    ----------
    df : pyspark dataframe
        Dataframe containing the JSON cols.
    *cols : string(s)
        Names of the columns containing JSON.
    sanitize : boolean
        Flag indicating whether you'd like to sanitize your records
        by wrapping and unwrapping them in another JSON object layer.

    Returns
    -------
    pyspark dataframe
        A dataframe with the decoded columns.
    """
    res = df
    i = json_col

    # sanitize if requested.
    if sanitize:
        res = (
            res.withColumn(
                i,
                concat(lit('{"data": '), i, lit('}'))
            )
        )
    # infer schema and apply it
    schema = spark.read.json(res.rdd.map(lambda x: x[i])).schema
    res = res.withColumn(i, from_json(col(i), schema))

    # unpack the wrapped object if needed
    if sanitize:
        res = res.withColumn(i, col(i).data)
    
    return res

# COMMAND ----------

file_path = 'file:/Workspace/Repos/kevin.barlow@databricks.com/e2-demo-fe/TFS/DataDogPOC/DistributionPackage.json'

df = json_data

new_df = parseJSONCols(df, 'SerializedJsonObject', True).select('*','SerializedJsonObject.*')
new_df = parseJSONCols(new_df, 'SerializedJsonData', True).select('*','SerializedJsonData.*')

display(new_df)

# COMMAND ----------

def decode_json_column(df, json_col):
    """
    Decode a JSON column in a DataFrame and return the decoded column.

    Parameters:
    df (DataFrame): Input DataFrame.
    json_col (str): Name of the JSON column to decode.

    Returns:
    Column: Decoded column.
    """
    sanitize = True

    if sanitize:
        df = df.withColumn(
            json_col,
            concat(lit('{"data": '), col(json_col), lit('}'))
        )

    schema = spark.read.json(df.rdd.map(lambda x: x[json_col])).schema
    decoded_column = from_json(col(json_col), schema)

    return decoded_column.data

# COMMAND ----------

test_df2 = spark.read.json(file_path, multiLine=True)

display(test_df2)
