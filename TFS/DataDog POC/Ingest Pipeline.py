# Databricks notebook source
# Importing the dlt module
import dlt

# COMMAND ----------

volume_path = '/Volumes/kbarlow/tfs_datadog_poc/json_data/'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load JSON into Raw Table

# COMMAND ----------

from pyspark.sql.functions import col, from_json, schema_of_json
from pyspark.sql.types import *

# COMMAND ----------

def flatten_df(nested_df):
    stack = [((), nested_df)]
    columns = []

    while len(stack) > 0:
        parents, df = stack.pop()

        flat_cols = [
            col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
            for c in df.dtypes
            if c[1][:6] != "struct"
        ]

        nested_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:6] == "struct"
        ]

        columns.extend(flat_cols)

        for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))

    return nested_df.select(columns)

# COMMAND ----------

json_schema = (spark
    .read
    # .option("basePath", volume_path)
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

from pyspark.sql.functions import col

# packageSchema = json_data.select('PackageTrace').schema

flattened = json_data.select('*', 'PackageTrace.*')

display(flattened)

# COMMAND ----------

flattened_full = flatten_df(json_data)

display(flattened_full)

# COMMAND ----------


