# Databricks notebook source
# Importing the dlt module
import dlt

# COMMAND ----------

volume_path = '/Volumes/kbarlow/tfs_datadog_poc/json_data/'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load JSON into Raw Table

# COMMAND ----------

json_schema = (
  spark
  .read
  .json(volume_path + 'DistributionPackage.json')
  .schema
)
display(json_schema)

# COMMAND ----------

@dlt.table(table_properties={'quality': 'bronze'})
def raw_data():
  return (
     spark.readStream.format('cloudFiles')
     .option('cloudFiles.format', 'json')
     .load(f'{volume_path}')
 )

# COMMAND ----------


