# Databricks notebook source
# MAGIC %md
# MAGIC # AutoLoader Ingestion
# MAGIC
# MAGIC The goal of AutoLoader is to take raw data files that land in S3 and quickly process them into Delta tables. AutoLoader should be thought of as a way to quickly get your data into Delta and Unity Catalog, but not necessarily to do complex transformation.
# MAGIC
# MAGIC Our JSON data exists in an S3 location that is registered to a Unity Catalog Volume. By doing so, we can control access and governance to the raw files, without needing to use Instance Profiles, IAM roles, or Access Keys.

# COMMAND ----------

# DBTITLE 1,AutoLoader Ingestion
# Import functions
from pyspark.sql.functions import col, current_timestamp

# Define variables used in code below
volume_path = '/Volumes/kbarlow/tfs_datadog_poc/json_data/'
table_name = f"kbarlow.tfs_datadog_poc.json_data_raw"
checkpoint_path = f"/tmp/kbarlow/_checkpoint/etl_quickstart"

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

json_schema = (spark
    .read
    # .option("basePath", volume_path)
    .json(volume_path + 'DistributionPackage.json', multiLine=True)
    .schema
)

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .option('multiline', True)
  .schema(json_schema)
  .load(volume_path)
  .select("*"
          ,'PackageTrace.*'
          ,current_timestamp().alias("processing_time")
          )
  .drop('versionofserializeddatastructure')
  .drop('PackageTrace')
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name)
  )
