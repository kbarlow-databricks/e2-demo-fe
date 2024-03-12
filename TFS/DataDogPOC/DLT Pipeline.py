# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables Pipeline
# MAGIC
# MAGIC Delta Live Tables (DLT) is a great tool to use when we need to process and transform our data, without needing to write the specific logic for data transformation. I will be using the Python API so I can include some more complex logic. For other, more simple pipelines, you can also use the SQL API.

# COMMAND ----------

# DBTITLE 1,Set variables, functions, and schema
# # Import functions
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# import dlt

# # Define variables used in code below
# volume_path = '/Volumes/kbarlow/tfs_datadog_poc/json_data/'
# checkpoint_path = f"/tmp/kbarlow/_checkpoint/etl_quickstart"

# # Clear out data from previous demo execution
# # spark.sql(f"DROP TABLE IF EXISTS kbarlow.tfs_datadog_poc.json_data_raw")
# # dbutils.fs.rm(checkpoint_path, True)

# json_schema = (spark
#     .read
#     .json(volume_path + 'DistributionPackage.json', multiLine=True)
#     .schema
# )

# COMMAND ----------

# DBTITLE 1,Ingest Files with AutoLoader
@dlt.table(
  name="json_data_raw",
  comment="Table to collect the microscope data into a Delta table.",
  # spark_conf={"<key>" : "<value", "<key" : "<value>"},
  # table_properties={"<key>" : "<value>", "<key>" : "<value>"},
  # path="<storage-location-path>",
  # partition_cols=["<partition-column>", "<partition-column>"],
  # schema="schema-definition",
  temporary=False
)
def json_data_raw():
  return (
    spark.readStream
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
    .drop('versionofserializeddatastructure', 'PackageTrace', 'SerializedJsonObject')
    # .writeStream
    # .option("checkpointLocation", checkpoint_path)
    # .trigger(availableNow=True)
    # .toTable("kbarlow.tfs_datadog_poc.json_data_raw")
  )

# COMMAND ----------

# DBTITLE 1,Package Trace Table
@dlt.table(
  name="package_trace_bronze",
  comment="Table for the package trace information specifically. Will focus on the delivery details of each JSON package.",
  # spark_conf={"<key>" : "<value", "<key" : "<value>"},
  # table_properties={"<key>" : "<value>", "<key>" : "<value>"},
  # path="<storage-location-path>",
  # partition_cols=["<partition-column>", "<partition-column>"],
  # schema="schema-definition",
  temporary=False
)
@dlt.expect_all({"valid_id": "DeviceIdentifier IS NOT NULL AND InstanceIdentifier IS NOT NULL"})
def package_trace_bronze():
  return(
    dlt.read_stream('json_data_raw')
    .select('*'
            ,'DistributionDetails.*'
            ,explode('PackageTraceSteps').alias('StepsStruct'))
    .select('*'
            ,'StepsStruct.*')
    .select('*'
            ,'Source.*')
    .drop('SerializedJsonObjectClean','processing_time','DistributionDetails','PackageTraceSteps'
          ,'Source','StepsStruct')
  )

# COMMAND ----------

# DBTITLE 1,Serialized JSON Table
@dlt.table(
  name="serial_json_bronze",
  comment="Table to parse out the Serialized JSON data.",
  # spark_conf={"<key>" : "<value", "<key" : "<value>"},
  # table_properties={"<key>" : "<value>", "<key>" : "<value>"},
  # path="<storage-location-path>",
  # partition_cols=["<partition-column>", "<partition-column>"],
  # schema="schema-definition",
  temporary=False
)
@dlt.expect_all({"valid_id": "DeviceIdentifier IS NOT NULL AND InstanceIdentifier IS NOT NULL"})
def serial_data_bronze():
  return(
    dlt.read_stream('json_data_raw')
    .select(
      'DataCollectorIdentifier'
      ,'DeviceIdentifier'
      ,'InstanceIdentifier'
      ,'NodeIdentifier'
      ,'SerializedJsonObjectClean'
    )
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building additional downstream views

# COMMAND ----------

@dlt.table (
  name = 'distribution',
  comment='View to see all data packages that have been distributed',
)
def dist_view ():
  return (
    dlt.read_stream('package_trace_bronze')
    .select(
      'PackageIdentifier'
      ,'DistributionFinished'
      ,'OccurredAt'
      ,'Mailbox'
      ,'RecipientMail'
      ,'SenderMail'
      ,'Type'
    )
    .filter(col('DistributionFinished').isNotNull())
  )

# COMMAND ----------

@dlt.table (
  name = 'transport',
  comment='View to see all data packages that have been transported.',
)
def dist_view ():
  return (
    dlt.read_stream('package_trace_bronze')
    .select(
      'DeviceIdentifier'
      ,'InstanceIdentifier'
      ,'PackageIdentifier'
      ,'DistributionFinished'
      ,'Identifier'
      ,'OccurredAt'
      ,'Mailbox'
      ,'RecipientMail'
      ,'SenderMail'
      ,'Type'
    )
    .filter(col('Identifier')=='Transport')
  )

# COMMAND ----------

# DBTITLE 1,Join Device Data
@dlt.table(
  name = 'device_data',
  comment = 'List of all devices with their IDs. For enriching sensor datasets.'
)
def devices():
  return(
    spark
    .read
    .option('header','True')
    .csv('/Volumes/kbarlow/tfs_datadog_poc/device_data')
  )

# COMMAND ----------

@dlt.table(
  name = 'device_transport',
  comment = 'Devices in transport'
)
def device_transport():
  return(
    dlt.read_stream('transport')
    .join(dlt.read('device_data'), ['DeviceIdentifier','InstanceIdentifier'], 'left')

  )
