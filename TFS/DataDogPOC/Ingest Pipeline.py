# Databricks notebook source
# MAGIC %md
# MAGIC ## Load JSON into Raw Table

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Example AutoLoader code
# (spark.readStream.format("cloudFiles")
#   .option("cloudFiles.format", "parquet")
#   # The schema location directory keeps track of your data schema over time
#   .option("cloudFiles.schemaLocation", "<path-to-checkpoint>")
#   .load("<path-to-source-S3>")
#   .writeStream
#   .option("checkpointLocation", "<path-to-checkpoint>")
#   .start("<path_to_S3_volume")
# )

# COMMAND ----------

volume_path = '/Volumes/kbarlow/tfs_datadog_poc/json_data/'

# COMMAND ----------

# DBTITLE 1,Loading the JSON data into DataFrames
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

# DBTITLE 1,Split out data to different DataFrames
serial_df = (json_data
             .select(
              'DataCollectorIdentifier'
              ,'DeviceIdentifier'
              ,'InstanceIdentifier'
              ,'NodeIdentifier'
              ,'SerializedJsonObject'
    ))

package_df = json_data.drop('SerializedJsonObject')

# COMMAND ----------

# DBTITLE 1,Flatten and split out the PackageTrace table
from pyspark.sql.functions import explode

flattened_full = flatten_df(package_df)
exploded = (flattened_full
            .select('*')
            .withColumn('PackageTraceSteps', explode(col('PackageTrace_PackageTraceSteps')))
            .drop('PackageTrace_PackageTraceSteps')
            )

exploded = flatten_df(exploded)
exploded.registerTempTable("temp_packageTrace")

display(exploded)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

serial_df = (
  json_data
    .select(
    'DataCollectorIdentifier'
    ,'DeviceIdentifier'
    ,'InstanceIdentifier'
    ,'NodeIdentifier'
    ,regexp_replace(col('SerializedJsonObject'),'\\\\','').alias('SerializedJsonObjectClean1')
    ,regexp_replace(col('SerializedJsonObjectClean1'),'\"\"','\"').alias('SerializedJsonObjectClean2')
    ,regexp_replace(col('SerializedJsonObjectClean2'),'"\{','\{').alias('SerializedJsonObjectClean')
    )
    # .withColumn('SerializedJsonObject',)
    # .replace('\\\\\\\\','','SerializedJsonObject')
)

# serial_schema = StructType([
#   StructField('InstanceIdentifier',StringType())
#   ,StructField('SerializedData',NullType())
#   ,StructField('SerializedType',StringType())
#   ,StructField('SerializedJsonData',StructType())
# ])

# serial_df = (
#   serial_df
#   .select('*'
#           ,from_json('SerializedJsonObjectClean', serial_schema).alias('SerializedJsonStruct')
#           )
# )

# serial_df = (serial_df
#              .replace('\\','','SerializedJsonObject')

#             #  .select(
#             #    '*'
#             #   #  ,json_tuple('SerializedJsonObject')
#             #    ,from_json('SerializedJsonObject', col('SerializedJsonObject').schema)
#             #  )
#              )

display(serial_df)
display(serial_schema)
