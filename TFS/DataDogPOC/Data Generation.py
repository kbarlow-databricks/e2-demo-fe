# Databricks notebook source
import pandas as pd
import os

# COMMAND ----------

volume_path = '/Volumes/kbarlow/tfs_datadog_poc/json_data/'

# COMMAND ----------

# dbutils.fs.rm(volume_path + '/generated/', True)

# COMMAND ----------

df = spark.read.json(volume_path, multiLine=True)

display(df)

# COMMAND ----------

filenames = os.listdir(volume_path + '/generated/')
max_value = int(filenames[-1:][0].strip('DistributionPackage.json'))

print(max_value)

# COMMAND ----------

from pyspark.sql.functions import *

df = spark.read.json(volume_path, multiLine=True)

id_cols = ['DataCollectorIdentifier','DeviceIdentifier']

for i in range(max_value + 1, max_value + 751):
  # i = 1
  pad = str(i).rjust(12,'0')

  for colName in id_cols:
    # colName = id_cols[0]

    df = df.withColumn(colName, concat(lit('00000000-0000-0000-0000-'), lit(pad)))

  pdf = df.toPandas()

  pdf.to_json(volume_path + f'/generated/DistributionPackage{pad}.json', orient='records', force_ascii=False, lines=True)
