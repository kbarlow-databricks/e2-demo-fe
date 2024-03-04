# Databricks notebook source
import boto3, zipfile, os, traceback, tempfile

import pandas as pd

import json

import re

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

from pyspark.sql.functions import split, substring,  from_json,col, explode, schema_of_json

from pyspark.sql import SparkSession

from collections.abc import Iterator




AWS_SECRET_ACCESS_KEY = "QSs94QZskKOiuDJ6pgcPkUrBuTAjnaDWoRhXYcM5"

AWS_ACCESS_KEY_ID = "AKIASEYYH3NODHR3MO4A"

#Check if the file has specific value

 

def is_data_collector_package(json_content):

    try:

        pattern = r'"EmbeddedSerializedType"\s*:\s*"MeasurementsNodeManagementPackage"'

 

        found = re.search(pattern, json_content)

        if found:

            return True

        else:

            return False

 

       

        # json_data = json.loads(json_content)

        # return json_data.get("EmbeddedSerializedType") == "MeasurementsNodeManagementPackage"

    except:

        return False





def s3_unzip(itr: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:

    # Cluster must have instance profile associated

    # Otherwise, use Secrets to get S3 access keys

    #s3_client = boto3.client("s3")

    s3_client = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # For every DataFrame:

 

    for df in itr:                                                                                                               ###   ***** see note above at instruction at *****

        # For every row in the DataFrame:

        for (index, row) in df.iterrows():    

                                                                                                           

            bucket_name = row["bucket_name"]

            source_key = row["source_key"]

            row.drop("source_key").drop("bucket_name")

            try:

                tmp_file_name = tempfile.NamedTemporaryFile(delete=False, suffix=".package").name

                s3_client.download_file(bucket_name, source_key, tmp_file_name)

 

                renamed_file_name = tmp_file_name[:-7] + ".zip"

                os.rename(tmp_file_name, renamed_file_name)

 

                # Open the .package file

                with zipfile.ZipFile(renamed_file_name, "r") as zip_ref:

                    id_iter = 0

                    for file_info in zip_ref.infolist():

                        file_data = zip_ref.read(file_info.filename).rstrip()

 

                        raw_content = file_data.decode("utf-8")

                       

                        if is_data_collector_package(raw_content):

                            row["raw_content"] = raw_content

                            row["file_name"] = file_info.filename

                            row["file_id"] = source_key.replace("/", "_").replace(".", "_") + str(id_iter)

                            #row["file_id"] = source_key + str(id_iter)

                            print(row["file_name"])

                            id_iter += 1

                       

                       

                            # For each file unpacked and identified as data collector package, emit a new row

                            yield row.to_frame().transpose()

                       

                # Clean up local file

                os.remove(renamed_file_name)

                   

            except:

                # Match the expected return format with errors for ease of debugging

                row["raw_content"], row["file_name"], row["file_id"] = (

                    traceback.format_exc(),

                    traceback.format_exc(),

                    traceback.format_exc(),

                )

                yield row.to_frame().transpose()

 

#The fields we want to load to the DLT table

unzip_schema = StructType(

    [

        StructField("path", StringType(), True),

        StructField("modificationTime", TimestampType(), True),

        StructField("length", LongType(), True),

        StructField("file_name", StringType(), True),

        StructField("raw_content", StringType(), True),

        StructField("file_id", StringType(), True),

    ]

)

 

# COMMAND ----------

 

s3_source = "s3://dc-distributionpackages-production/distributionpackages/CentralProduction/2024/2/27"

#s3_source = "s3://aig-msd-distributionpackages-poc"

 

#Autoloader

 

data = (

  spark.readStream

      .format('cloudFiles')

      .option('cloudFiles.format', "BINARYFILE")

    #   .option("cloudFiles.useNotifications", "true")

    #   .option("cloudFiles.autoIngestEnabled", "true")

      .option("cloudFiles.includeExistingFiles", "true")

      .load(s3_source)

      .select("path", "modificationTime", "length")

    #   .withColumn("bucket_name", split("path",'/').getItem(1))

    #   .withColumn("source_key", split("path",'/',limit=4).getItem(1))

      .withColumn("bucket_name", split("path",'/').getItem(2))

      .withColumn("source_key", split("path",'/',limit=4).getItem(3))

      .mapInPandas(s3_unzip, schema=unzip_schema)

      )

   

 

#write to dlt table

## must be replaced with @dlt.table once DLT Workflows works

 

(

     #transformationonjsonondata.writeStream

    data.writeStream

    .queryName("Autoloader")

    .outputMode("append")

    .format('delta')

    .option("checkpointLocation", "/tmp/delta/_checkpoints/")

    .toTable('mock.bronze.testpackageswithrawdata')

   

)

 

# import dlt

 

# @dlt.table

# def testpackageswithrawdata():

#   return spark.writeStream.format("delta").option("checkpointLocation", "/tmp/delta/_checkpoints/")




data.select("path").display()
