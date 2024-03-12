# Databricks notebook source
# Import functions
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import dlt

# Define variables used in code below
volume_path = '/Volumes/kbarlow/tfs_datadog_poc/json_data/'
checkpoint_path = f"/tmp/kbarlow/_checkpoint/etl_quickstart"

# Clear out data from previous demo execution
# spark.sql(f"DROP TABLE IF EXISTS kbarlow.tfs_datadog_poc.json_data_raw")
# dbutils.fs.rm(checkpoint_path, True)

json_schema = (spark
    .read
    .json(volume_path + 'DistributionPackage.json', multiLine=True)
    .schema
)

# COMMAND ----------

packageTrace_schema = StructType(
    [
        StructField("CompressedPackageSize", IntegerType(), True),
        StructField("DeviceJsonContainer", StringType(), True),
        StructField(
            "DistributionDetails",
            StructType(
                [
                    StructField("DistributionFinished", StringType(), True),
                    StructField(
                        "Source",
                        StructType(
                            [
                                StructField("Mailbox", StringType(), True),
                                StructField("ReceivedAt", StringType(), True),
                                StructField("RecipientMail", StringType(), True),
                                StructField("SenderMail", StringType(), True),
                                StructField("SenderName", StringType(), True),
                                StructField("Type", IntegerType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField("PackageIdentifier", StringType(), True),
        StructField(
            "PackageTraceSteps",
            ArrayType(
                StructType(
                    [
                        StructField("Identifier", StringType(), True),
                        StructField("OccurredAt", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField("SessionDetails", StringType(), True),
        StructField("VersionOfSerializedDataStructure", IntegerType(), True),
    ]
)

# COMMAND ----------

def parseJSONCols(df, *cols, sanitize=True):
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
    for i in cols:

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
