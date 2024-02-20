# Databricks notebook source
# MAGIC %pip install mlflow

# COMMAND ----------

import mlflow
mlflow.set_registry_uri('databricks-uc')
#                                                                                                               Stage/version  
#                                                                      Model name                                         |        
#                                                                                         |                               |        
predict_maintenance_udf = mlflow.pyfunc.spark_udf(spark, "models:/kbarlow.iot.dbdemos_turbine_maintenance@prod", "string") #, env_manager='virtualenv'
spark.udf.register("kbarlow.iot.predict_maintenance", predict_maintenance_udf)
#Note that this cell is just as example (dlt will ignore it), python needs to be on a separate notebook and the real udf is declared in the companion UDF notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION kbarlow.iot.predict_maintenance(s STRING) 
# MAGIC RETURNS STRING 
# MAGIC AS $$
# MAGIC   import mlflow
# MAGIC   mlflow.set_registry_uri('databricks-uc')
# MAGIC   predict_maintenance_udf = mlflow.pyfunc.spark_udf(spark, "models:/kbarlow.iot.dbdemos_turbine_maintenance@prod", "string")
# MAGIC   spark.udf.register("kbarlow.iot.predict_maintenance", predict_maintenance_udf)
# MAGIC $$
