# Databricks notebook source
# MAGIC %pip install dbdemos

# COMMAND ----------

import dbdemos

dbdemos.help()

# COMMAND ----------

dbdemos.list_demos()

# COMMAND ----------

dbdemos.install("lakehouse-retail-c360"
                , catalog="kbarlow"
                , schema="c360"
                , skip_dashboards=True
                )
