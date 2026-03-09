# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime
import os
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %run ./../_modules/epma_global/functions

# COMMAND ----------

if os.environ.get("env") == "dev":
  dbutils.notebook.exit("skipping this uplift in the dev environment")

# COMMAND ----------

def update_skip_status_in_prod(db_name:str,
                          replacement_table_name:str,
                          table_name: str):
  """
  This function will replace table_name with the data in replacement_table_name.
  """
  
  new_replacing_table = spark.table(f"{db_name}.{replacement_table_name}")
  new_replacing_table.write \
                     .mode("overwrite") \
                     .insertInto(f"{db_name}.{table_name}", overwrite=True)

# COMMAND ----------

def change_status(db_name, status_table_name, change_to):
  spark = SparkSession.builder.getOrCreate()
  df_status = spark.createDataFrame([(change_to,)], ["status"])

  df_status.write \
           .mode("overwrite") \
           .insertInto(f"{db_name}.{status_table_name}", overwrite=True)

# COMMAND ----------



# COMMAND ----------

SKIP_AUTOCODING_STATUS_SET_TABLE_NAME = "skip_autocoding_status_set"
SKIP_AUTOCODING_STATUS_INITIATE_TABLE_NAME = "skip_autocoding_status_initiate"
STATUS_TABLE_NAME = "skip_autocoding_status_set"

dbutils.widgets.text('db', '', 'db')
DB = dbutils.widgets.get('db')

status = get_status(DB, STATUS_TABLE_NAME)

if status == "skip" or status == "restart":
  update_skip_status_in_prod(DB, SKIP_AUTOCODING_STATUS_SET_TABLE_NAME, SKIP_AUTOCODING_STATUS_INITIATE_TABLE_NAME)
  change_status(DB, SKIP_AUTOCODING_STATUS_SET_TABLE_NAME, "closed")