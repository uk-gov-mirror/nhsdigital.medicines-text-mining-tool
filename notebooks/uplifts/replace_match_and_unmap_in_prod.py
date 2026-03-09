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

def replace_table_in_prod(db_name:str,
                          replacement_table_name:str,
                          table_name: str):
  """
  This function will replace match_lookup_final with the data in replacement_match_lookup_final.
  """
  
  date_stamp = datetime.today().strftime("%Y_%m_%d_%H%M")
  
  backup_name = f"{table_name}_{date_stamp}"
  
  spark.sql(f"""CREATE TABLE {db_name}.{backup_name} USING PARQUET AS SELECT * FROM {db_name}.{table_name}""")
  
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

REPLACEMENT_MATCH_LOOKUP_FINAL_TABLE_NAME = "replacement_match_lookup_final"
MATCH_LOOKUP_FINAL_TABLE_NAME = "match_lookup_final"
REPLACEMENT_UNMAPPABLE_TABLE_NAME = "replacement_unmappable"
UNMAPPABLE_TABLE_NAME = "unmappable"
STATUS_TABLE_NAME = "replacement_status"

dbutils.widgets.text('db', '', 'db')
DB = dbutils.widgets.get('db')

status = get_status(DB, STATUS_TABLE_NAME)

if status == "ready":
  replace_table_in_prod(DB, REPLACEMENT_MATCH_LOOKUP_FINAL_TABLE_NAME, MATCH_LOOKUP_FINAL_TABLE_NAME)
  replace_table_in_prod(DB, REPLACEMENT_UNMAPPABLE_TABLE_NAME, UNMAPPABLE_TABLE_NAME)
  change_status(DB, STATUS_TABLE_NAME, "closed")
