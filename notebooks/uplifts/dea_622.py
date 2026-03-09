# Databricks notebook source
import os
if os.environ.get("env") == "dev":
  dbutils.notebook.exit("skipping this uplift in the dev environment")

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime

def update_match_table(db_name:str,
                       table_name:str,
                       date_stamp: str,
                       vtm_hist_name: str,
                       vmp_hist_name: str):  
  """
  This function saves a version of the match_lookup_table with the current date in the name 
  and updates the original match_lookup_table with any updated match_ids from the vtm_history and vmp_history tables
  """
  
  temp_name = f"{table_name}_{date_stamp}"
  
  spark.sql(f"""CREATE TABLE {db_name}.{temp_name} USING PARQUET AS SELECT * FROM {db_name}.{table_name}""")
  
  vtm_history = spark.table(f"dss_corporate.{vtm_hist_name}")
  vmp_history = spark.table(f"dss_corporate.{vmp_hist_name}")
  match_lookup = spark.table(f"{db_name}.{temp_name}")
  
  vtm_and_vmp_history = vtm_history.union(vmp_history)
  window_for_history = Window.partitionBy("IDPREVIOUS").orderBy(vtm_and_vmp_history["STARTDT"].desc())
  history_deduped = vtm_and_vmp_history.withColumn("row_num",F.row_number().over(window_for_history)).filter("row_num = 1").drop("row_num")
  
  updated_ml = match_lookup.join(history_deduped, match_lookup.match_id == history_deduped.IDPREVIOUS, 'left')
  updated_ml = (
    updated_ml
    .withColumn('match_id', F.when(updated_ml.IDCURRENT.isNull(), updated_ml.match_id).otherwise(updated_ml.IDCURRENT))
    .drop(*['IDCURRENT','IDPREVIOUS','STARTDT','ENDDT'])
  )
  
  original_count = match_lookup.count()
  new_count = updated_ml.count()
  
  if original_count == new_count:
    updated_ml.write.mode('overwrite').format('parquet').saveAsTable(f"{db_name}.{table_name}")
    

# COMMAND ----------

TABLE_NAME = 'match_lookup_final'
DATE = datetime.today().strftime('%Y_%m_%d_%H%M')
VTM_HIST = 'vtm_history'
VMP_HIST = 'vmp_history'

dbutils.widgets.text('db', '', 'db')
DB = dbutils.widgets.get('db')

update_match_table(DB, TABLE_NAME, DATE, VTM_HIST, VMP_HIST)