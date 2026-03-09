# Databricks notebook source
# MAGIC %run ../_modules/epma_global/functions

# COMMAND ----------

from uuid import uuid4
from pyspark.sql.types import StructType, StructField, LongType
from pyspark.sql.functions import lit
from typing import List

def recreate_table(db_name:str,
                   table_name:str,
                   fields_to_add: List[StructField] = None,
                   fields_to_remove: List[StructField] = None,
                   fields_order: List = None):  
  
  orig_cols = {*spark.table(f"{db_name}.{table_name}").columns}
  
  #temp name  
  temp_name = table_name + uuid4().hex[0:10]  
  #create temp table including all data  
  spark.sql(f"""CREATE TABLE {db_name}.{temp_name} USING PARQUET AS SELECT * FROM {db_name}.{table_name}""")  

  # uplift data  
  df = spark.table(f"{db_name}.{temp_name}") 

  if fields_to_add:    
    df = df.select("*", *[lit(None).cast(fld.dataType).alias(fld.name) for fld in fields_to_add if fld.name not in orig_cols])  
  if fields_to_remove:    
    df = df.drop([fld.name for fld in fields_to_remove])   

  #drop original table  
  spark.sql(f"""DROP TABLE {db_name}.{table_name}""")
  # apply field order specified
  if fields_order:
    df = df.select(*fields_order)
  # create new table  
  df.write.mode('overwrite').format('parquet').saveAsTable(f"{db_name}.{table_name}")
  # clean up temp table
  spark.sql(f"""DROP TABLE {db_name}.{temp_name}""")   

# COMMAND ----------

UPLIFT_TABLE_NAME = 'requests'
FIELDS_TO_ADD = [
    StructField('distinct_records', LongType(), True)
  ]
FIELDS_ORDER = ['dataset_id','submission_id','distinct_records','submission_date']
                
dbutils.widgets.text('db', '', 'db')
DB = dbutils.widgets.get('db')
assert DB
if table_exists(DB, UPLIFT_TABLE_NAME):
  recreate_table(db_name=DB, table_name=UPLIFT_TABLE_NAME, fields_to_add=FIELDS_TO_ADD, fields_order=FIELDS_ORDER)