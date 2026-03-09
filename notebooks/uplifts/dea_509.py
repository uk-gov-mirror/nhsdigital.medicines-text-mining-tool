# Databricks notebook source
# MAGIC %run ../_modules/epma_global/functions

# COMMAND ----------

def add_field_to_existing_table(db: str, uplift_table_name: str):
  if table_exists(db, uplift_table_name) and 'distinct_records' not in spark.table(f'{db}.{uplift_table_name}').columns:
    spark.sql(f"ALTER TABLE {db}.{uplift_table_name} ADD COLUMNS (distinct_records BIGINT)")

# COMMAND ----------

UPLIFT_TABLE_NAME_1 = 'requests'

dbutils.widgets.text('db', '', 'db')
DB = dbutils.widgets.get('db')
assert DB

add_field_to_existing_table(DB, UPLIFT_TABLE_NAME_1)