# Databricks notebook source
dbutils.widgets.text('db', '', 'db')
DB = dbutils.widgets.get('db')
assert DB

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {DB}.responses")