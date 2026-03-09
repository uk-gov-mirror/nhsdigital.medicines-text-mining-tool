# Databricks notebook source
dbutils.widgets.text('db', '', 'db')
DB = dbutils.widgets.get('db')
assert DB

dbutils.widgets.text('uplift_notebook', '', 'uplift_notebook')
UPLIFT_NOTEBOOK = dbutils.widgets.get('uplift_notebook')

dbutils.widgets.text('uplift_table', 'match_lookup_final', 'uplift_table')
UPLIFT_TABLE = dbutils.widgets.get('uplift_table')

dbutils.widgets.text('match_lookup_final_table_name', 'match_lookup_final', 'match_lookup_final_table_name')
MATCH_LOOKUP_FINAL_TABLE_NAME = dbutils.widgets.get('match_lookup_final_table_name')
assert MATCH_LOOKUP_FINAL_TABLE_NAME

dbutils.widgets.text('unmappable_table_name', 'unmappable', 'unmappable_table_name')
UNMAPPABLE_TABLE_NAME = dbutils.widgets.get('unmappable_table_name')
assert UNMAPPABLE_TABLE_NAME

dbutils.widgets.text('accuracy_table_name', 'accuracy', 'accuracy_table_name')
ACCURACY_TABLE_NAME = dbutils.widgets.get('accuracy_table_name')
assert ACCURACY_TABLE_NAME

dbutils.widgets.text('requests_table_name', 'requests', 'requests_table_name')
REQUESTS_TABLE_NAME = dbutils.widgets.get('requests_table_name')
assert REQUESTS_TABLE_NAME

dbutils.widgets.text('responses_table_name', 'responses', 'responses_table_name')
RESPONSES_TABLE_NAME = dbutils.widgets.get('responses_table_name')
assert RESPONSES_TABLE_NAME

dbutils.widgets.text('replacement_match_lookup_final_table_name', 'replacement_match_lookup_final', 'replacement_match_lookup_final_table_name')
REPLACEMENT_MATCH_LOOKUP_FINAL_TABLE_NAME = dbutils.widgets.get('replacement_match_lookup_final_table_name')
assert REPLACEMENT_MATCH_LOOKUP_FINAL_TABLE_NAME

dbutils.widgets.text('replacement_unmappable_table_name', 'replacement_unmappable', 'replacement_unmappable_table_name')
REPLACEMENT_UNMAPPABLE_TABLE_NAME = dbutils.widgets.get('replacement_unmappable_table_name')
assert REPLACEMENT_UNMAPPABLE_TABLE_NAME

dbutils.widgets.text('replacement_status_table_name', 'replacement_status', 'replacement_status_table_name')
REPLACEMENT_STATUS_TABLE_NAME = dbutils.widgets.get('replacement_status_table_name')
assert REPLACEMENT_STATUS_TABLE_NAME

dbutils.widgets.text('reviewed_matches_table_name', 'reviewed_matches', 'reviewed_matches_table_name')
REVIEWED_MATCHES_TABLE_NAME = dbutils.widgets.get('reviewed_matches_table_name')
assert REVIEWED_MATCHES_TABLE_NAME

dbutils.widgets.text('skip_autocoding_status_set_table_name', 'skip_autocoding_status_set', 'skip_autocoding_status_set_table_name')
SKIP_AUTOCODING_STATUS_SET_TABLE_NAME = dbutils.widgets.get('skip_autocoding_status_set_table_name')
assert SKIP_AUTOCODING_STATUS_SET_TABLE_NAME

dbutils.widgets.text('skip_autocoding_status_initiate_table_name', 'skip_autocoding_status_initiate', 'skip_autocoding_status_initiate_table_name')
SKIP_AUTOCODING_STATUS_INITIATE_TABLE_NAME = dbutils.widgets.get('skip_autocoding_status_initiate_table_name')
assert SKIP_AUTOCODING_STATUS_INITIATE_TABLE_NAME

# COMMAND ----------

# MAGIC %run ./notebooks/_modules/epma_global/functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType, DateType

# COMMAND ----------

OVERWRITE = True if DB == '__test_epma_autocoding' else False

# COMMAND ----------

if UPLIFT_NOTEBOOK != '':
  dbutils.notebook.run('./notebooks/uplifts/' + UPLIFT_NOTEBOOK, 0, {'db': DB,
                                                                     'uplift_table_name': DB + '.' + UPLIFT_TABLE,
                                                                     'match_lookup_final_table_name': MATCH_LOOKUP_FINAL_TABLE_NAME,
                                                                     'unmappable_table_name': UNMAPPABLE_TABLE_NAME,
                                                                     'accuracy_table_name': ACCURACY_TABLE_NAME,
                                                                     'requests_table_name': REQUESTS_TABLE_NAME,
                                                                     'responses_table_name': RESPONSES_TABLE_NAME,
                                                                     'replacement_match_lookup_final_table_name': REPLACEMENT_MATCH_LOOKUP_FINAL_TABLE_NAME,
                                                                     'replacement_unmappable_table_name': REPLACEMENT_UNMAPPABLE_TABLE_NAME,
                                                                     'replacement_status_table_name': REPLACEMENT_STATUS_TABLE_NAME,
                                                                     'reviewed_matches_table_name': REVIEWED_MATCHES_TABLE_NAME,
                                                                     'skip_autocoding_status_set_table_name': SKIP_AUTOCODING_STATUS_SET_TABLE_NAME,
                                                                     'skip_autocoding_status_initiate_table_name': SKIP_AUTOCODING_STATUS_INITIATE_TABLE_NAME,
                                                                    })

# COMMAND ----------

schema_match_lookup_final = StructType([
  StructField('original_epma_description', StringType(), True, {'comment': 'Input description'}),
  StructField('form_in_text', StringType()),
  StructField('match_id', StringType(), True),
  StructField('match_term', StringType(), True),
  StructField('id_level', StringType(), True, {'comment': 'Level of dm+d match ie VMP, AMP, or VPID'}),
  StructField('match_level', StringType(), True),
  StructField('match_datetime', TimestampType(), True, {'comment': 'Date and time matched'}),
  StructField('version_id', StringType(), True),
  StructField('run_id', StringType(), True),
])

create_table_from_schema(schema_match_lookup_final, DB, table=MATCH_LOOKUP_FINAL_TABLE_NAME, overwrite=OVERWRITE)

# COMMAND ----------

schema_unmappable = StructType([
  StructField('original_epma_description', StringType(), True, {'comment': 'Input description'}),
  StructField('form_in_text', StringType()),
  StructField('reason', StringType(), True),
  StructField('match_datetime', TimestampType(), True),
  StructField('run_id', StringType(), True),
])

create_table_from_schema(schema_unmappable, DB, table=UNMAPPABLE_TABLE_NAME, overwrite=OVERWRITE)

# COMMAND ----------

schema_accuracy = StructType([
  StructField('pipeline_match_id_level', StringType(), True, {'comment': 'Pipeline match id level'}),
  StructField('pipeline_match_level', StringType(), True, {'comment': 'Pipeline match level'}),
  StructField('source_match_id_level', StringType(), True, {'comment': 'Source match id level'}),
  StructField('pipeline_mismatch', StringType(), True, {'comment': 'Reason for pipeline mismatch'}),
  StructField('total_match_count', LongType(), True, {'comment': 'Total match count'}),
  StructField('match_datetime', TimestampType(), False, {'comment': 'Date and time matched'}),
  StructField('run_id', StringType(), False)
])

create_table_from_schema(schema_accuracy, DB, table=ACCURACY_TABLE_NAME, overwrite=OVERWRITE, allow_nullable_schema_mismatch=True)

# COMMAND ----------

schema_requests = StructType([
  StructField('dataset_id', StringType(), True),
  StructField('submission_id', StringType(), True),
  StructField('distinct_records', LongType(), True),
  StructField('submission_date', DateType(), True)
])

create_table_from_schema(schema_requests, DB, table=REQUESTS_TABLE_NAME, overwrite=OVERWRITE, allow_nullable_schema_mismatch=True, format_type="delta")

# COMMAND ----------

schema_responses = StructType([
  StructField('dataset_id', StringType(), True),
  StructField('submission_id', StringType(), True),
  StructField('run_date', TimestampType(), True),
  StructField('status', StringType(), True)
])

create_table_from_schema(schema_responses, DB, table=RESPONSES_TABLE_NAME, overwrite=OVERWRITE, allow_nullable_schema_mismatch=True, format_type="delta")

# COMMAND ----------

schema_replacement_match_lookup_final = StructType([
  StructField('original_epma_description', StringType(), True),
  StructField('form_in_text', StringType()),
  StructField('match_id', StringType(), True),
  StructField('match_term', StringType(), True),
  StructField('id_level', StringType(), True),
  StructField('match_level', StringType(), True),
  StructField('match_datetime', TimestampType(), True),
  StructField('version_id', StringType(), True),
  StructField('run_id', StringType(), True),
])

create_table_from_schema(schema_replacement_match_lookup_final, DB, table=REPLACEMENT_MATCH_LOOKUP_FINAL_TABLE_NAME, overwrite=OVERWRITE)

# COMMAND ----------

schema_replacement_unmappable = StructType([
  StructField('original_epma_description', StringType(), True),
  StructField('form_in_text', StringType()),
  StructField('reason', StringType(), True),
  StructField('match_datetime', TimestampType(), True),
  StructField('run_id', StringType(), True),
])

create_table_from_schema(schema_replacement_unmappable, DB, table=REPLACEMENT_UNMAPPABLE_TABLE_NAME, overwrite=OVERWRITE)

# COMMAND ----------

schema_replacement_status = StructType([
  StructField('status', StringType(), True),
])

create_table_from_schema(schema_replacement_status, DB, table=REPLACEMENT_STATUS_TABLE_NAME, overwrite=OVERWRITE)

# COMMAND ----------

schema_reviewed_matches = StructType([
  StructField('original_epma_description', StringType(), True),
  StructField('form_in_text', StringType()),
  StructField('match_id', StringType(), True),
  StructField('match_term', StringType(), True),
  StructField('score', StringType(), True),
  StructField('comments', StringType(), True),
  StructField('episode_name', StringType(), True),
])

create_table_from_schema(schema_reviewed_matches, DB, table=REVIEWED_MATCHES_TABLE_NAME, overwrite=OVERWRITE)

# COMMAND ----------

schema_skip_autocoding_status_set = StructType([
  StructField('status', StringType(), True),
])

create_table_from_schema(schema_skip_autocoding_status_set, DB, table=SKIP_AUTOCODING_STATUS_SET_TABLE_NAME, overwrite=OVERWRITE)

# COMMAND ----------

schema_skip_autocoding_status_initiate = StructType([
  StructField('status', StringType(), True),
])

create_table_from_schema(schema_skip_autocoding_status_initiate, DB, table=SKIP_AUTOCODING_STATUS_INITIATE_TABLE_NAME, overwrite=OVERWRITE)

# COMMAND ----------

status = get_status(DB, REPLACEMENT_STATUS_TABLE_NAME)

if status == "ready":
  dbutils.notebook.run('./notebooks/uplifts/replace_match_and_unmap_in_prod', 0, {'db': DB,
                                                                     'uplift_table_name': DB + '.' + UPLIFT_TABLE,
                                                                     'match_lookup_final_table_name': MATCH_LOOKUP_FINAL_TABLE_NAME,
                                                                     'unmappable_table_name': UNMAPPABLE_TABLE_NAME,
                                                                     'accuracy_table_name': ACCURACY_TABLE_NAME,
                                                                     'requests_table_name': REQUESTS_TABLE_NAME,
                                                                     'responses_table_name': RESPONSES_TABLE_NAME,
                                                                     'replacement_match_lookup_final_table_name': REPLACEMENT_MATCH_LOOKUP_FINAL_TABLE_NAME,
                                                                     'replacement_unmappable_table_name': REPLACEMENT_UNMAPPABLE_TABLE_NAME,
                                                                     'replacement_status_table_name': REPLACEMENT_STATUS_TABLE_NAME,
                                                                     'reviewed_matches_table_name': REVIEWED_MATCHES_TABLE_NAME,
                                                                     'skip_autocoding_status_set_table_name': SKIP_AUTOCODING_STATUS_SET_TABLE_NAME,
                                                                     'skip_autocoding_status_initiate_table_name': SKIP_AUTOCODING_STATUS_INITIATE_TABLE_NAME,
                                                                    })
  

# COMMAND ----------

status = get_status(DB, SKIP_AUTOCODING_STATUS_SET_TABLE_NAME)

if status == "skip" or status == "restart":
  dbutils.notebook.run('./notebooks/uplifts/skip_autocoding', 0, {'db': DB,
                                                                     'uplift_table_name': DB + '.' + UPLIFT_TABLE,
                                                                     'match_lookup_final_table_name': MATCH_LOOKUP_FINAL_TABLE_NAME,
                                                                     'unmappable_table_name': UNMAPPABLE_TABLE_NAME,
                                                                     'accuracy_table_name': ACCURACY_TABLE_NAME,
                                                                     'requests_table_name': REQUESTS_TABLE_NAME,
                                                                     'responses_table_name': RESPONSES_TABLE_NAME,
                                                                     'replacement_match_lookup_final_table_name': REPLACEMENT_MATCH_LOOKUP_FINAL_TABLE_NAME,
                                                                     'replacement_unmappable_table_name': REPLACEMENT_UNMAPPABLE_TABLE_NAME,
                                                                     'replacement_status_table_name': REPLACEMENT_STATUS_TABLE_NAME,
                                                                     'reviewed_matches_table_name': REVIEWED_MATCHES_TABLE_NAME,
                                                                     'skip_autocoding_status_set_table_name': SKIP_AUTOCODING_STATUS_SET_TABLE_NAME,
                                                                     'skip_autocoding_status_initiate_table_name': SKIP_AUTOCODING_STATUS_INITIATE_TABLE_NAME,
                                                                    })