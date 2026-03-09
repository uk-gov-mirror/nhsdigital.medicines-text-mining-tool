# Databricks notebook source
import os

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/exception_list

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/constants

# COMMAND ----------

# MAGIC %run ../functions/exceptions_and_preprocessing_functions

# COMMAND ----------

import os
from datetime import datetime
import warnings
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql.dataframe import DataFrame
import functools 

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('notebook_location', './0_exceptions_and_preprocessing/drivers/exceptions_and_preprocessing_driver', 'notebook_location')
dbutils.widgets.text('epma_table', '', 'epma_table')
dbutils.widgets.text('requests_table', '', 'requests_table')
dbutils.widgets.text('responses_table', '', 'responses_table')
dbutils.widgets.text('next_submission_table', '', 'next_submission_table')
dbutils.widgets.text('db', '', 'db')
dbutils.widgets.text('output_table', '', 'output_table')
dbutils.widgets.text('unmappable_table', '', 'unmappable_table')
dbutils.widgets.text('match_lookup_final_table', '', 'match_lookup_final_table')
dbutils.widgets.text('batch_size', '', 'batch_size')
dbutils.widgets.text('run_id', '', 'run_id')

stage = {
  'notebook_path' : os.path.basename(dbutils.widgets.get('notebook_location')),
  'epma_table': dbutils.widgets.get('epma_table'),
  'requests_table': dbutils.widgets.get('requests_table'),
  'responses_table': dbutils.widgets.get('responses_table'),
  'next_submission_table': dbutils.widgets.get('next_submission_table'),
  'db': dbutils.widgets.get('db'),
  'output_table': dbutils.widgets.get('output_table'),
  'unmappable_table': dbutils.widgets.get('unmappable_table'),
  'match_lookup_final_table': dbutils.widgets.get('match_lookup_final_table'),
  'batch_size': int(dbutils.widgets.get('batch_size')),
  'run_id': dbutils.widgets.get('run_id'),
}

# COMMAND ----------

exit_message = []
exit_message.append(f"\n notebook {stage['notebook_path']} execution started  @ {datetime.now().isoformat()}\n")

# COMMAND ----------

# if epma_table was not specified, select the oldest request from the requests table which does not have a corresponding record in the responses table with a status of "success".

# In the responses table, possible statuses are:
# running: The request has already been run and the batch size was less than the number of records available. 
#          The run may have failed, or may have finished, in which case there would also be a response record with status "success". Either way, there are some records still to be processed from this request.
# running_final_batch: The request has already been run and the batch size was more than the number of records available.
#          The run may have failed, or may have finished, in which case there would also be a response record with status "success".
# processing: This means that a batch of records from a request was successfully processed.
# success: This means that all records from a request were successfully processed.

RAW_INPUT_TABLE = stage['epma_table']
BATCH_SIZE = stage['batch_size']

df_responses = get_data(stage['responses_table'])
  
if (RAW_INPUT_TABLE != '') & (RAW_INPUT_TABLE != 'epma_prod_rerun'):
  df_next_submission = (
    spark.createDataFrame(
      [(RAW_INPUT_TABLE, ''),],
      StructType([StructField('dataset_id', StringType(), True), StructField('submission_id', StringType(), True)])
    )
  ).withColumn('run_date', F.current_timestamp())
  
  row = df_next_submission.first()
  df_grouped_submissions = spark.table(stage['db'] + '.' + row.dataset_id)
  
else:
  df_requests = get_data(stage['requests_table'])

  df_completed_requests = df_responses.filter(F.col('status') == 'success')
  
  df_next_submission = (
    df_requests
    .join(df_completed_requests, on=['dataset_id', 'submission_id'], how='left_anti')
    .orderBy(F.col('submission_id'))
    .select('dataset_id', 'submission_id','distinct_records')
    .withColumn('run_date', F.current_timestamp())# timestamp set on execution of the table write due to lazy evaluation
  )
  
  if df_next_submission.first() is not None:
    df_submissions_nulls = df_next_submission.where(F.col('distinct_records').isNull())
    if df_submissions_nulls.count() == 0:
      df_next_submission = (
        df_next_submission
        .withColumn('cumul', F.sum('distinct_records').over(Window.partitionBy().orderBy("submission_id")))
      )# create cumulative count

      if df_next_submission.first().cumul > BATCH_SIZE:
        df_next_submission = df_next_submission.limit(1)
      else:
        df_next_submission = df_next_submission.where(F.col('cumul') <= BATCH_SIZE)
    else:
      df_next_submission = df_submissions_nulls.limit(1)
      
    df_next_submission = df_next_submission.drop(*['distinct_records','cumul'])
    submissions_list = df_next_submission.select('dataset_id','submission_id').collect()# get list of submissions for the next batch
    df_raw_input_tables_list = [spark.table(stage['db'] + '.' + row.dataset_id + '_' + row.submission_id) for row in submissions_list]
    df_grouped_submissions = functools.reduce(DataFrame.union, df_raw_input_tables_list)# union into one table
    exit_message.append(f"\n Data will be read from the oldest pending requests, which are {df_raw_input_tables_list}\n")
  else:
    warnings.warn('There is no entry in the requests table which has not already been run successfully.', RuntimeWarning)

# COMMAND ----------

source_table = df_grouped_submissions

if 'original_epma_description' in source_table.columns:
  src_medication_col = 'original_epma_description'
elif 'Drug' in source_table.columns:
  src_medication_col = 'Drug'
elif 'medication_name_value' in source_table.columns:
  src_medication_col = 'medication_name_value'
  
if FORM_IN_TEXT_COL in source_table.columns:
  HAS_FORM_IN_TEXT_COL = True
else:
  HAS_FORM_IN_TEXT_COL = False
  source_table = source_table.withColumn(FORM_IN_TEXT_COL, lit(' '))
  
# In the ref environment, only records with this EVENT_ID are unhashed
if (os.environ.get('env') == 'ref') and ('META' in source_table.columns):
  source_table = source_table.where(col('META.EVENT_ID').contains('582331:'))

# COMMAND ----------

source_table_without_null = drop_null_in_medication_col(source_table, src_medication_col)
df_match_lookup_final = spark.table(stage['match_lookup_final_table'])
df_unmappable_table = spark.table(stage['unmappable_table'])

df_epma = select_distinct_descriptions(source_table_without_null, src_medication_col=src_medication_col, original_text_col=ORIGINAL_TEXT_COL, form_in_text_col=FORM_IN_TEXT_COL, id_col=ID_COL)
df_selected, RECORDS_EXHAUSTED_FLAG = select_record_batch_to_process(df_epma, df_match_lookup_final, df_unmappable_table, stage['batch_size'], [ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL])

if RECORDS_EXHAUSTED_FLAG == True:
  df_next_submission = df_next_submission.withColumn('status', lit('running_final_batch'))
else:
  df_next_submission = df_next_submission.withColumn('status', lit('running'))

create_table(df_next_submission, stage['next_submission_table'], overwrite=True)

df_next_submission.write.format("delta").mode("append").saveAsTable(stage['responses_table'])

# COMMAND ----------

# Preprocessing.
# Includes creating two versions of the epma_description, one with form_in_text appended and one without.
# In exact matching we try to match to both versions, to increase the chance of getting a match.
# For later stages of the pipeline we only match to the epma_description which has form_in_text appended, to save on processing time.

if HAS_FORM_IN_TEXT_COL:
  df_selected_cleaned = df_selected.df \
    .withColumn(TEXT_WITHOUT_FORM_COL, F.lower(col(ORIGINAL_TEXT_COL))) \
    .withColumn(FORM_IN_TEXT_COL, F.lower(col(FORM_IN_TEXT_COL))) \
    .withColumn(TEXT_WITHOUT_FORM_COL, standardise_interchangeable_words(col(TEXT_WITHOUT_FORM_COL))) \
    .withColumn(TEXT_WITHOUT_FORM_COL, standardise_doseform(col(TEXT_WITHOUT_FORM_COL))) \
    .withColumn(TEXT_WITHOUT_FORM_COL, standardise_drug_name(col(TEXT_WITHOUT_FORM_COL))) \
    .withColumn(TEXT_WITHOUT_FORM_COL, replace_hyphens_between_dosages_with_slashes(col(TEXT_WITHOUT_FORM_COL))) \
    .withColumn(TEXT_WITHOUT_FORM_COL, correct_common_unit_errors(col(TEXT_WITHOUT_FORM_COL))) \
    .withColumn(TEXT_COL, F.when(col(TEXT_WITHOUT_FORM_COL).contains(col(FORM_IN_TEXT_COL)), col(TEXT_WITHOUT_FORM_COL))
                          .when(col(FORM_IN_TEXT_COL) == lit(' '), col(TEXT_WITHOUT_FORM_COL))
                          .when(col(FORM_IN_TEXT_COL).contains('other (add comment)'), col(TEXT_WITHOUT_FORM_COL))
                          .otherwise(F.concat(col(TEXT_WITHOUT_FORM_COL), lit(' '), col(FORM_IN_TEXT_COL))))
  df_remaining, df_unmappable = filter_user_curated_unmappables(df_selected_cleaned, text_col=TEXT_COL, unmappable_regexes=unmappable_inputs)
  
if not HAS_FORM_IN_TEXT_COL:
  df_selected_cleaned = df_selected.df \
    .withColumn(TEXT_WITHOUT_FORM_COL, lit(None).cast(StringType())) \
    .withColumn(TEXT_COL, F.lower(col(ORIGINAL_TEXT_COL))) \
    .withColumn(TEXT_COL, standardise_interchangeable_words(col(TEXT_COL))) \
    .withColumn(TEXT_COL, standardise_doseform(col(TEXT_COL))) \
    .withColumn(TEXT_COL, standardise_drug_name(col(TEXT_COL))) \
    .withColumn(TEXT_COL, replace_hyphens_between_dosages_with_slashes(col(TEXT_COL))) \
    .withColumn(TEXT_COL, correct_common_unit_errors(col(TEXT_COL)))
  df_remaining, df_unmappable = filter_user_curated_unmappables(df_selected_cleaned, text_col=TEXT_COL, unmappable_regexes=unmappable_inputs)                                   

# COMMAND ----------

create_table(df_remaining, stage['output_table'], overwrite=True)

df_unmappable = df_unmappable.select(ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL) \
                             .withColumn(REASON_COL, lit('user_curated_list')) \
                             .withColumn(MATCH_DATETIME_COL, F.current_timestamp()) \
                             .withColumn(RUN_ID_COL, lit(stage['run_id']))

append_to_table(df_unmappable, [ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL], stage['unmappable_table'], allow_nullable_schema_mismatch=True)

df_selected.delete()

# COMMAND ----------

exit_message.append(f"notebook {stage['notebook_path']} execution completed  @ {datetime.now().isoformat()}")
exit_message = [i for i in exit_message if i] 
exit_message = '\n'.join(exit_message)
dbutils.notebook.exit(exit_message)