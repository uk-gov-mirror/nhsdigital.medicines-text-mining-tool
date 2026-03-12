# Databricks notebook source
# MAGIC %run ../notebooks/_modules/epma_global/integration_test_helpers

# COMMAND ----------

# MAGIC %run ../notebooks/_pipeline_execution/run_matching_pipeline

# COMMAND ----------

dbutils.widgets.text('uplift_notebook', INT_TEST_UPLIFT_NOTEBOOK, 'uplift_notebook')
UPLIFT_NOTEBOOK = dbutils.widgets.get('uplift_notebook')
assert UPLIFT_NOTEBOOK

# COMMAND ----------

DB = 'test_epma_autocoding'

RAW_INPUT_TABLE = str('') # set this as an empty string if you want the raw input table to be drawn from the requests table below.

GROUND_TRUTH_TABLE = 'epma.epmawspc2'
assert GROUND_TRUTH_TABLE

CURATED_GROUND_TRUTH_TABLE = 'test_epma_autocoding.source_b_gt'
assert CURATED_GROUND_TRUTH_TABLE

CURATED_ACCURACY_BASELINE_TABLE = 'test_epma_autocoding.source_b_baseline_2021_10_27'
assert CURATED_ACCURACY_BASELINE_TABLE

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType
from datetime import date

schema_requests = StructType([
  StructField('dataset_id', StringType(), True),
  StructField('submission_id', StringType(), True),
  StructField('submission_date', DateType(), True)
])

schema_responses = StructType([
  StructField('dataset_id', StringType(), True),
  StructField('submission_id', StringType(), True),
  StructField('status', StringType(), True)
])

requests_df = spark.createDataFrame([('source_b', 'sample_3', date(2022, 5, 5)),
                                     ('source_b', '4k', date(2022, 5, 1)),
                                     ('source_a', '15k', date(2022, 4, 10)),
                                     ('source_a', '1k', date(2022, 4, 6))],
                                     schema_requests)

responses_df = spark.createDataFrame([], schema_responses)

REQUESTS_TABLE = f'{DB}.requests'
RESPONSES_TABLE = f'{DB}.responses'

requests_df.write.mode('overwrite').saveAsTable(REQUESTS_TABLE)
responses_df.write.mode('overwrite').saveAsTable(RESPONSES_TABLE)

# COMMAND ----------

# ATTRS_TO_SAVE = ['match_lookup_final_table', 'unmappable_table']
ATTRS_TO_SAVE = None

# COMMAND ----------

def init_schemas_integration_test(db, uplift_notebook_asset, match_lookup_final_asset, unmappable_table_asset, accuracy_table_asset, requests_table_asset, responses_table_asset, uplift_table_asset):
  match_lookup_final_table_name = match_lookup_final_asset.split('.')[1]
  unmappable_table_name = unmappable_table_asset.split('.')[1]
  accuracy_table_name = accuracy_table_asset.split('.')[1]
  requests_table_name = requests_table_asset.split('.')[1]
  responses_table_name = responses_table_asset.split('.')[1]
  uplift_notebook_name = uplift_notebook_asset.split('.')[1]
  uplift_table_name = uplift_table_asset.split('.')[1] 
 
  dbutils.notebook.run('./../init_schemas', 0, {
    'db' : db,
    'match_lookup_final_table_name': match_lookup_final_table_name,
    'unmappable_table_name': unmappable_table_name,
    'accuracy_table_name': accuracy_table_name,
    'requests_table_name': requests_table_name,  # Only here so that init_schemas has all the required parameters. The real requests table for this integration test notebook is defined as REQUESTS_TABLE above.
    'responses_table_name': responses_table_name, # Only here so that init_schemas has all the required parameters. The real responses table for this integration test notebook is defined as RESPONSES_TABLE above.
    'uplift_notebook': uplift_notebook_name,
    'uplift_table': uplift_table_name
  })

# COMMAND ----------

with IntegrationTestConfig(DB, UPLIFT_NOTEBOOK, ATTRS_TO_SAVE) as ct:
 
  init_schemas_integration_test(ct.target_db, ct.uplift_notebook, ct.match_lookup_final_table, ct.unmappable_table, ct.accuracy_table, ct.requests_table, ct.responses_table, ct.uplift_table) 
  
  if not table_exists(ct.target_db, ct.uplift_table.split('.')[1]):
       raise AssertionError(f'INFO: Uplift table {ct.uplift_table} does not exist')

  PIPELINE_CONFIG = [
    { # Raw data inputs. Must be the zeroth stage
      'epma_table': RAW_INPUT_TABLE,    
      'vtm_table': 'dss_corporate.vtm', 
      'vmp_table': 'dss_corporate.vmp',
      'amp_table': 'dss_corporate.amp',
      'parsed_vtm_table': '',
      'parsed_vmp_table': 'dss_corporate.vmp_parsed',
      'parsed_amp_table': 'dss_corporate.amp_parsed',
      'requests_table': REQUESTS_TABLE,
      'responses_table': RESPONSES_TABLE,
      'db': DB
    },
    {
      'stage_id': 'exceptions_and_preprocessing',
      'notebook_location': '../notebooks/0_exceptions_and_preprocessing/drivers/exceptions_and_preprocessing_driver',
      'raw_data_required': True,
      'unmappable_table': ct.unmappable_table,
      'output_table': ct._inter_preprocessed_inputs,
      'match_lookup_final_table': ct.match_lookup_final_table,
      'next_submission_table': ct._inter_next_submission,
      'batch_size': '10',
      'run_id': '666',
      'execute': True,
    },
    {
      'stage_id': 'exact_match',
      'notebook_location': '../notebooks/1_exact_match/drivers/exact_match_driver',
      'raw_data_required': True,
      'input_table': ct._inter_preprocessed_inputs,
      'output_table': ct._inter_exact_non_match,
      'match_table': ct._inter_match_lookup,
      'execute': True,
    },
    {
      'stage_id': 'entity_matching',
      'notebook_location': '../notebooks/2_entity_extraction/drivers/entity_extraction_driver',
      'raw_data_required': True,
      'input_table': ct._inter_exact_non_match,
      'match_table': ct._inter_match_lookup,
      'output_table': ct._inter_entity_non_match,
      'unmappable_table': ct.unmappable_table,
      'run_id': '666',
      'execute': True,
    },
    {
      'stage_id': 'fuzzy_matching',
      'notebook_location': '../notebooks/3_fuzzy_matching/drivers/fuzzy_match_driver',
      'raw_data_required': True,
      'input_table': ct._inter_entity_non_match,
      'output_table': ct._inter_match_lookup,
      'match_lookup_final_table': ct.match_lookup_final_table,
      'unmappable_table': ct.unmappable_table,
      'fuzzy_non_linked': ct._cache_fuzzy_non_linked,
      'fuzzy_nonlinked_non_match_output': ct._cache_fuzzy_non_linked_non_match,
      'next_submission_table': ct._inter_next_submission,
      'match_lookup_final_version': 'integration_test_version',
      'run_id': '666',
      'execute': True,
    },
    {
      'stage_id': 'accuracy_calculating',
      'notebook_location': '../notebooks/4_accuracy_calculating/drivers/accuracy-calculating-driver',
      'raw_data_required': True,
      'input_table': ct.match_lookup_final_table,
      'output_table': ct.accuracy_table,
      'ground_truth_table': GROUND_TRUTH_TABLE,
      'execute': True
    },
    {
      'stage_id': 'curated_accuracy',
      'notebook_location': '../notebooks/5_curated_accuracy/drivers/curated_accuracy_driver',
      'raw_data_required': True,
      'input_table': ct.match_lookup_final_table,
      'unmappable_table': ct.unmappable_table,
      'output_table': ct.curated_accuracy_table,
      'baseline_table': CURATED_ACCURACY_BASELINE_TABLE,
      'ground_truth_table': CURATED_GROUND_TRUTH_TABLE,
      'execute': True
    }
  ]

  run_matching_pipeline(PIPELINE_CONFIG)