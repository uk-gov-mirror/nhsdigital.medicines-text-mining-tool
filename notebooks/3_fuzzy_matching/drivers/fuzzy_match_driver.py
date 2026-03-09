# Databricks notebook source
# MAGIC %run ../../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/constants

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/ref_data_lib

# COMMAND ----------

# MAGIC %run ../functions/fuzzy_match_functions

# COMMAND ----------

spark.conf.set("spark.sql.broadcastTimeout",  36000)
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

import pyspark.sql.functions as F
import os
from datetime import datetime

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('notebook_location', './3_fuzzy_matching/drivers/fuzzy_match_driver', 'notebook_location')
dbutils.widgets.text('input_table', '', 'input_table')
dbutils.widgets.text('output_table', '', 'output_table')
dbutils.widgets.text('unmappable_table', '', 'unmappable_table')
dbutils.widgets.text('match_lookup_final_table', '', 'match_lookup_final_table')
dbutils.widgets.text('fuzzy_non_linked', '', 'fuzzy_non_linked')
dbutils.widgets.text('fuzzy_nonlinked_non_match_output', '', 'fuzzy_nonlinked_non_match_output')
dbutils.widgets.text('responses_table', '', 'responses_table')
dbutils.widgets.text('next_submission_table', '', 'next_submission_table')
dbutils.widgets.text('db', '', 'db')
dbutils.widgets.text('match_lookup_final_version', '', 'match_lookup_final_version')
dbutils.widgets.text('run_id', '', 'run_id')
dbutils.widgets.text('reviewed_matches_table', '', 'reviwed_matches_table')

stage = {
  'notebook_path' : os.path.basename(dbutils.widgets.get('notebook_location')),
  'input_table' : dbutils.widgets.get('input_table'),
  'output_table' : dbutils.widgets.get('output_table'),
  'unmappable_table' : dbutils.widgets.get('unmappable_table'),
  'match_lookup_final_table' : dbutils.widgets.get('match_lookup_final_table'),
  'fuzzy_non_linked' : dbutils.widgets.get('fuzzy_non_linked'),
  'fuzzy_nonlinked_non_match_output' : dbutils.widgets.get('fuzzy_nonlinked_non_match_output'),
  'responses_table' : dbutils.widgets.get('responses_table'),
  'next_submission_table' : dbutils.widgets.get('next_submission_table'),
  'db' : dbutils.widgets.get('db'),
  'match_lookup_final_version' : dbutils.widgets.get('match_lookup_final_version'),
  'run_id' : dbutils.widgets.get('run_id'),
  'reviewed_matches_table' : dbutils.widgets.get('reviewed_matches_table'),
}

# COMMAND ----------

exit_message = []
exit_message.append(f"\n notebook {stage['notebook_path']} execution started  @ {datetime.now().isoformat()}\n")

# COMMAND ----------

spark.sql('REFRESH TABLE '+ stage['input_table'])
df_input = get_data(stage['input_table']).drop_duplicates(subset=['epma_id','match_id'])   

# COMMAND ----------

step1_output = diluent_fuzzy_match_step(df_input, RefDataStore, 
                                        confidence_threshold=98,
                                        id_col=ID_COL,
                                        original_text_col=ORIGINAL_TEXT_COL,
                                        text_col=TEXT_COL,
                                        form_in_text_col=FORM_IN_TEXT_COL,
                                        match_term_col=MATCH_TERM_COL,
                                        match_level_col=MATCH_LEVEL_COL,
                                        id_level_col=ID_LEVEL_COL,
                                        match_id_col=MATCH_ID_COL,
                                        match_datetime_col=MATCH_DATETIME_COL,
                                        reason_col=REASON_COL)

# COMMAND ----------

append_to_table(step1_output.df_mappable, [ID_COL], stage['output_table'], allow_nullable_schema_mismatch=True)
append_to_table(step1_output.df_unmappable.withColumn(RUN_ID_COL, lit(stage['run_id'])), [ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL], stage['unmappable_table'], allow_nullable_schema_mismatch=True)

# COMMAND ----------

df_step1_remaining = step1_output.df_remaining.df

step1b_output = free_from_fuzzy_match_step(df_step1_remaining,
                                          RefDataStore,
                                          confidence_threshold=95,
                                          id_col=ID_COL,
                                          match_id_col=MATCH_ID_COL,
                                          original_text_col=ORIGINAL_TEXT_COL,
                                          form_in_text_col=FORM_IN_TEXT_COL,
                                          text_col=TEXT_COL,
                                          id_level_col=ID_LEVEL_COL,
                                          match_level_col=MATCH_LEVEL_COL,
                                          match_term_col=MATCH_TERM_COL,
                                          match_datetime_col=MATCH_DATETIME_COL,
                                          reason_col=REASON_COL)


# COMMAND ----------

append_to_table(step1b_output.df_mappable, [ID_COL], stage['output_table'], allow_nullable_schema_mismatch=True)
append_to_table(step1b_output.df_unmappable.withColumn(RUN_ID_COL, lit(stage['run_id'])), [ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL], stage['unmappable_table'], allow_nullable_schema_mismatch=True)

# COMMAND ----------

df_step1b_remaining = step1b_output.df_remaining.df

step2_output = linked_fuzzy_matching_step(df_step1b_remaining,
                                          RefDataStore,
                                          confidence_threshold_vtm_direct_match=95,
                                          confidence_threshold_fuzzy_match=70,
                                          id_col=ID_COL,
                                          match_id_col=MATCH_ID_COL,
                                          original_text_col=ORIGINAL_TEXT_COL,
                                          form_in_text_col=FORM_IN_TEXT_COL,
                                          text_col=TEXT_COL,
                                          id_level_col=ID_LEVEL_COL,
                                          match_level_col=MATCH_LEVEL_COL,
                                          match_term_col=MATCH_TERM_COL,
                                          match_datetime_col=MATCH_DATETIME_COL,
                                          reason_col=REASON_COL)

# COMMAND ----------

append_to_table(step2_output.df_mappable.df, [ID_COL], stage['output_table'], allow_nullable_schema_mismatch=True)
append_to_table(step2_output.df_unmappable.df.withColumn(RUN_ID_COL, lit(stage['run_id'])), [ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL], stage['unmappable_table'], allow_nullable_schema_mismatch=True)

# COMMAND ----------

# Temporarily save the step 2 remaining.
create_table(step2_output.df_remaining, stage['fuzzy_non_linked'], overwrite=True)

step1_output.df_remaining.delete()
step2_output.df_mappable.delete()
step2_output.df_unmappable.delete()

# COMMAND ----------

df_step2_remaining = spark.table(stage['fuzzy_non_linked']).select(ID_COL, ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL, TEXT_COL)

step3_output = full_fuzzy_matching_step(RefDataStore,
                                        df_step2_remaining, 
                                        confidence_threshold=91,
                                        id_col=ID_COL, 
                                        original_text_col=ORIGINAL_TEXT_COL,
                                        form_in_text_col=FORM_IN_TEXT_COL,
                                        text_col=TEXT_COL,
                                        match_term_col=MATCH_TERM_COL,
                                        match_level_col=MATCH_LEVEL_COL,
                                        id_level_col=ID_LEVEL_COL,
                                        match_id_col=MATCH_ID_COL,
                                        match_datetime_col=MATCH_DATETIME_COL)

# COMMAND ----------

append_to_table(step3_output.df_mappable.df, [ID_COL], stage['output_table'], allow_nullable_schema_mismatch=True)

# COMMAND ----------

# Temporarily save the step 3 remaining.
create_table(step3_output.df_remaining, stage['fuzzy_nonlinked_non_match_output'], overwrite=True)

step3_output.df_mappable.delete()

# COMMAND ----------

df_step3_remaining = get_data(stage['fuzzy_nonlinked_non_match_output']).select(ID_COL, ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL, TEXT_COL)

step4_output = moiety_sort_ratio_fuzzy_match_step(df_step3_remaining, RefDataStore,
                                                 confidence_threshold=90,
                                                 original_text_col=ORIGINAL_TEXT_COL,
                                                 form_in_text_col=FORM_IN_TEXT_COL,
                                                 text_col=TEXT_COL,
                                                 id_col=ID_COL,
                                                 id_level_col=ID_LEVEL_COL,
                                                 match_level_col=MATCH_LEVEL_COL,
                                                 match_datetime_col=MATCH_DATETIME_COL,
                                                 match_id_col=MATCH_ID_COL,
                                                 reason_col=REASON_COL)

# COMMAND ----------

append_to_table(step4_output.df_unmappable.df.withColumn(RUN_ID_COL, lit(stage['run_id'])), [ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL], stage['unmappable_table'], allow_nullable_schema_mismatch=True) 
append_to_table(step4_output.df_mappable.df, [ID_COL], stage['output_table'], allow_nullable_schema_mismatch=True)

# COMMAND ----------

step4_output.df_unmappable.delete()
step4_output.df_mappable.delete()

# COMMAND ----------

spark.sql('REFRESH Table' + stage['output_table'])
df_match_lookup = spark.table(stage['output_table'])

# Throughout the pipeline we have sometimes used deduplicated versions of amp or amp_parsed, where the amp records have the same name.
# This was to save duplicated effort in fuzzy matching. Now, however, we need to know if there were multiple amps with the same name.
# If there are multiple amps with the same name and they map up to a unique vmp, then we should report at vmp level.
# If we matched to a uniquely named amp and this maps to a vmp with the same name, then we should report at vmp level.
df_match_lookup_final = map_amp_to_vmp_if_there_are_amp_desc_duplicates_or_matching_vmp_desc(df_match_lookup, RefDataStore,
                                                                                             id_col=ID_COL,
                                                                                             original_text_col=ORIGINAL_TEXT_COL,
                                                                                             form_in_text_col=FORM_IN_TEXT_COL,
                                                                                             text_col=TEXT_COL,
                                                                                             id_level_col=ID_LEVEL_COL,
                                                                                             match_level_col=MATCH_LEVEL_COL,
                                                                                             match_datetime_col=MATCH_DATETIME_COL,
                                                                                             match_id_col=MATCH_ID_COL)

# COMMAND ----------

df_match_lookup_correct_only, df_unmappable_incorrects = remove_incorrects(df_match_lookup_final, 
                                                                           original_text_col=ORIGINAL_TEXT_COL,
                                                                           form_in_text_col=FORM_IN_TEXT_COL,
                                                                           match_id_col=MATCH_ID_COL,
                                                                           match_datetime_col=MATCH_DATETIME_COL,
                                                                           reviewed_matches_table_name=stage['reviewed_matches_table'])

# COMMAND ----------

append_to_table(df_unmappable_incorrects.withColumn(RUN_ID_COL, lit(stage['run_id'])), [ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL], stage['unmappable_table'], allow_nullable_schema_mismatch=True)

# COMMAND ----------

# Throughout the pipeline we have not kept the match_term column. So now we should get it by joining on the match_id.
# Also at this stage, add the version_id and run_id.
df_match_lookup_correct_only = add_match_term(df_match_lookup_correct_only,
                                       RefDataStoreUntouched,
                                       id_col=ID_COL,
                                       original_text_col=ORIGINAL_TEXT_COL,
                                       form_in_text_col=FORM_IN_TEXT_COL,
                                       text_col=TEXT_COL, 
                                       match_id_col=MATCH_ID_COL,
                                       match_term_col=MATCH_TERM_COL,
                                       id_level_col=ID_LEVEL_COL,
                                       match_level_col=MATCH_LEVEL_COL,
                                       match_datetime_col=MATCH_DATETIME_COL) \
                        .drop(TEXT_COL, ID_COL) \
                        .withColumn(VERSION_ID_COL, lit(stage['match_lookup_final_version'])) \
                        .withColumn(RUN_ID_COL, lit(stage['run_id']))

append_to_table(df_match_lookup_correct_only, [ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL], stage['match_lookup_final_table'], allow_nullable_schema_mismatch=True)

# COMMAND ----------

# Set the responses table to show that this run was successful.

df_responses = get_data(stage['responses_table'])
df_next_submission = get_data(stage['next_submission_table'])

df_next_submission = (df_next_submission.withColumn('run_date', F.current_timestamp())
                                       .withColumn('status', F.when(F.col('status') == 'running_final_batch', F.lit('success'))
                                                              .when(F.col('status') == 'running', F.lit('processing'))
                                                              .otherwise(F.col('status'))))

df_next_submission.write.format("delta").mode("append").saveAsTable(stage['responses_table'])

# COMMAND ----------

exit_message.append(f"notebook {stage['notebook_path']} execution completed  @ {datetime.now().isoformat()}")
exit_message = [i for i in exit_message if i] 
exit_message = '\n'.join(exit_message)
dbutils.notebook.exit(exit_message)