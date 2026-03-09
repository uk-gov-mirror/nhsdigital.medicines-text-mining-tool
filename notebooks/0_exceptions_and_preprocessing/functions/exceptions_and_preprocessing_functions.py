# Databricks notebook source
# MAGIC %run ../../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/exception_list

# COMMAND ----------

from functools import reduce
from operator import or_
from typing import List, Tuple

import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame, Column
import warnings

# COMMAND ----------

def drop_null_in_medication_col(source_table: DataFrame,
                                src_medication_col: str
                               ) -> DataFrame:  
  
  source_table_drop_null = source_table.where(col(src_medication_col).isNotNull())
  # If more than 50% are null, show a warning.
  if source_table_drop_null.count()/source_table.count() < 0.5:
    warnings.warn('Less than 50% of the records are not null.', RuntimeWarning)
 
  return source_table_drop_null

# COMMAND ----------

def filter_user_curated_unmappables(df: DataFrame, 
                                    text_col: str, 
                                    unmappable_regexes: List[str]
                                   ) -> Tuple[DataFrame, DataFrame]:
  '''
  Certain patterns are known to be unmappable. These should be assigned as user curated unmappable before feeding into the pipeline.
  '''
  
  unmappable_strings_condition = reduce(or_, [col(text_col).rlike(unmappable_regex) for unmappable_regex in unmappable_regexes])
  df_unmappable = df.where(unmappable_strings_condition
                           | (col(text_col).rlike(r'^[Vv]itamin') & (~col(text_col).rlike(vitamin_regex_pattern()))))
  df_remaining = df.join(df_unmappable, on=(df[text_col] == df_unmappable[text_col]), how='leftanti')
    
  return df_remaining, df_unmappable

# COMMAND ----------

def select_record_batch_to_process(df: DataFrame, 
                                   df_match_lookup_final: DataFrame,
                                   df_unmappable: DataFrame,
                                   batch_size: int, 
                                   join_cols: List[str]
                                  ) -> DFCheckpoint:
  '''
  Given the source data, return only the records that haven't yet been processed, by anti joining to the match_lookup_final and unmappable tables.
  The data is limited to batch_size number of records. A DFCheckpoint object is returned that needs to be deleted when the data is no longer in use. 
  
  arguments
    df (DataFrame): Input data.
    df_match_lookup_final (DataFrame): Dataframe of records that have already been processed in previous runs.
    df_unmappable (DataFrame): Another DataFrame of records that have already been processed in previous runs.
    batch_size (int): Batch size to select.
    join_col (str): The column name(s) that uniquely identify a record, to join the dataframes on.
  
  returns
    (DFCheckpoint): A checkpointed dataframe object containing the seleted batch.
  '''
  df_eligible = df.join(df_match_lookup_final, list(map(lambda x: df[x].eqNullSafe(df_match_lookup_final[x]), join_cols)), 'leftanti') \
                  .join(df_unmappable, list(map(lambda x: df[x].eqNullSafe(df_unmappable[x]), join_cols)), 'leftanti')
  
  df_selected = df_eligible.limit(batch_size)
  
  if batch_size >= df_eligible.count():
    RECORDS_EXHAUSTED_FLAG = True
  else:
    RECORDS_EXHAUSTED_FLAG = False
  
  return DFCheckpoint(df_selected), RECORDS_EXHAUSTED_FLAG

# COMMAND ----------

def select_distinct_descriptions(df: DataFrame,
                                 src_medication_col: str,
                                 original_text_col: str,
                                 form_in_text_col: str,
                                 id_col: str
                                ) -> DataFrame:
  '''
  Select records that are unique by original_epma_description and form_in_text
  '''
  df = df.withColumn(original_text_col, F.lower(col(src_medication_col))).withColumn(form_in_text_col, F.lower(col(form_in_text_col)))
  df_distinct_medication_names = df.select(original_text_col, form_in_text_col).distinct()
  return df_distinct_medication_names.withColumn(id_col, F.monotonically_increasing_id().cast(StringType()))

# COMMAND ----------

def standardise_interchangeable_words(text_col: Column, function_on: bool = True) -> Column:
  """
  Applies a series of regex based text substituations to a spark column, text_col.
  The function iterates over the pairs from interchangeable_words_dict and applies the replacement to text_col for each pair.
  When function_on is False, the functtion returns text_col unchanged.
  Note: replacements are applied sequentially and the output of each step is the input to the next. This means that earlier replacements can effect later pattern matches.
  """
  if function_on:
    for interchange_replacement, interchange_pattern in interchangeable_words_dict.items():
      text_col = F.regexp_replace(text_col, interchange_pattern, interchange_replacement)
    
  return text_col

# COMMAND ----------

def standardise_doseform(text_col: Column) -> Column:

  doseform_dict = {
    'topical': ' ',
#    'oral tablet': ' ',
    'inh ': 'dose',
    'nebuliser solution': 'nebuliser liquid',
    r'capsule\s?\(gastro-resistant\)': 'gastro-resistant tablet',
    r'capsule\s?\(modified release\)': 'modified-release capsule',
    r'tablet\s?\(chewable\)': 'chewable tablet',
    r'tablet\s?\(gastro-resistant\)': 'gastro-resistant tablet', 
    r'tablet\s?\(soluble\)': 'soluble tablet',
    r'\bsf\b': 'sugar free',
    r'\bgf\b': 'gluten free',
    r'\btts\b': 'transdermal patches',
    r'\bpreserv\b': 'preservative',
    r'\bcfc-free\b': 'cfc free',
    r'\bpatch\b': 'patches',
    r'\btablet\b': 'tablets',
    r'\bsuppository\b': 'suppositories',
    r'\bcapsule\b': 'capsules',
    # replace "50mcg in 1ml" with "50mcg / 1ml", but don't change "50mcg in 24 hours" (or "24h", etc)
    r'([a-z]\s)in(\s(?!24\s?h)\d)': '$1/$2',
    '  ': ' ',
    '(replaced less than character)!\\[cdata\\[': '', #Note: https://stackoverflow.com/questions/21816788/unclosed-character-class-error
    '\\]\\](replaced greater than character)':'',
    'non adhesive': 'non-adhesive'
  }
  for doseform_pattern, doseform_replacement in doseform_dict.items():
    text_col = F.regexp_replace(text_col, doseform_pattern, doseform_replacement)
    
  return text_col

# COMMAND ----------

def standardise_drug_name(text_col: Column) -> Column:

  drug_name_dict = {
    r'\badcal\s?\-?d3\b': 'adcal-d3',
    r'\bfultium\s?\-?d3\b': 'fultium-d3',
    r'\bbetnovate\s?\-?c\b': 'betnovate-c',
    r'\bbetnovate\s?\-?n\b': 'betnovate-n',
    r'\baugmentin\s?\-?duo\b': 'augmentin-duo', 
    r'\bbetaloc\s?\-?sa\b': 'betaloc-sa',
    r'\btimoptol\s?\-?la\b': 'timoptol-la',
    'amethocaine': 'tetracaine',
    'aminacrine': 'aminoacridine',
    'amoxycillin': 'amoxicillin',
    'amphetamine': 'amfetamine',
    'amylobarbitone sodium': 'amobarbital sodium',
    'beclomethasone': 'beclometasone',
    'bendrofluazide': 'bendroflumethiazide',
    'benzhexol': 'trihexyphenidyl',
    'benzphetamine': 'benzfetamine',
    'benztropine': 'benzatropine',
    'busulphan': 'busulfan',
    'butobarbitone': 'butobarbital',
    'carticaine': 'articaine',
    'cephalexin': 'cefalexin',
    'cephradine': 'cefradine',
    'chloral betaine': 'cloral betaine',
    'chlorbutol': 'chlorobutanol',
    'chlormethiazole': 'clomethiazole',
    'chlorpheniramine': 'chlorphenamine',
    'chlorthalidone': 'chlortalidone',
    'cholecalciferol': 'colecalciferol',
    'cholestyramine': 'colestyramine',
    'clomiphene': 'clomifene',
    'colistin sulphomethate sodium': 'colistimethate sodium',
    'corticotrophin': 'corticotropin',
    'cyclosporin': 'ciclosporin',
    'cysteamine': 'mercaptamine',
    'danthron': 'dantron',
    'dexamphetamine': 'dexamfetamine',
    'dibromopropamidine': 'dibrompropamidine',
    'dicyclomine': 'dicycloverine',
    'dienoestrol': 'dienestrol',
    'dimethicone(s)': 'dimeticone',
    'dimethyl sulphoxide': 'dimethyl sulfoxide',
    'dothiepin': 'dosulepin',
    'doxycycline hydrochloride (hemihydrate hemiethanolate)': 'doxycycline hyclate',
    'eformoterol': 'formoterol',
    'ethamsylate': 'etamsylate',
    'ethinyloestradiol': 'ethinylestradiol',
    'ethynodiol': 'etynodiol',
    'flumethasone': 'flumetasone',
    'flupenthixol': 'flupentixol',
    'flurandrenolone': 'fludroxycortide',
    'frusemide': 'furosemide',
    'guaiphenesin': 'guaifenesin',
    'hexachlorophane': 'hexachlorophene',
    'hexamine hippurate': 'methenamine hippurate',
    'hydroxyurea': 'hydroxycarbamide',
    'indomethacin': 'indometacin',
    'lignocaine': 'lidocaine',
    'methotrimeprazine': 'levomepromazine',
    'methyl cysteine': 'mecysteine',
    'methylene blue': 'methylthioninium chloride',
    'methicillin': 'meticillin',
    'mitozantrone': 'mitoxantrone',
    'nicoumalone': 'acenocoumarol',
    'oestradiol': 'estradiol',
    'oestriol': 'estriol',
    'oestrone': 'estrone',
    'oxpentifylline': 'pentoxifylline',
    'phenobarbitone': 'phenobarbital',
    'pipothiazine': 'pipotiazine',
    'polyhexanide': 'polihexanide',
    'pramoxine': 'pramocaine',
    'procaine penicillin': 'procaine benzylpenicillin',
    'prothionamide': 'protionamide',
    'quinalbarbitone': 'secobarbital',
    'riboflavine': 'riboflavin',
    'salcatonin': 'calcitonin (salmon)',
    'sodium calciumedetate': 'sodium calcium edetate',
    'sodium cromoglycate': 'sodium cromoglicate',
    'sodium ironedetate': 'sodium feredetate',
    'sodium picosulphate': 'sodium picosulfate',
    'sorbitan monostearate': 'sorbitan stearate',
    'stibocaptate': 'sodium stibocaptate',
    'stilboestrol': 'diethylstilbestrol',
    'sulphacetamide': 'sulfacetamide',
    'sulphadiazine': 'sulfadiazine',
    'sulphamethoxazole': 'sulfamethoxazole',
    'sulphapyridine': 'sulfapyridine',
    'sulphasalazine': 'sulfasalazine',
    'sulphathiazole': 'sulfathiazole',
    'sulphinpyrazone': 'sulfinpyrazone',
    'tetracosactrin': 'tetracosactide',
    'thiabendazole': 'tiabendazole',
    'thioguanine': 'tioguanine',
    'thiopentone': 'thiopental',
    'thymoxamine': 'moxisylyte',
    'thyroxine sodium': 'levothyroxine sodium',
    'tribavirin': 'ribavirin',
    'trimeprazine': 'alimemazine',
    'urofollitrophin': 'urofollitropin'
  }
  for drug_name_pattern, drug_name_replacement in drug_name_dict.items():
    text_col = F.regexp_replace(text_col, drug_name_pattern, drug_name_replacement)
    
  return text_col

# COMMAND ----------

def vitamin_regex_pattern():
  
  # Matches: Vitamin A1 20mg, Vitamin B oil, vitamin K compound
  # Unmatched: Vitamin A, vitamin B12
  return r'[Vv]itamin\s[A-Za-z]+\d*\s*((\s\d+\w+)|(oil)|(compound))'

# COMMAND ----------

def replace_hyphens_between_dosages_with_slashes(text_col: Column) -> Column:
  
    #This only needs to be done twice due to the pairing of terms. See the unit tests.
    for _ in range(0, 2):
      text_col = F.regexp_replace(text_col, r'([\d]+[\s]*([\w]|[%])+)[\s]*[-]+[\s]*([\d]+[\s]*[.]*([\w]|[%])+)', r'$1/$3')
    
    return text_col

# COMMAND ----------

def correct_common_unit_errors(text_col: Column) -> Column:
  
  unit_mappings = {
    'units/1ml': 'unit/ml',	
    'mcg/ml': 'microgram/ml',	
    'mcg/hr': 'microgram/hour',		
    'mcg': 'microgram',
    r'1000\s*ml': '1litre',
    r'1,000\s*ml': '1litre',
    r'2000\s*ml': '2litre',
    r'2,000\s*ml': '2litre'
  }
  
  for pattern, replacement in unit_mappings.items(): 
    text_col = F.regexp_replace(text_col, pattern, replacement)
  
  return text_col