# Databricks notebook source
# These are items which may be inconsistent in dm+d and free text boxes. It will change the dict item in BOTH dm+d and free text boxes to dict key.
interchangeable_words_dict = {
    'modified-release': r'modified[- ]release|sustained[- ]release|extended[- ]release|time[- ]release|prolonged[- ]release|\bm\/?\.?r\b|\bs\/?\.?r\b',
    'sugar free': r'\bsugar[- ]free\b|\bs\.?f\b',
    ' and ': ' & ',
    'oral tablet': r'tablets? sugar\s?-?coated|tablets? film\s?-?coated|sugar\s?-?coated tablets?|film\s?-?coated tablets?',
    'gastro resistant': r'gastro-resistant|enteric coated|e/c|gastro-res',
    'uncoated': r'non-coated'
  }

# COMMAND ----------

# These pairs are of words/phrases which are not (and should not be seen as) the same thing but match closely with fuzzy matching. This dictionary and related functions are to avoiding these pairs from matching together.
never_mix_up_dict = {
  r'[\s]ag\s|[\s]ag$': r'[\s]ag\+\s|[\s]ag\+$',  
  r'hepatitis b\b': r'hepatitis a\b|hep a\b',
  r'hep b\b': r'hepatitis a\b|hep a\b',
  'opium': 'optium',
  'immediate release': r'modified-release|gastro resistant',
  r'\sdispersible': r'\sorodispersible',
  r'[\s]one\s|[\s]one$': r'[\s]one\+\s|[\s]one\+$',
  r'cream': r'ointment',
  r'caplet': r'chewable tablet',
  r'cream': r'foam',
  r'[\s|^]adhesive ': r'\bnon-adhesive',
  r'vial': r'ampoule'
}

# COMMAND ----------

# These words, if mentioned in the description will require the match term to also include this term - e.g. "paracetamol sugar-free" description would be prevented from mapping to any paracetamol dm+d medications which don't use the term sugar-free, unless at VTM level.
say_it_or_wrong_list = [
  'sugar free',
  'gastro resistant',
  'cfc-free',
  'orodispersible',
  'preservative free',
  'chewable',
  'dispersibe',
  'electrolyte'
]

# COMMAND ----------

#this was exact by previous name, but name now is probably not correctly mapped. This removes from historical tables amp_prev and vmp_prev
historical_but_now_incorrect = [
  'codeine phosphate',
  'citric acid monohydrate',
  'methadone hydrochloride',
  'peppermint oil',
  'balneum',
  'cosopt'
]

# COMMAND ----------

#If you see this, set it straight to unmappable
unmappable_inputs = [
  'freetext medication',
  r'\+ neat',
  r'water .*\+',
  r'\w*\s*macrogol\s*(\d+)?\s?(?:with|and|\+)(?:\s\w+)*\s*electrolytes?\s*\w*',
  r'\brequired\b',
  r'\bnot to be supplied\b'
]

# COMMAND ----------

#flavours to exclude from being tagged as a VTM
flavours = ['blackcurrant', 'orange', 'peppermint', 'raspberry', 'syrup', 'honey']

# COMMAND ----------

#for fuzzy moiety exclusions
non_moiety_words_extra = [
  # words found in the parsed reference data
  'and', 'daily', 'dose', 'dry', 'extra', 'flavour', 'for', 'formula', 'free', 'generic', 'in', 'inhaler', 'injection', 'kwikpen', 
  'lemon', 'max',  'mint', 'normal', 'original', 'patient', 'pen', 'penfill', 'plus', 'pre-filled', 'release', 'solution', 'strength',  
  'strong', 'weight', 'with'
  # words found in source_b samples
  'actuated', 'add', 'adult', 'advance', 'breath', 'cart', 'cfc', 'chewable', 'comment', 'dosealation', 'effervescent', 'fluid', 
  'infant', 'injectable', 'jelly', 'level', 'lubricating', 'modified', 'non-adhesive', 'only', 'per', 'pre-dose', 'ribbon',  
  'scalp', 'shower', 'sugar',  'suppositories', 'tube', 'testing'
  ]

# COMMAND ----------

