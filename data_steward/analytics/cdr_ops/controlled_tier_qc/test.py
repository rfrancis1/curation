# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: 'Python 3.7.3 64-bit (''base'': conda)'
#     language: python
#     name: python37364bitbaseconda5b767f67375d47e0bc47dd4f7a1d12d0
# ---

# %load_ext autoreload
# %autoreload 2

from jinja2 import Template
import pandas as pd

# # SQL

from sql.query_templates import (QUERY_SUPPRESSED_NULLABLE_FIELD_NOT_NULL, 
                                QUERY_SUPPRESSED_REQUIRED_STRING_NOT_EMPTY,
                                QUERY_SUPPRESSED_NUMERIC_NOT_ZERO,
                                QUERY_SUPPRESSED_CONCEPT)

project_id = "aou-res-curation-output-prod"
dataset = "R2020Q4R2"

table_name = "observation"
column_name = "value_as_string"
query = Template(QUERY_SUPPRESSED_NULLABLE_FIELD_NOT_NULL)
query = query.render(table_name=table_name, column_name=column_name,
                                                       project_id=project_id, dataset=dataset)
print(query)
pd.read_gbq(query, dialect='standard')

table_name = "drug_exposure"
column_name = "sig"
query = Template(QUERY_SUPPRESSED_NULLABLE_FIELD_NOT_NULL)
query = query.render(table_name=table_name, column_name=column_name,
                                                       project_id=project_id, dataset=dataset)
print(query)
pd.read_gbq(query, dialect='standard')



table_name = "note"
column_name = "note_title"
query = Template(QUERY_SUPPRESSED_REQUIRED_STRING_NOT_EMPTY)
query = query.render(table_name=table_name, column_name=column_name,
                                                       project_id=project_id, dataset=dataset)
print(query)
pd.read_gbq(query, dialect='standard')



table_name = "provider"
column_name = "provider_id"
query = Template(QUERY_SUPPRESSED_NUMERIC_NOT_ZERO)
query = query.render(table_name=table_name, column_name=column_name,
                                                       project_id=project_id, dataset=dataset)
print(query)
pd.read_gbq(query, dialect='standard')



table_name = "observation"
column_name = "observation_source_value"
concept_code = "PIIBirthInformation_BirthDate"
query = Template(QUERY_SUPPRESSED_CONCEPT)
query = query.render(table_name=table_name, column_name=column_name, code=concept_code,
                                                       project_id=project_id, dataset=dataset)
print(query)
pd.read_gbq(query, dialect='standard')

table_name = "observation"
column_name = "observation_concept_id"
concept_code = "43529107"
query = Template(QUERY_SUPPRESSED_CONCEPT)
query = query.render(table_name=table_name, column_name=column_name, code=concept_code,
                                                       project_id=project_id, dataset=dataset)
print(query)
pd.read_gbq(query, dialect='standard')



# # Controlled Tier QC

from code.config import (CONCEPT_CSV_FILE, CSV_FOLDER, FIELD_CSV_FILE, TABLE_CSV_FILE)
from code.controlled_tier_qc import (load_dataframe_for_rule, get_field_violation_counts, 
                                    get_concept_violation_counts, Suppression_Check, Field_Suppression_Check,
                                    Concept_Suppression_Check)

concept_df = pd.read_csv(CSV_FOLDER/CONCEPT_CSV_FILE, dtype='object')
concept_df.dtypes

field_df = pd.read_csv(CSV_FOLDER/FIELD_CSV_FILE, dtype='object')
field_df.dtypes

# ## Load dataframe for rule

data_path = CSV_FOLDER/FIELD_CSV_FILE
rule = "DC-1370"
df = load_dataframe_for_rule(data_path, rule)
df['rule'].value_counts()

null_df = df[df['is_nullable'] == 'YES']
required_df = df[df['is_nullable'] == 'NO']

# ## Get field violation counts

from code.config import (CSV_FOLDER, FIELD_CSV_FILE, CONCEPT_CSV_FILE, TABLE_CSV_FILE,
                    NEEDED_COLUMNS_FROM_FIELD_CHECK, NUMERIC_DATA_TYPES,
                    NEEDED_COLUMNS_FOR_CONCEPT_CODE_CHECK_MERGE, NEEDED_COLUMNS_FOR_CONCEPT_ID_CHECK_MERGE)
from code.controlled_tier_qc import get_field_violation_counts

test_df = pd.DataFrame({'table_name': ['observation', 'provider'], 'column_name': ['value_as_string', 'provider_name']})

# nullable string violations
get_field_violation_counts(test_df, QUERY_SUPPRESSED_NULLABLE_FIELD_NOT_NULL, 
                           project_id, dataset, NEEDED_COLUMNS_FROM_FIELD_CHECK)

# nullable string violations
get_field_violation_counts(null_df, QUERY_SUPPRESSED_NULLABLE_FIELD_NOT_NULL, 
                           project_id, dataset, NEEDED_COLUMNS_FROM_FIELD_CHECK)

# required string violations
get_field_violation_counts(required_df, QUERY_SUPPRESSED_REQUIRED_STRING_NOT_EMPTY, 
                           project_id, dataset, NEEDED_COLUMNS_FROM_FIELD_CHECK)



# ## Get concept violation counts

from code.controlled_tier_qc import get_concept_violation_counts
from code.config import (NEEDED_COLUMNS_FROM_CONCEPT_CHECK,
                        NEEDED_COLUMNS_FOR_CONCEPT_CODE_CHECK_MERGE,
                        NEEDED_COLUMNS_FOR_CONCEPT_ID_CHECK_MERGE)

test_df = pd.DataFrame({'table_name': ['observation', 'observation'], 
                        'column_name': ['observation_source_value', 'observation_source_value'],
                       'concept_code': ['PIIBirthInformation_BirthDate', 'MENA_MENASpecific']})

get_concept_violation_counts(test_df, QUERY_SUPPRESSED_CONCEPT, project_id, 
                             dataset, NEEDED_COLUMNS_FROM_CONCEPT_CHECK)

data_path = CSV_FOLDER/CONCEPT_CSV_FILE
rule = "DC-1366"
df = load_dataframe_for_rule(data_path, rule)
df['rule'].value_counts()

c_code_df = df.loc[df['concept_code'].notnull(), NEEDED_COLUMNS_FOR_CONCEPT_CODE_CHECK_MERGE]
c_id_df = df.loc[df['concept_id'].notnull(), NEEDED_COLUMNS_FOR_CONCEPT_ID_CHECK_MERGE]

get_concept_violation_counts(c_code_df, QUERY_SUPPRESSED_CONCEPT, project_id, 
                             dataset, NEEDED_COLUMNS_FROM_CONCEPT_CHECK)

get_concept_violation_counts(c_id_df, QUERY_SUPPRESSED_CONCEPT, project_id, 
                             dataset, NEEDED_COLUMNS_FROM_CONCEPT_CHECK)

# # Classes

from code.controlled_tier_qc import Field_Suppression_Check

RULE_CODE = "DC-1370"
string_field_qc = Field_Suppression_Check(project_id, dataset, RULE_CODE)
string_field_qc._run_check()

# +
RULE_CODE = "DC-1373"

id_field_qc = Field_Suppression_Check(project_id, dataset, RULE_CODE)
id_field_qc._run_check()

# +
RULE_CODE = "DC-1359"

concept_qc = Concept_Suppression_Check(project_id, dataset, RULE_CODE)
concept_qc._run_check()

# +
RULE_CODE = "DC-1364"

concept_qc = Concept_Suppression_Check(project_id, dataset, RULE_CODE)
concept_qc._run_check()

# +
RULE_CODE = "DC-1366"

concept_qc = Concept_Suppression_Check(project_id, dataset, RULE_CODE)
concept_qc._run_check()
# -


