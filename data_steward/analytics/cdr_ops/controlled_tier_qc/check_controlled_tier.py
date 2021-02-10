# %load_ext autoreload
# %autoreload 2

import pandas as pd

project_id = "aou-res-curation-output-prod"
post_deid_dataset = "R2020Q4R2"
pre_deid_dataset = ""

from code.controlled_tier_qc import (run_all_checks, display_summary_checks, Table_Suppression_Check)

checks = run_all_checks(project_id, post_deid_dataset, pre_deid_dataset)

# # Summary

display_summary_checks(checks)

# # DC-1370: String type field suppression

checks.get('DC-1370')

# # DC-1377: All Zip Code Values are generalized

checks.get('DC-1377')

# # DC-1346: PID to RID worked

checks.get('DC-1346')

# # DC-1348: Questionnaire_response_id Mapped

checks.get('DC-1348')

# # DC-1355: Site id mappings ran on the controlled tier

checks.get('DC-1355')

# # DC-1357: Person table does not have month or day of birth

checks.get('DC-1357')

# # DC-1359: Observation does not have birth information

checks.get('DC-1359')

# # DC-1362: Table suppression

checks.get('DC-1362')

# # DC-1364: Explicit identifier record suppression

checks.get('DC-1364')

# # DC-1366: Race/Ethnicity record suppression

checks.get('DC-1366')

# # DC-1368: Motor Vehicle Accident record suppression

checks.get('DC-1368')

# # DC-1373: Identifying field suppression works

checks.get('DC-1373')

# # DC-1380: Generalized Zip Code Values are Aggregated

checks.get('DC-1380')

# # DC-1382: Record Suppression of some cancer condition

checks.get('DC-1382')

# # DC-1388: Free Text survey response are suppressed

checks.get('DC-1388')
