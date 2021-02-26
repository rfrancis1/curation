# %load_ext autoreload
# %autoreload 2

import pandas as pd

project_id = "aou-res-curation-output-prod"
post_deid_dataset = "R2020Q4R2"
pre_deid_dataset = "R2020Q4R2"

from code.controlled_tier_qc import run_qc, display_check_summary_by_rule, display_check_detail_of_rule

to_include = ['DC-1368']
checks = run_qc(project_id, post_deid_dataset, pre_deid_dataset, rule_code=to_include)





# # Summary

display_check_summary_by_rule(checks)

# # DC-1370: String type field suppression

display_check_detail_of_rule(checks, 'DC-1370')

# # DC-1377: All Zip Code Values are generalized

display_check_detail_of_rule(checks, 'DC-1377')

# # DC-1346: PID to RID worked

display_check_detail_of_rule(checks, 'DC-1346')

# # DC-1348: Questionnaire_response_id Mapped

display_check_detail_of_rule(checks, 'DC-1348')

# # DC-1355: Site id mappings ran on the controlled tier

display_check_detail_of_rule(checks, 'DC-1355')

# # DC-1357: Person table does not have month or day of birth

display_check_detail_of_rule(checks, 'DC-1357')

# # DC-1359: Observation does not have birth information

display_check_detail_of_rule(checks, 'DC-1359')

# # DC-1362: Table suppression

display_check_detail_of_rule(checks, 'DC-1362')

# # DC-1364: Explicit identifier record suppression

display_check_detail_of_rule(checks, 'DC-1364')

# # DC-1366: Race/Ethnicity record suppression

display_check_detail_of_rule(checks, 'DC-1366')

# # DC-1368: Motor Vehicle Accident record suppression

display_check_detail_of_rule(checks, 'DC-1368')

# # DC-1373: Identifying field suppression works

display_check_detail_of_rule(checks, 'DC-1373')

# # DC-1380: Generalized Zip Code Values are Aggregated

display_check_detail_of_rule(checks, 'DC-1380')

# # DC-1382: Record Suppression of some cancer condition

display_check_detail_of_rule(checks, 'DC-1382')

# # DC-1388: Free Text survey response are suppressed

display_check_detail_of_rule(checks, 'DC-1388')




