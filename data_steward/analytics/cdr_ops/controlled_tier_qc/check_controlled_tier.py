# %load_ext autoreload
# %autoreload 2

import pandas as pd

project_id = "aou-res-curation-output-prod"
dataset = "R2020Q4R2"

from code.controlled_tier_qc import (run_all_checks, display_summary_checks, Table_Suppression_Check)



# # Summary

checks = run_all_checks(project_id, dataset)

for rule, df in checks.items():
    result = df.groupby('rule')['n_row_violation'].sum().reset_index()
    print(result)

display_summary_checks(checks)






