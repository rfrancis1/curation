import pandas as pd

from utils.helpers import run_check_by_row
from sql.query_templates import (QUERY_SUPPRESSED_CONCEPT)


def check_concept_suppression(check_df, project_id, post_dataset_id, rule_code, pre_deid_dataset=None):
    concept_check = run_check_by_row(check_df, QUERY_SUPPRESSED_CONCEPT,
        project_id, post_dataset_id)
    
    return concept_check.reset_index(drop=True)

