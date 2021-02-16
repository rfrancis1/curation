import pandas as pd

from utils.helpers import run_check_by_row
from sql.query_templates import QUERY_SUPPRESSED_TABLE


def check_table_suppression(check_df, project_id, post_dataset_id, rule_code, pre_deid_dataset=None):
    table_check = run_check_by_row(check_df, QUERY_SUPPRESSED_TABLE,
        project_id, post_dataset_id)
    
    return table_check.reset_index(drop=True)
