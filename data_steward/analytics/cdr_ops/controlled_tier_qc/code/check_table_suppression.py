import pandas as pd

from utils.helpers import (load_check_file, run_check_by_row)
from sql.query_templates import (QUERY_SUPPRESSED_TABLE)

from code.config import TABLE_CSV_FILE

def check_table_suppression(project_id, post_dataset_id, pre_deid_dataset=None, rule_code='DC-1362'):
    check_df = load_check_file(TABLE_CSV_FILE, rule_code)
    table_check = run_check_by_row(check_df, QUERY_SUPPRESSED_TABLE,
        project_id, post_dataset_id)
    
    return table_check.reset_index(drop=True)
