import pandas as pd

from utils.helpers import (load_check_file, run_check_by_row)
from sql.query_templates import (QUERY_SUPPRESSED_CONCEPT)

from code.config import CONCEPT_CSV_FILE

def check_concept_suppression(project_id, post_dataset_id, rule_code, pre_deid_dataset=None):
    check_df = load_check_file(CONCEPT_CSV_FILE, rule_code)
    concept_check = run_check_by_row(check_df, QUERY_SUPPRESSED_CONCEPT,
        project_id, post_dataset_id)
    
    return concept_check.reset_index(drop=True)

