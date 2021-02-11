# for data manipulation
import pandas as pd 

# Path
from code.config import (CSV_FOLDER, CHECK_LIST_CSV_FILE)

# SQL template
from jinja2 import Template

# functions for QC
from code.check_table_suppression import check_table_suppression
from code.check_field_suppression import check_field_suppression
from code.check_concept_suppression import check_concept_suppression
from code.check_mapping import check_mapping

from utils.helpers import load_check_file, highlight

FUNC = {
    'Table': check_table_suppression,
    'Field': check_field_suppression,
    'Concept': check_concept_suppression,
    'Mapping': check_mapping
}

def run_qc(project_id, post_deid_dataset, pre_deid_dataset, rule_code=None):
    list_checks = load_check_file(CHECK_LIST_CSV_FILE, rule_code)
    list_checks = list_checks[list_checks['level'].notnull()].copy()
    
    checks = []
    for _, row in list_checks.iterrows():
        rule = row['rule']
        check_level = row['level']
        level_lower_case = check_level.lower()
        check_function = FUNC.get(check_level)
        df = check_function(project_id, post_deid_dataset, pre_deid_dataset, rule)
        checks.append(df)
    return pd.concat(checks, sort=True).reset_index(drop=True)
      

def display_check_summary_by_rule(checks_df):
    by_rule = checks_df.groupby('rule')['n_row_violation'].sum().reset_index()
    needed_description_columns = ['rule', 'description']
    check_description = (load_check_file(CHECK_LIST_CSV_FILE)
                            .filter(items=needed_description_columns)
                        )
    if not by_rule.empty:
        by_rule = by_rule.merge(check_description, how='outer', on='rule')
    else:
        by_rule = check_description.copy()
    by_rule['n_row_violation'] = by_rule['n_row_violation'].fillna(0).astype(int)
    return by_rule.style.apply(highlight, axis=1)

    
def display_check_detail_of_rule(checks_df, rule):
    return checks_df[checks_df['rule'] == rule]