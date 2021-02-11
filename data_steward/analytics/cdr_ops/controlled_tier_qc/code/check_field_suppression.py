import pandas as pd

from utils.helpers import (load_check_file, run_check_by_row)
from sql.query_templates import (QUERY_SUPPRESSED_NULLABLE_FIELD_NOT_NULL,
                                QUERY_SUPPRESSED_REQUIRED_FIELD_NOT_EMPTY,
                                QUERY_SUPPRESSED_NUMERIC_NOT_ZERO)

from code.config import FIELD_CSV_FILE


def check_field_suppression(project_id, post_dataset_id, pre_deid_dataset=None, rule_code='DC-1373'):
    check_df = load_check_file(FIELD_CSV_FILE, rule_code)
    nullable_field = check_df[check_df['is_nullable'] == 'YES']
    required_numeric_field = check_df[(check_df['is_nullable'] == 'NO') & (check_df['data_type'] == 'INT64')]
    required_other_field = check_df[(check_df['is_nullable'] == 'NO') & (check_df['data_type'] != 'INT64')]

    nullable_field_check = run_check_by_row(nullable_field, QUERY_SUPPRESSED_NULLABLE_FIELD_NOT_NULL,
        project_id, post_dataset_id)
    
    required_numeric_field_check = run_check_by_row(required_numeric_field, QUERY_SUPPRESSED_NUMERIC_NOT_ZERO,
        project_id, post_dataset_id)

    required_other_field_check = run_check_by_row(required_other_field, QUERY_SUPPRESSED_REQUIRED_FIELD_NOT_EMPTY,
        project_id, post_dataset_id)

    return pd.concat([nullable_field_check, required_numeric_field_check, required_other_field_check], sort=True)

