import pandas as pd

from utils.helpers import run_check_by_row
from sql.query_templates import (QUERY_SUPPRESSED_NULLABLE_FIELD_NOT_NULL,
                                QUERY_SUPPRESSED_REQUIRED_FIELD_NOT_EMPTY,
                                QUERY_SUPPRESSED_NUMERIC_NOT_ZERO,
                                QUERY_VEHICLE_ACCIDENT_SUPPRESSION_ICD9, QUERY_VEHICLE_ACCIDENT_SUPPRESSION_ICD10,
                                QUERY_CANCER_CONCEPT_SUPPRESSION)


def check_field_suppression(check_df, project_id, post_dataset_id, rule_code, pre_deid_dataset=None):
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


def check_vehicle_accident_suppression(check_df, project_id, post_deid_dataset, rule_code, pre_deid_dataset=None):
    icd9_vehicle_accident = run_check_by_row(check_df, QUERY_VEHICLE_ACCIDENT_SUPPRESSION_ICD9,
        project_id, post_deid_dataset)
    icd10_vehicle_accident = run_check_by_row(check_df, QUERY_VEHICLE_ACCIDENT_SUPPRESSION_ICD10,
        project_id, post_deid_dataset)
    return pd.concat([icd9_vehicle_accident, icd10_vehicle_accident], sort=True)


def check_cancer_concept_suppression(check_df, project_id, post_deid_dataset, rule_code, pre_deid_dataset=None):
    cancer_concept = run_check_by_row(check_df, QUERY_CANCER_CONCEPT_SUPPRESSION,
        project_id, post_deid_dataset)
    return cancer_concept