# for data manipulation
import pandas as pd
from collections import defaultdict

# Path
from code.config import (CSV_FOLDER, FIELD_CSV_FILE, CONCEPT_CSV_FILE, TABLE_CSV_FILE, 
                        MAPPING_CSV_FILE, COLUMN_VIOLATION,
                    NEEDED_COLUMNS_FROM_CONCEPT_CHECK, NEEDED_COLUMNS_FROM_FIELD_CHECK, NUMERIC_DATA_TYPES,
                    NEEDED_COLUMNS_FOR_CONCEPT_CODE_CHECK_MERGE, NEEDED_COLUMNS_FOR_CONCEPT_ID_CHECK_MERGE,
                    NEEDED_COLUMNS_FROM_TABLE_CHECK, MAPPING_CHECK_DESCRIPTION,
                    EXCLUDED_TABLES_FOR_WRONG_MAPPING_CHECK)

# SQL template
from jinja2 import Template
from sql.query_templates import (QUERY_SUPPRESSED_NULLABLE_FIELD_NOT_NULL, 
                                QUERY_SUPPRESSED_REQUIRED_STRING_NOT_EMPTY,
                                QUERY_SUPPRESSED_NUMERIC_NOT_ZERO,
                                QUERY_SUPPRESSED_CONCEPT, QUERY_SUPPRESSED_TABLE,
                                QUERY_ID_NOT_OF_CORRECT_TYPE, QUERY_ID_NOT_CHANGED_BY_DEID, QUERY_ID_NOT_IN_MAPPING,
                                QUERY_ID_NOT_MAPPED_PROPERLY
                                )

# helper functions for violation counts
from utils.helpers import (load_dataframe_for_rule, get_table_violation_counts, get_field_violation_counts, 
                        get_field_violation_counts, get_mapping_violation_counts)


def run_check_based_on_file(csv_file:str, dict_result:dict, project_id:str, post_deid_dataset:str, pre_deid_dataset:str=None) -> dict:
    checks_df = load_dataframe_for_rule(CSV_FOLDER/csv_file)
    rule_codes = checks_df['rule'].unique()
    for rule in rule_codes:
        if csv_file == TABLE_CSV_FILE:
            df = Table_Suppression_Check(project_id, post_deid_dataset, rule)._run_check()
        if csv_file == FIELD_CSV_FILE:
            df = Field_Suppression_Check(project_id, post_deid_dataset, rule)._run_check()
        if csv_file == CONCEPT_CSV_FILE:
            df = Concept_Suppression_Check(project_id, post_deid_dataset, rule)._run_check()
        if csv_file == MAPPING_CSV_FILE:
            df = Mapping_Check(project_id, post_deid_dataset, pre_deid_dataset, rule)
        dict_result[rule] = df
    return dict_result


def run_all_checks(project_id:str, post_deid_dataset:str, pre_deid_dataset:str=None) -> dict:
    result_checks = defaultdict(list)
    # Table checks
    result_checks = run_check_based_on_file(TABLE_CSV_FILE, result_checks, project_id, post_deid_dataset, pre_deid_dataset)
    
    # Field Checks
    result_checks = run_check_based_on_file(FIELD_CSV_FILE, result_checks, project_id, post_deid_dataset, pre_deid_dataset)

    # Concept Checks
    # result_checks = run_check_based_on_file(CONCEPT_CSV_FILE, result_checks, project_id, post_deid_dataset, pre_deid_dataset)

    # TODO Mapping checks

    return result_checks


def display_summary_checks(result_checks):
    check_list = []
    for rule, df in result_checks.items():
        result = df.groupby('rule')['n_row_violation'].sum().reset_index()
        check_list.append(result)
    check_df = pd.concat(check_list).reset_index(drop=True)
    return check_df.style.applymap(lambda x: 'background-color:yellow' if isinstance(x, int) and x>0 else '')


class Check():
    def __init__(self, project_id, dataset, rule_code, csv_filepath):
        self._project_id = project_id
        self._dataset = dataset
        self._rule_code = rule_code
        self._csv_filepath = csv_filepath
        self._extract_data_to_check()
    
    def _extract_data_to_check(self):
        self._df_to_check = load_dataframe_for_rule(CSV_FOLDER/self._csv_filepath, self._rule_code)

      
class Field_Suppression_Check(Check):
    def __init__(self, project_id, dataset, rule_code, csv_filepath=FIELD_CSV_FILE):
        super().__init__(project_id, dataset, rule_code, csv_filepath)
        self._needed_cols = NEEDED_COLUMNS_FROM_FIELD_CHECK
        self._nullable_df = self._extract_nullable_field()
        self._numeric_required_df = self._extract_numeric_required_field()
        self._other_required_df = self._extract_other_than_numeric_required_field()

    def _extract_nullable_field(self):
        return self._df_to_check[self._df_to_check['is_nullable'] == 'YES']

    def _extract_numeric_required_field(self):
        return self._df_to_check[(self._df_to_check['is_nullable'] == 'NO') & (self._df_to_check['data_type'].isin(NUMERIC_DATA_TYPES))]

    def _extract_other_than_numeric_required_field(self):
        return self._df_to_check[(self._df_to_check['is_nullable'] == 'NO') & (~self._df_to_check['data_type'].isin(NUMERIC_DATA_TYPES))]

    def _run_check(self):
        result_nullable_df = get_field_violation_counts(self._nullable_df, QUERY_SUPPRESSED_NULLABLE_FIELD_NOT_NULL,
                                            self._project_id, self._dataset, self._needed_cols)

        result_numeric_required_df = get_field_violation_counts(self._numeric_required_df, QUERY_SUPPRESSED_NUMERIC_NOT_ZERO,
                                            self._project_id, self._dataset, self._needed_cols)

        result_other_required_df = get_field_violation_counts(self._other_required_df, QUERY_SUPPRESSED_REQUIRED_STRING_NOT_EMPTY,
                                            self._project_id, self._dataset, self._needed_cols)

        return pd.concat([result_nullable_df, result_numeric_required_df, result_other_required_df], sort=True)


class Concept_Suppression_Check(Check):
    def __init__(self, project_id, dataset, rule_code, csv_filepath=CONCEPT_CSV_FILE):
        super().__init__(project_id, dataset, rule_code, csv_filepath)
        self._needed_cols = NEEDED_COLUMNS_FROM_CONCEPT_CHECK
        self._extract_concept_code_df()
        self._extract_concept_id_df()

    def _extract_concept_code_df(self):
        self._concept_code_df = self._df_to_check.loc[self._df_to_check['concept_code'].notnull(), 
                                            NEEDED_COLUMNS_FOR_CONCEPT_CODE_CHECK_MERGE]

    def _extract_concept_id_df(self):
        self._concept_id_df = self._df_to_check.loc[self._df_to_check['concept_id'].notnull(), 
                                            NEEDED_COLUMNS_FOR_CONCEPT_ID_CHECK_MERGE]

    def _run_check(self):
        result_concept_code_df = get_concept_violation_counts(self._concept_code_df, QUERY_SUPPRESSED_CONCEPT,
                                                self._project_id, self._dataset, self._needed_cols)
        result_concept_id_df = get_concept_violation_counts(self._concept_id_df, QUERY_SUPPRESSED_CONCEPT,
                                                self._project_id, self._dataset, self._needed_cols)
        return pd.concat([result_concept_code_df, result_concept_id_df], sort=True)


class Table_Suppression_Check(Check):
    def __init__(self, project_id, dataset, rule_code, csv_filepath=TABLE_CSV_FILE):
        super().__init__(project_id, dataset, rule_code, csv_filepath)
        self._needed_cols = NEEDED_COLUMNS_FROM_TABLE_CHECK
    
    def _run_check(self):
        return get_table_violation_counts(self._df_to_check, QUERY_SUPPRESSED_TABLE,
                                        self._project_id, self._dataset, self._needed_cols)


class Mapping_Check(Check):
    def __init__(self, project_id, post_deid_dataset, pre_deid_dataset, rule_code, csv_filepath=MAPPING_CSV_FILE):
        super().__init__(project_id, post_deid_dataset, rule_code, csv_filepath)
        self._pre_deid_dataset = pre_deid_dataset
        self._needed_cols = NEEDED_COLUMNS_FROM_FIELD_CHECK
    
    def _run_check(self):
        result_type_df = get_mapping_violation_counts(self._df_to_check, QUERY_ID_NOT_OF_CORRECT_TYPE,
                        self._project_id, self._pre_deid_dataset, self._dataset,self._needed_cols, MAPPING_CHECK_DESCRIPTION['data_type'])

        result_old_id_exist_df = get_mapping_violation_counts(self._df_to_check, QUERY_ID_NOT_CHANGED_BY_DEID,
            self._project_id, self._pre_deid_dataset, self._dataset, self._needed_cols, MAPPING_CHECK_DESCRIPTION['old_id_in_input'])

        result_id_not_in_mapping_df = get_mapping_violation_counts(self._df_to_check, QUERY_ID_NOT_IN_MAPPING,
            self._project_id, self._pre_deid_dataset, self._dataset, self._needed_cols, MAPPING_CHECK_DESCRIPTION['new_id_not_in_map'])

        result_id_not_properly_mapped_df = get_mapping_violation_counts(self._df_to_check, QUERY_ID_NOT_MAPPED_PROPERLY,
            self._project_id, self._pre_deid_dataset, self._dataset, self._needed_cols, MAPPING_CHECK_DESCRIPTION['wrong_mapping'])
            
        return pd.concat([result_type_df, result_old_id_exist_df, result_id_not_in_mapping_df, result_id_not_properly_mapped_df], sort=True)