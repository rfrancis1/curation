# for data manipulation
import pandas as pd 

# Path
from code.config import (CSV_FOLDER, FIELD_CSV_FILE, CONCEPT_CSV_FILE, TABLE_CSV_FILE, 
                        MAPPING_CSV_FILE, COLUMN_VIOLATION,
                    NEEDED_COLUMNS_FROM_CONCEPT_CHECK, NEEDED_COLUMNS_FROM_FIELD_CHECK, NUMERIC_DATA_TYPES,
                    NEEDED_COLUMNS_FOR_CONCEPT_CODE_CHECK_MERGE, NEEDED_COLUMNS_FOR_CONCEPT_ID_CHECK_MERGE,
                    NEEDED_COLUMNS_FROM_TABLE_CHECK, MAPPING_CHECK_DESCRIPTION)

# SQL template
from jinja2 import Template


def load_dataframe_for_rule(data_path:str, rule_code:str=None) -> pd.DataFrame:
    df = pd.read_csv(data_path, dtype='object')
    if rule_code:
        df = df[df['rule'] == rule_code]
    return df


def get_field_violation_counts(df:pd.DataFrame, template_query:str, project_id:str, dataset:str, needed_cols:list) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=needed_cols)

    result_df = df.copy()
    results = []
    for _, row in result_df.iterrows():
        query = Template(template_query).render(table_name=row['table_name'], column_name=row['column_name'], 
            project_id=project_id, dataset=dataset)
        result = pd.read_gbq(query)
        results.append(result)
    all_results = pd.concat(results)
    result_df = (result_df.merge(all_results, how='left', on=['table_name', 'column_name'])
                          .filter(items=needed_cols)
                )
    return result_df[result_df[COLUMN_VIOLATION] > 0]


def get_concept_violation_counts(df:pd.DataFrame, template_query:str, project_id:str, dataset:str, needed_cols:list) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=needed_cols)

    result_df = df.copy()
    results = []
    for _, row in result_df.iterrows():
        if 'concept_code' in row:
            code = row['concept_code']
        if 'concept_id' in row:
            code = int(row['concept_id'])
        query = Template(template_query).render(table_name=row['table_name'], column_name=row['column_name'], code=code,
            project_id=project_id, post_deid_dataset=dataset)
        result = pd.read_gbq(query)
        results.append(result)
    all_results = pd.concat(results)

    merge_cols = [col for col in result_df if col != 'rule']
    result_df = (result_df.merge(all_results, how='left', left_on=merge_cols, right_on=['table_name', 'column_name', 'code'])
                          .filter(items=needed_cols)
                )
    return result_df[result_df[COLUMN_VIOLATION] > 0]


def get_table_violation_counts(df:pd.DataFrame, template_query:str, project_id:str, dataset:str, needed_cols:list) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=needed_cols)

    result_df = df.copy()
    results = []
    for _, row in result_df.iterrows():
        query = Template(template_query).render(table_name=row['table_name'], project_id=project_id, post_deid_dataset=dataset)
        result = pd.read_gbq(query)
        results.append(result)
    all_results = pd.concat(results)
    result_df = (result_df.merge(all_results, how='left', on=['table_name'])
                          .filter(items=needed_cols)
                )
    return result_df[result_df[COLUMN_VIOLATION] > 0]


def get_mapping_violation_counts(df, template_query, project_id, pre_deid_dataset, post_deid_dataset, needed_cols, check_description=None):
    if df.empty:
        return pd.DataFrame(columns=needed_cols)

    result_df = df.copy()
    results = []
    for _, row in result_df.iterrows():
        if check_description == MAPPING_CHECK_DESCRIPTION['wrong_mapping'] and row['table_name'] in ['person', 'heart_rate_summary', 'activity_summary',
                'death', 'steps_intraday', 'heart_rate_minute_level']:
            continue
        query = Template(template_query).render(table_name=row['table_name'], column_name=row['column_name'], 
        project_id=project_id, post_deid_dataset=post_deid_dataset, pre_deid_dataset=pre_deid_dataset)
        result = pd.read_gbq(query)
        results.append(result)
    all_results = pd.concat(results)
    result_df = (result_df.merge(all_results, how='left', on=['table_name', 'column_name'])
                          .filter(items=needed_cols)
                )
    if check_description:
        result_df['check_description'] = check_description
    return result_df[result_df[COLUMN_VIOLATION] > 0]


