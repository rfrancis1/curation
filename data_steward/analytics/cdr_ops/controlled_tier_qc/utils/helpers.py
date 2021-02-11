import pandas as pd
from jinja2 import Template
from code.config import (CSV_FOLDER, COLUMNS_IN_CHECK_RESULT)

def load_check_file(filename, rule_code=None):
    check_df = pd.read_csv(CSV_FOLDER/filename, dtype='object')
    if rule_code:
        if not isinstance(rule_code, list):
            rule_code = [rule_code]
        check_df = check_df[check_df['rule'].isin(rule_code)]
    return check_df

def form_field_param_from_row(row, field):
    return row[field] if field in row and row[field] != None else ''

def get_list_of_common_columns_for_merge(check_df, results_df):
    return [col for col in check_df if col in results_df]

def format_cols_to_string(df):
    df = df.copy()
    for col in df:
        if col == 'n_row_violation':
            df[col] = df[col].astype(int)
            continue
        if df[col].dtype == 'float':
            df[col] = df[col].astype(pd.Int64Dtype())
        df[col] = df[col].astype(str)
    return df

def run_check_by_row(df, template_query, project_id, post_deid_dataset, pre_deid_dataset=None, mapping_issue_description=None):
    if df.empty:
        return pd.DataFrame(columns=[col for col in df if col in COLUMNS_IN_CHECK_RESULT])

    check_df = df.copy()
    queries = ""
    results = []
    for _, row in check_df.iterrows():
        column_name = form_field_param_from_row(row, 'column_name')
        concept_id = form_field_param_from_row(row, 'concept_id')
        concept_code = form_field_param_from_row(row, 'concept_code')
        data_type = form_field_param_from_row(row, 'data_type')
        primary_key = form_field_param_from_row(row, 'primary_key')
        mapping_table = form_field_param_from_row(row, 'mapping_table')
        new_id = form_field_param_from_row(row, 'new_id')
        query = Template(template_query).render(project_id=project_id, 
                post_deid_dataset=post_deid_dataset, pre_deid_dataset=pre_deid_dataset,
                table_name=row['table_name'],column_name=column_name,
                concept_id=concept_id, concept_code=concept_code, data_type=data_type,
                primary_key=primary_key, new_id=new_id, mapping_table=mapping_table)
        print(query)
        result_df = pd.read_gbq(query)
        print(result_df)
        results.append(result_df)

    results_df = (pd.concat(results)
                    .pipe(format_cols_to_string))
    merge_cols = get_list_of_common_columns_for_merge(check_df, results_df)
    result_columns = merge_cols + ['rule', 'n_row_violation']
    final_result =  (check_df.merge(results_df, on=merge_cols, how='left')
                            .filter(items=result_columns)
                            .query('n_row_violation > 0')
            )
    if not final_result.empty and mapping_issue_description:
        final_result['mapping_issue'] = mapping_issue_description 
    return final_result if not final_result.empty else pd.DataFrame(columns=result_columns)


def highlight(row):
    '''
    highlight the maximum in a Series yellow.
    '''
    s = row['n_row_violation']
    if s > 0:
        css = 'background-color: red'
    else:
        css = ''
    return [css] * len(row)