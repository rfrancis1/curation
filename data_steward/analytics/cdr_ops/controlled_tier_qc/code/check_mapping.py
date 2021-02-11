import pandas as pd

from utils.helpers import (load_check_file, run_check_by_row)
from sql.query_templates import (QUERY_ID_NOT_OF_CORRECT_TYPE, QUERY_ID_NOT_CHANGED_BY_DEID, 
                                QUERY_ID_NOT_IN_MAPPING, QUERY_ID_NOT_MAPPED_PROPERLY)

from code.config import MAPPING_CSV_FILE

def check_mapping(project_id, post_dataset_id, pre_deid_dataset, rule_code='DC-1346'):
    check_df = load_check_file(MAPPING_CSV_FILE, rule_code)
    # Correct type
    type_check = run_check_by_row(check_df, QUERY_ID_NOT_OF_CORRECT_TYPE,
        project_id, post_dataset_id, pre_deid_dataset, "ID datatype has been changed")
    
    # id changed by de-id
    id_map_check = run_check_by_row(check_df, QUERY_ID_NOT_CHANGED_BY_DEID,
        project_id, post_dataset_id, pre_deid_dataset, "Old ID still in the dataset")

    # new id not in mapping
    id_not_in_mapping_check = run_check_by_row(check_df, QUERY_ID_NOT_IN_MAPPING,
        project_id, post_dataset_id, pre_deid_dataset, "New ID not found in mapping table")

    # new id properly mapped to old id
    id_properly_mapped_check = run_check_by_row(check_df, QUERY_ID_NOT_MAPPED_PROPERLY,
        project_id, post_dataset_id, pre_deid_dataset, "ID not properly mapped")
    
    return pd.concat([type_check, id_map_check, id_not_in_mapping_check, id_properly_mapped_check], sort=True)
