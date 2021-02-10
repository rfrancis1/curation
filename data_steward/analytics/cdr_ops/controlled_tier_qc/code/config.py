from pathlib import Path

CSV_FOLDER = Path('csv')
SQL_FOLDER = Path('sql')

CONCEPT_CSV_FILE = "Controlled_Tier_Concept_Level.csv"
FIELD_CSV_FILE = "Controlled_Tier_Field_Level.csv"
TABLE_CSV_FILE = "Controlled_Tier_Table_Level.csv"
MAPPING_CSV_FILE = "Controlled_Tier_Mapping.csv"

COLUMN_VIOLATION = 'n_row_violation'

NEEDED_COLUMNS_FROM_FIELD_CHECK = ['rule', 'table_name', 'column_name', 'n_row_violation']
NEEDED_COLUMNS_FROM_CONCEPT_CHECK = ['rule', 'table_name', 'column_name', 'concept_id', 'concept_code', 'n_row_violation']
NEEDED_COLUMNS_FROM_TABLE_CHECK = ['rule', 'table_name', 'n_row_violation']

NEEDED_COLUMNS_FOR_CONCEPT_CODE_CHECK_MERGE = ['table_name', 'column_name', 'concept_code', 'rule']
NEEDED_COLUMNS_FOR_CONCEPT_ID_CHECK_MERGE = ['table_name', 'column_name', 'concept_id', 'rule']

NUMERIC_DATA_TYPES = ['INT64']

MAPPING_CHECK_DESCRIPTION = {
    'data_type': 'Correct Type', 'old_id_in_output': 'Old ID not changed',
    'new_id_not_in_map': 'New ID not in mapping', 'wrong_mapping': 'ID not properly mapped'
}
