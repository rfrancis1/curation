# utils
from controlled_tier_qc import Field_Suppression_Check

RULE_CODE = "DC-1370"

def run_string_suppression_check(project_id, dataset):
    string_field_qc = Field_Suppression_Check(project_id, dataset, RULE_CODE)
    return string_field_qc._run_check()