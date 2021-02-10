# utils
from controlled_tier_qc import Field_Suppression_Check

RULE_CODE = "DC-1373"

def run_identifying_field_suppression_check(project_id, dataset):
    id_field_qc = Field_Suppression_Check(project_id, dataset, RULE_CODE)
    return id_field_qc._run_check()