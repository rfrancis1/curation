# utils
from controlled_tier_qc import Concept_Suppression_Check

RULE_CODE = "DC-1359"

def run_birth_data_suppression_check(project_id, dataset):
    concept_qc = Concept_Suppression_Check(project_id, dataset, RULE_CODE)
    return concept_qc._run_check()