from jinja2 import Template

# """
# Each field with STRING type and is_nullablle == 'YES' should be null
# EXCEPT for:
#   -observation.value_as_string with zip codes observation_source_concept_id in (1585966, 1585914, 1585930, 1585250)
# """
QUERY_SUPPRESSED_NULLABLE_FIELD_NOT_NULL = """
SELECT
    '{{ table_name }}' AS table_name,
    '{{ column_name }}' AS column_name,
    COUNT(*) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
WHERE {{ column_name }} IS NOT NULL
{% if table_name == 'observation' and column_name == 'value_as_string' %}
AND observation_source_concept_id NOT IN (1585966, 1585914, 1585930, 1585250)
AND observation_concept_id NOT IN (1585966, 1585914, 1585930, 1585250)
{% endif %}
"""

# """
# Each REQUIRED (Not nullable) field with STRING type should be an empty string
# """
QUERY_SUPPRESSED_REQUIRED_FIELD_NOT_EMPTY = """
SELECT
    '{{ table_name }}' AS table_name,
    '{{ column_name }}' AS column_name,
    COUNT(*) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
WHERE {{ column_name }} != ''
"""

# """
# Any numeric field should be 0 or NULL
# """
QUERY_SUPPRESSED_NUMERIC_NOT_ZERO = """
SELECT
    '{{ table_name }}' AS table_name,
    '{{ column_name }}' AS column_name,
    COUNT(*) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
WHERE {{ column_name }} != 0 AND {{ column_name }} IS NOT NULL
"""

# """
# Records with specific concept id or concept code must be suppressed
# """
QUERY_SUPPRESSED_CONCEPT = """
SELECT
    '{{ table_name }}' AS table_name,
    '{{ column_name }}' AS column_name,
    {% if concept_id|int(-999) != -999 %}
    {{ concept_id }} AS concept_id,
    {% else %}
    '{{ concept_code }}' AS concept_code,
    {% endif %}
    COUNT(*) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
{% if concept_id|int(-999) != -999 %}
    WHERE {{ column_name }} IN ({{ concept_id|int }})
{% else %}
    WHERE {{ column_name }} IN ('{{ concept_code|string }}')
{% endif %}
"""

# """
# Suppressed tables must be empty
# """
QUERY_SUPPRESSED_TABLE = """
SELECT
    '{{ table_name }}' AS table_name,
    COUNT(*) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
"""

# """
# person_ids should all be int
# Questionnaire_response_id should be int
# """
QUERY_ID_NOT_OF_CORRECT_TYPE = """
SELECT
    table_name,
    CASE WHEN data_type != {{ data_type }} THEN 1 ELSE 0 END AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.INFORMATION_SCHEMA.COLUMNS`
WHERE column_name = '{{ column_name }}'
AND table_name = '{{ table_name }}'
"""

# """
# person_id post de-id should not be the same as the person_id pre de-id in tables other than person
# questionnaire_id post de-id should not be the same as questionnaire_id pre de-id
# """
QUERY_ID_NOT_CHANGED_BY_DEID = """
SELECT
    '{{ table_name }}' AS table_name,
    SUM(CASE WHEN input.{{ column_name }} = output.{{ column_name }} THEN 1 ELSE 0 END) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}` output
JOIN `{{ project_id }}.{{ pre_deid_dataset }}.{{ table_name }}` input USING {{ primary_key }}
"""

# """
# person_id post deid should be the same as research_id from the mapping table
# questionnaire_id post deid should be the same as research_response_id from the mapping table
# """
QUERY_ID_NOT_IN_MAPPING = """
SELECT
    '{{ table_name }}' AS table_name,
    1 AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
WHERE {{ column_name }} NOT IN (
    SELECT {{ new_id }}
    FROM `{{ project_id }}.{{ pre_deid_dataset }}.{{ mapping_table }}`
)
"""

# """
# person_id post-deid should be mapped correctly to person_id pre-deid if we use the mapping table
# questionnaire_id post-deid should be mapped correctly to response_research_id pre-deid if we use the mapping table
# """
QUERY_ID_NOT_MAPPED_PROPERLY = """
WITH data AS (
SELECT
    map.{{ new_id }} AS expected_pid,
    post_deid.{{ column_name }} AS output_pid
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}` post_deid
LEFT JOIN `{{ project_id }}.{{ pre_deid_dataset }}.{{ table_name }}` pre_deid USING({{ primary_key }})
LEFT JOIN `{{ project_id }}.{{ pre_deid_dataset }}.{{ mapping_table }}` map ON pre_deid.{{ column_name }} = map.{{ column_name }}
)
SELECT
    '{{ table_name }}' AS table_name,
    SUM(CASE WHEN output_pid != expected_pid THEN 1 ELSE 0 END) AS n_row_violation
FROM data
"""