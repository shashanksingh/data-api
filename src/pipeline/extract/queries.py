from typing import List, Union

import pandas as pd

from facts import FACTS_TABLE, FACTS_QUESTIONS
from engine import get_engine


def get_subquery_for_submission_type_parsing_macro(
    name_of_submission_column: str,
) -> Union[str, str]:
    # handle question vs table split
    subquery_for_table_type = " ".join(
        [
            f"WHEN {name_of_submission_column}::jsonb ? '{table}' THEN '{table}'"
            for table in FACTS_TABLE
        ]
    )

    subquery_for_question_type = " ".join(
        [
            f"WHEN {name_of_submission_column}::jsonb ? '{table}' THEN '{table}'"
            for table in FACTS_QUESTIONS
        ]
    )
    return subquery_for_table_type, subquery_for_question_type


def get_submission_timeline_query() -> str:
    # handle question vs table split
    subquery_for_table_type, subquery_for_question_type = (
        get_subquery_for_submission_type_parsing_macro(
            name_of_submission_column="sections"
        )
    )

    subquery_for_extracted_data = " ".join(
        [
            f"WHEN extracted_data_with_id::jsonb ? '{table}'  THEN extracted_data_with_id ->'{table}' "
            for table in FACTS_TABLE
        ]
    )

    subquery_for_extracted_table_data_with_id = " ".join(
        [
            f"WHEN sections::jsonb ? '{table}'  THEN Jsonb_insert( sections::jsonb , '{{ {table}, primary_key}}', "
            f"To_jsonb(id), true )"
            for table in FACTS_TABLE
        ]
    )

    subquery_for_extracted_question_data_with_id = " ".join(
        [
            f"WHEN sections::jsonb ? '{question}'  THEN Jsonb_insert( sections::jsonb , '{{ {question}, primary_key}}', "
            f"To_jsonb(id), true )"
            for question in FACTS_QUESTIONS
        ]
    )

    return f"""
    WITH extracted_data_cte AS (
        SELECT
            *,
            id as primary_key,
            sections -> 'dates' -> 'date' ->> 'start' AS start_date,
            sections -> 'dates' -> 'date' ->> 'end' AS end_date,
            sections -> 'siteId' AS site_id,
            CASE {subquery_for_table_type}
                ELSE NULL
            END AS type_of_table_data,
            CASE {subquery_for_question_type}
                ELSE NULL
            END AS type_of_question_data,
            CASE
               {subquery_for_extracted_table_data_with_id}
                ELSE NULL
            END AS extracted_data_with_id,
            CASE
               {subquery_for_extracted_question_data_with_id}
                ELSE NULL
            END AS extracted_question_data_with_id

        FROM
            submission_timelines
    )
    SELECT
        *,
        CASE
            {subquery_for_extracted_data}
                ELSE NULL
            END AS extracted_data
    FROM
        extracted_data_cte
        """


def get_fullload_query(table: str) -> str:
    return f"SELECT * from {table}"


def get_unstructured_columns_types_from_tables(table_name: str) -> List[str]:
    json_columns_query = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = '{table_name}' AND data_type IN ('json', 'jsonb');
    """

    # Execute the query and fetch the results
    return pd.read_sql_query(json_columns_query, get_engine())["column_name"].tolist()


def get_submission_attributes_query() -> str:
    # handle question vs table split
    subquery_for_table_type, subquery_for_question_type = (
        get_subquery_for_submission_type_parsing_macro(
            name_of_submission_column="submission"
        )
    )

    return f""" 
WITH formatted_data AS (
    SELECT 
        id,
        submission_timeline_id,
        author_user_id,
        version,
        submission,
        jsonb_array_elements(calculation::jsonb) AS outer_array
    FROM 
        submission_attribute_calculations
)
SELECT 
    submission_timeline_id as primary_key,
    fd.submission -> 'dates' -> 'date' ->> 'start' AS start_date,
    fd.submission -> 'dates' -> 'date' ->> 'end' AS end_date,
    fd.submission -> 'siteId' AS site_id,
    fd.id,
    fd.submission_timeline_id,
    fd.author_user_id,
    fd.version,
    fd.submission,
    CASE {subquery_for_table_type}
                ELSE NULL
    END AS type_of_table_data,
    CASE {subquery_for_question_type}
                ELSE NULL
    END AS type_of_question_data,
    record.id AS record_id,
    record.value AS record_value,
    record.label AS record_label,
    record.type AS record_type
FROM 
    formatted_data fd,
    LATERAL jsonb_to_recordset(fd.outer_array) 
    AS record(id text, value text, label text, type text) ;
"""


def get_all_tables_query() -> str:
    return """
    SELECT table_name, table_schema, table_type
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
ORDER BY table_schema, table_name;
    """


def get_current_db_schema() -> str:
    return """SELECT 
    current_database() AS database_name,
    current_schema() AS schema_name;"""
