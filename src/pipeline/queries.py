from typing import List

from facts import FACTS_TABLE, FACTS_QUESTIONS


def get_submission_timeline_query() -> str:
    # handle question vs table split
    subquery_for_table_type = " ".join(
        [f"WHEN sections::jsonb ? '{table}' THEN '{table}'" for table in FACTS_TABLE]
    )

    subquery_for_question_type = " ".join(
        [
            f"WHEN sections::jsonb ? '{table}' THEN '{table}'"
            for table in FACTS_QUESTIONS
        ]
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
