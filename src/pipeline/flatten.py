from typing import Any, Callable, List
import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine
import functools

from src.pipeline.dimension import DIMENSIONS
from src.pipeline.facts import FACTS


def get_submission_timeline_query() -> str:
    subquery_for_type = " ".join(
        [f"WHEN sections::jsonb ? '{table}' THEN '{table}'" for table in FACTS]
    )

    subquery_for_extracted_data = " ".join(
        [
            f"WHEN extracted_data_with_id::jsonb ? '{table}'  THEN extracted_data_with_id ->'{table}' "
            for table in FACTS
        ]
    )

    subquery_for_extracted_data_with_id = " ".join(
        [
            f"WHEN sections::jsonb ? '{table}'  THEN Jsonb_insert( sections::jsonb , '{{ {table}, primary_key}}', "
            f"To_jsonb(id), true )"
            for table in FACTS
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
            CASE {subquery_for_type}
                ELSE NULL
            END AS type_of_data,
            CASE
               {subquery_for_extracted_data_with_id}
                ELSE NULL
            END AS extracted_data_with_id
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


def get_engine(database: str = "postgres") -> Any:
    # Define the PostgreSQL database connection details
    username = "admin"
    password = "supersecret123"
    host = "localhost"
    port = "5432"

    return pg.connect(
        f"dbname='{database}' user='{username}' host='{host}' port='{port}' password='{password}'"
    )


def get_data_from_db(sql_callback: Callable) -> pd.DataFrame:
    return pd.read_sql(sql=sql_callback(), con=get_engine())


def get_unstructured_columns_types_from_tables(table_name: str) -> List[str]:
    json_columns_query = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = '{table_name}' AND data_type IN ('json', 'jsonb');
    """

    # Execute the query and fetch the results
    return pd.read_sql_query(json_columns_query, get_engine())["column_name"].tolist()


# submission timeline
df_raw = get_data_from_db(sql_callback=get_submission_timeline_query)

# temporary
df_raw.dropna(subset=["type_of_data"], inplace=True)

# FACTS
hashmap_of_df = {table: df_raw[df_raw["type_of_data"] == table] for table in FACTS}

for table, df in hashmap_of_df.items():
    df_normalized = pd.json_normalize(
        df["extracted_data"], record_path="table", meta=["primary_key"]
    )

    df_final = pd.merge(df, df_normalized, on="primary_key", how="left")

    username = "dataapi"
    password = "dataapi"
    host = "localhost"
    port = "5433"

    engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{host}:{port}/reporting"
    )

    df_final.drop(
        ["sections", "extracted_data_with_id", "extracted_data"], axis=1
    ).to_sql(
        name=f"fact_{table.lower()}",
        con=engine,
        if_exists="append",
        method="multi",
        schema="public",
    )

# Dimension
for table in DIMENSIONS:
    partial_callback = functools.partial(
        get_fullload_query, table=f"{table.schema_name}.{table.table_name}"
    )

    df_raw: pd.DataFrame = get_data_from_db(sql_callback=partial_callback)

    normalize_columns: List = get_unstructured_columns_types_from_tables(
        table_name=table.table_name
    )
    for column in normalize_columns:
        df_normalized = pd.json_normalize(df_raw[column]).drop("id", axis=1)
        df_raw = pd.concat([df_raw, df_normalized], axis=1)

    df_excluded = df_raw.loc[:, ~df_raw.columns.isin(normalize_columns)]

    df_excluded.to_sql(
        name=f"dimension_{table.target_table_name.lower()}",
        con=engine,
        if_exists="replace",
        method="multi",
        schema=table.target_schema_name,
        index=False,
        chunksize=500,
    )
