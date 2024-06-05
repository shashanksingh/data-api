from typing import Callable, List
import pandas as pd
from sqlalchemy import create_engine
import functools
import json
from dimension import DIMENSIONS
from facts import FACTS_TABLE, FACTS_QUESTIONS
from extract.queries import (
    get_submission_timeline_query,
    get_fullload_query,
    get_unstructured_columns_types_from_tables,
)
from engine import get_engine


def get_data_from_db(sql_callback: Callable) -> pd.DataFrame:
    return pd.read_sql(sql=sql_callback(), con=get_engine())


# submission timeline
df_raw = get_data_from_db(sql_callback=get_submission_timeline_query)

# temporary
df_raw_table_data = df_raw.dropna(subset=["type_of_table_data"])
df_raw_question_data = df_raw.dropna(subset=["type_of_question_data"])

# FACTS
hashmap_of_table_df = {
    table: df_raw[df_raw["type_of_table_data"] == table] for table in FACTS_TABLE
}
hashmap_of_question_df = {
    question: df_raw[df_raw["type_of_question_data"] == question]
    for question in FACTS_QUESTIONS
}

# FACTS WITH TABLE
for table, df in hashmap_of_table_df.items():
    df_normalized = pd.json_normalize(
        df["extracted_data"], record_path="table", meta=["primary_key"]
    )

    try:
        df_final = pd.merge(df, df_normalized, on="primary_key", how="left")
    except Exception as e:
        print("===" * 10, table, "===" * 10)
        print("[Exception][TABLE]", str(e))
        print("[DF]", df)
        print("[DF_NORMALIZED]", df_normalized)
        print("===" * 10)
        continue

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

# FACTS WITH QUESTIONS
for question, df in hashmap_of_question_df.items():
    df_normalized = pd.json_normalize(df["extracted_question_data_with_id"])
    try:
        df_final = pd.merge(
            df,
            df_normalized,
            left_on="primary_key",
            right_on=f"{question}.primary_key",
            how="left",
        )
    except Exception as e:
        print("===" * 10, question, "===" * 10)
        print("[Exception][QUESTIONS]", str(e))
        print("[DF]", df.columns)
        print("[DF_NORMALIZED]", df_normalized.columns)
        print("===" * 10)
        continue

    df_final = df_final.drop(
        [
            "sections",
            "extracted_data_with_id",
            "extracted_data",
            "extracted_question_data_with_id",
            "sourceMaterial.file",
        ],
        axis=1,
    )

    # Just in offchance we have more object struct
    df_final = df_final.applymap(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    df_final.to_sql(
        name=f"fact_{question.lower()}",
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
