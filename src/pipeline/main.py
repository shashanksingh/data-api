import pandas as pd
import json
from facts import FACTS_TABLE, FACTS_QUESTIONS
from extract.queries import (
    get_submission_timeline_query,
    get_all_tables_query,
    get_current_db_schema,
)
from engine import REPORTING_ENGINE
from helper import drop_columns_if_exists, pretty_print_load_exception, get_data_from_db
from load.dimensions import load_dimension, load_dimension_table

# source : get all tables
get_data_from_db(sql_callback=get_all_tables_query).to_sql(
    name=f"source_tables",
    con=REPORTING_ENGINE,
    if_exists="replace",
    method="multi",
    schema="public",
)

get_data_from_db(sql_callback=get_current_db_schema).to_sql(
    name=f"source_database",
    con=REPORTING_ENGINE,
    if_exists="replace",
    method="multi",
    schema="public",
)

# source : submission attribute calculator
# df_sac_raw = get_data_from_db(sql_callback=get_submission_attributes_query)
# df_sac_raw.drop(["submission"], axis=1, inplace=True)
# df_sac_raw.to_sql(
#     name=f"fact_precalculated_submissions",
#     con=REPORTING_ENGINE,
#     if_exists="replace",
#     method="multi",
#     schema="public",
#     chunksize=100,
# )


# source : submission timeline
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
    print(f"[Copied]{table}")
    df_normalized = pd.json_normalize(
        df["extracted_data"], record_path="table", meta=["primary_key"]
    )

    try:
        df_final = pd.merge(df, df_normalized, on="primary_key", how="left")
    except Exception as e:
        pretty_print_load_exception(table=table, e=e, df=df, df_normalized=df)
        continue

    df_final.drop(
        [
            "sections",
            "extracted_data_with_id",
            "extracted_data",
            "extracted_question_data_with_id",
        ],
        axis=1,
    ).to_sql(
        name=f"fact_{table.lower()}",
        con=REPORTING_ENGINE,
        if_exists="replace",
        method="multi",
        schema="public",
    )

# FACTS WITH QUESTIONS
for question, df in hashmap_of_question_df.items():
    print(f"[Copied]{question}")
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
        pretty_print_load_exception(table=question, e=e, df=df, df_normalized=df)
        continue

    df_final = drop_columns_if_exists(
        df=df_final,
        columns=[
            "sections",
            "extracted_data_with_id",
            "extracted_data",
            "extracted_question_data_with_id",
            "sourceMaterial.file",
        ],
    )

    # Just in offchance we have more object struct
    df_final = df_final.apply(
        lambda col: col.apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
    )

    df_final.to_sql(
        name=f"fact_{question.lower()}",
        con=REPORTING_ENGINE,
        if_exists="replace",
        method="multi",
        schema="public",
    )

load_dimension()
load_dimension_table()
