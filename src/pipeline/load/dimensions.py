import functools
from dataclasses import asdict
from typing import Dict, List

import pandas as pd
from constants import UNITS
from engine import REPORTING_ENGINE
from dimension import DIMENSIONS
from extract.queries import (
    get_fullload_query,
    get_unstructured_columns_types_from_tables,
)
from src.pipeline.main import get_data_from_db


# Dimension - Unit
def load_dimension() -> None:
    units_dict: Dict = {key: asdict(value) for key, value in UNITS.items()}
    df_units: pd.DataFrame = pd.DataFrame.from_dict(units_dict, orient="index")
    df_units.to_sql(
        "dimension_units", con=REPORTING_ENGINE, if_exists="replace", schema="public"
    )


# Dimension - Table
def load_dimension_table() -> None:
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
            con=REPORTING_ENGINE,
            if_exists="replace",
            method="multi",
            schema=table.target_schema_name,
            index=False,
            chunksize=500,
        )
