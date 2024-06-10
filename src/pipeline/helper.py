from typing import List

import pandas as pd


def drop_columns_if_exists(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    existing_cols = df.columns.intersection(columns)
    return df.drop(existing_cols, axis=1)


def pretty_print_load_exception(
    table: str, e: Exception, df: pd.DataFrame, df_normalized: pd.DataFrame
) -> None:
    print("===" * 10, table, "===" * 10)
    print("[Exception][TABLE]", str(e))
    print("[DF]", df)
    print("[DF_NORMALIZED]", df_normalized)
    print("===" * 10)
