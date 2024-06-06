from typing import List

import pandas as pd


def drop_columns_if_exists(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    existing_cols = df.columns.intersection(columns)
    return df.drop(existing_cols, axis=1)
