from typing import List, Callable
import pandas as pd
from datetime import datetime
from engine import get_engine, REPORTING_ENGINE


def drop_columns_if_exists(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    existing_cols = df.columns.intersection(columns)
    return df.drop(existing_cols, axis=1)


def pretty_print_load_exception(
    table: str, e: Exception, df: pd.DataFrame, df_normalized: pd.DataFrame
) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}][Exception] {table}", "===" * 5)
    print("[Exception][TABLE]", str(e))
    print("[DF]", df)
    print("[DF_NORMALIZED]", df_normalized)
    print("===" * 12)


def get_data_from_db(sql_callback: Callable) -> pd.DataFrame:
    print("[get_data_from_db] Get Data")
    response = pd.read_sql(sql=sql_callback(), con=get_engine(), chunksize=100)
    print("[get_data_from_db] Data Recieved From DB", response.shape)
    return response


def write_to_table(table: str, df: pd.DataFrame, chunksize=100) -> None:
    df.to_sql(
        table,
        con=REPORTING_ENGINE,
        if_exists="replace",
        schema="public",
        chunksize=chunksize,
    )
