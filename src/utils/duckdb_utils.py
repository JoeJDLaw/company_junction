import duckdb
import pandas as pd
from contextlib import contextmanager

@contextmanager
def connect(threads: int | None = None):
    con = duckdb.connect()
    if threads is not None:
        con.execute(f"PRAGMA threads={int(threads)}")
    # deterministic ordering where needed
    con.execute("PRAGMA preserve_insertion_order=true")
    try:
        yield con
    finally:
        con.close()

def sql_df(query: str, /, **tables: pd.DataFrame) -> pd.DataFrame:
    # registers provided DataFrames as temp views, returns a pandas DataFrame
    with connect() as con:
        for name, df in tables.items():
            con.register(name, df)
        return con.execute(query).df()

def sql_df_with_threads(query: str, threads: int | None = None, **tables: pd.DataFrame) -> pd.DataFrame:
    """SQL query with configurable thread count for heavy operations."""
    with connect(threads=threads) as con:
        for name, df in tables.items():
            con.register(name, df)
        return con.execute(query).df()

def ensure_pandas_strings(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("string")
    return df
