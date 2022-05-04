import io
import json
import re

import pandas as pd

from .convert import (normalize, partition, read_parquet, to_csv, to_dataframe,
                      to_parquet)
from .http import execute
from .utils import escape


def selection(query, tables=None, index=True):
    query = query.strip().strip(';')
    query = '{} FORMAT Parquet'.format(query)

    external = {}
    tables = tables or {}
    for name, df in tables.items():
        dtypes, df = normalize(df, index=index)
        data = to_csv(df)
        structure = ', '.join(map(' '.join, dtypes.items()))
        external[name] = (structure, data)

    return query, external


def insertion(df, table, index=True):
    insert = 'INSERT INTO {db}.{table} ({columns}) FORMAT Parquet'
    _, df = normalize(df, index=index)

    columns = ', '.join(map(escape, df.columns))
    query = insert.format(db='{db}', columns=columns, table=escape(table))

    return query, df


def read_clickhouse(query, tables=None, index=True, connection=None, verify=True, stream=True, pqfile=None, chunksize=65535, decode_binary=True, **kwargs):
    """Reads clickhouse query to pandas dataframe

    Parameters
    ----------

    query: str
        Clickhouse sql query, {db} will automatically replaced
        with `database` argument
    host: str
        clickhouse host to connect
    tables: dict of pandas DataFrames
        external table definitions for query processing
    database: str, default 'default'
        clickhouse database
    user: str, default None
        clickhouse user
    password: str, default None
        clickhouse password
    index: bool, default True
        whether to serialize `tables` with index or not
    verify: bool, default True
        SSL Cert Verification

    Additional keyword arguments passed to `pandas.read_csv`
    """
    if re.match("LIMIT", query, re.IGNORECASE):
        query0 = query + " LIMIT 0"
    else:
        query0 = re.sub("LIMIT\s+\d+", "LIMIT 0", query, re.IGNORECASE)
    query0 = query0 + "  FORMAT JSONCompactEachRowWithNamesAndTypes"
    lines = execute(query0, stream=False, connection=connection)
    names, types = map(json.loads, lines.decode("utf-8").split("\n", 1))
    def converter(df):
        for (name, type) in zip(names, types):
            if type == "Date":
                df[name] = pd.to_timedelta(df[name], unit="day") + pd.Timestamp("1970-01-01")
            elif type == "Date32":
                df[name] = pd.to_timedelta(df[name], unit="day") + pd.Timestamp("1925-01-01")
            elif type == "DateTime":
                df[name] = df[name].mul(1e9).astype("datetime64[ns]")
            elif type == "String" and decode_binary:
                df[name] = df[name].str.decode("utf-8")
        return df
    query, external = selection(query, tables=tables, index=index)
    lines = execute(query, external=external, stream=stream,
                    connection=connection, verify=verify)
    if stream:
        if pqfile is None:
            import tempfile
            pqfile = tempfile.mktemp(suffix=".pq")
        with open(pqfile, "wb") as f:
            while True:
                buf = lines.read(10*1024**2)
                if len(buf) == 0:
                    break
                else:
                    f.write(buf)
        lines.release_conn()
        df = read_parquet(pqfile, chunksize, converter, remove=True)
    else:
        df = converter(pd.read_parquet(io.BytesIO(lines)))
    return df


def to_clickhouse(df, table, index=True, chunksize=1000, connection=None):
    query, df = insertion(df, table, index=index)
    for chunk in partition(df, chunksize=chunksize):
        execute(query, data=to_parquet(chunk), connection=connection)

    return df.shape[0]
