#!/usr/bin/env/python
# coding: utf-8

import os
import argparse
from time import time

import pandas as pd
from sqlalchemy import create_engine, text


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = "output.csv"

    os.system(f"wget {url} -O {csv_name}")

    df = pd.read_csv(csv_name, nrows=0, compression="gzip")

    datetime_cols = []

    for col_name in df.columns:
        if "_datetime" in col_name:
            datetime_cols.append(col_name)

    df = pd.read_csv(csv_name, parse_dates=datetime_cols, nrows=100, compression="gzip")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    df_iter = pd.read_csv(
        csv_name,
        parse_dates=datetime_cols,
        compression="gzip",
        iterator=True,
        chunksize=100000,
        low_memory=False,
    )

    chunk = 1

    while (df := next(df_iter, None)) is not None:
        t_start = time()
        df.to_sql(name=table_name, con=engine, if_exists="append")
        t_end = time()

        print(
            f"Chunk no. {chunk:2d}:  {len(df):>7,d} records inserted in {t_end - t_start:.2f} seconds."
        )

        chunk += 1

    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT count(1) FROM {table_name}")).fetchall()
        print()
        print(f"Total rows: {result[0][0]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument("--user", help="Postgres username")
    parser.add_argument("--password", help="Postgres password")
    parser.add_argument("--host", help="Postgres container name")
    parser.add_argument("--port", help="Postgres container port")
    parser.add_argument("--db", help="Postgres db name")
    parser.add_argument("--table_name", help="Name of table in Postgres db")
    parser.add_argument("--url", help="URL of CSV file")

    args = parser.parse_args()

    main(args)
