import os
from datetime import datetime

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
DATA_DIR = "/opt/airflow/data/"
POSTGRES_CONN_ID = "postgres"


def insert_table():
    pg_hook = PostgresHook.get_hook(conn_id=POSTGRES_CONN_ID)
    for file in os.listdir(DATA_DIR):
        if file.endswith(".parquet") and str(file).startswith("yellow"):
            df = pd.read_parquet(os.path.join(DATA_DIR, file))
            print("Inserting data from file: " + file)
            # df=df.sample(1000)
            pg_hook.insert_rows(table="nyc_taxi_warehouse", rows=df.values.tolist())

