from datetime import datetime
import subprocess
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from airflow import DAG
import os
import pandas as pd
import requests
from get_data_source import (
    download_data_task,
    process_parquet_files_task,
    create_streaming_data_task,
)
from load_dwh import insert_table

POSTGRES_CONN_ID = "postgres"

# Create a DAG with the appropriate settings
with DAG(
    dag_id="get_data_from_source", start_date=datetime(2023, 7, 1), schedule=None
) as dag:
    # BashOperator to set the necessary permissions for the data directory
    grant_permissions = BashOperator(
        task_id="grant_permissions_task",
        bash_command="""
        data_path="/opt/airflow/data/"
        echo "Setting ownership for $data_path"
        echo "Setting permissions for $data_path"
        sudo chmod -R 755 $data_path
        echo "Permissions granted successfully for $data_path"
        """,
    )

    @task
    def download_data(data_type: str):
        download_data_task(data_type)

    @task
    def process_parquet_files(process_type: str):
        process_parquet_files_task(process_type)

    @task
    def create_streaming_data():
        create_streaming_data_task()

    # Defining the task dependencies
    (
        grant_permissions
        >> [download_data("yellow"), download_data("green")]
        >> process_parquet_files("fix_data_type")
        >> process_parquet_files("transform")
        >> process_parquet_files("drop_missing")
        >> create_streaming_data()
    )


with DAG(dag_id="load_into_dwh", start_date=datetime(2023, 7, 1), schedule=None) as dag:
    # This is often used for tasks which are more suitable for executing commands
    # For example, submit a job to a Spark cluster, initiate a new cluster,
    # run containers, upgrade software packages on Linux systems,
    # or installing a PyPI package
    create_table_pg = PostgresOperator(
        task_id="create_table_pg",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS nyc_taxi_warehouse(
            vendorid  INT, 
            pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
            dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
            passenger_count FLOAT, 
            trip_distance FLOAT, 
            ratecodeid FLOAT, 
            store_and_fwd_flag VARCHAR(1), 
            pulocationid INT, 
            dolocationid INT, 
            payment_type INT, 
            fare_amount FLOAT, 
            extra FLOAT, 
            mta_tax FLOAT, 
            tip_amount FLOAT, 
            tolls_amount FLOAT, 
            improvement_surcharge FLOAT, 
            total_amount FLOAT, 
            congestion_surcharge FLOAT
        );
        
    """,
    )

    insert_table_pg = PythonOperator(
        task_id="insert_table_pg",
        python_callable=insert_table,
    )
    gx_validate_pg = GreatExpectationsOperator(
        task_id="gx_validate_pg",
        conn_id=POSTGRES_CONN_ID,
        data_context_root_dir="include/great_expectations",
        data_asset_name="public.nyc_taxi",
        # database="k6",
        expectation_suite_name="nyctaxi_suite",
        return_json_dict=True,
    )

create_table_pg >> insert_table_pg >> gx_validate_pg
