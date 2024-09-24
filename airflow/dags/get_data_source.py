from datetime import datetime
import subprocess
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow import DAG
import os
import pandas as pd
import requests

# Create a DAG with the appropriate settings
with DAG(dag_id="get_data_from_source", start_date=datetime(2023, 7, 1), schedule=None) as dag:
    
    # BashOperator to set the necessary permissions for the data directory
    grant_permissions = BashOperator(
        task_id='grant_permissions_task',
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
        """
        Task to download NYC trip data (yellow/green) from the specified source.
        
        Args:
            data_type (str): The type of data to download ("yellow" or "green").
        """
        DATA_DIR = "/opt/airflow/data/"
        os.makedirs(DATA_DIR, exist_ok=True)

        years = ["2021", "2022"]
        months = [f"{i:02d}" for i in range(1, 13)]
        url_prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

        for year in years:
            for month in months:
                file_name = f"{data_type}_tripdata_{year}-{month}.parquet"
                url_download = f"{url_prefix}{file_name}"
                file_path = os.path.join(DATA_DIR, file_name)
                
                if os.path.exists(file_path):
                    print(f"File already exists: {file_path}")
                    continue

                try:
                    response = requests.get(url_download, allow_redirects=True)
                    response.raise_for_status()
                    with open(file_path, "wb") as file:
                        file.write(response.content)
                    print(f"Downloaded {file_name}")
                except requests.RequestException as e:
                    print(f"Error downloading file {url_download}: {e}")

    @task
    def process_parquet_files(process_type: str):
        """
        Generic task to process parquet files for different types of transformations.

        Args:
            process_type (str): The type of processing ('drop_column', 'drop_missing', 'transform', or 'fix_data_type').
        """
        data_path = "/opt/airflow/data/"
        
        for file in os.listdir(data_path):
            if not file.endswith(".parquet"):
                continue

            file_path = os.path.join(data_path, file)
            try:
                df = pd.read_parquet(file_path)

                if process_type == "drop_column":
                    df.dropna(axis=1, inplace=True)
                    if "store_and_fwd_flag" in df.columns:
                        df.drop(columns=["store_and_fwd_flag"], inplace=True)
                        print(f"Dropped 'store_and_fwd_flag' from {file}")
                
                elif process_type == "drop_missing":
                    df.dropna(inplace=True)
                    df = df.reindex(sorted(df.columns), axis=1)
                    print(f"Dropped missing data from {file}")

                elif process_type == "transform":
                    if file.startswith("green"):
                        df.rename(columns={
                            "lpep_pickup_datetime": "pickup_datetime",
                            "lpep_dropoff_datetime": "dropoff_datetime",
                            "ehail_fee": "fee"
                        }, inplace=True)
                        if "trip_type" in df.columns:
                            df.drop(columns=["trip_type"], inplace=True)
                    else:
                        df.rename(columns={
                            "tpep_pickup_datetime": "pickup_datetime",
                            "tpep_dropoff_datetime": "dropoff_datetime",
                            "airport_fee": "fee"
                        }, inplace=True)
                    df.columns = map(str.lower, df.columns)
                    if "fee" in df.columns:
                        df.drop(columns=["fee"], inplace=True)
                    print(f"Transformed data in {file}")

                elif process_type == "fix_data_type":
                    if "payment_type" in df.columns:
                        df["payment_type"] = df["payment_type"].astype(int)
                        print(f"Fixed 'payment_type' data type in {file}")

                df.to_parquet(file_path)
            
            except Exception as e:
                print(f"Error processing file {file}: {e}")

    @task
    def create_streaming_data():
        """
        Task to create streaming data from processed parquet files.
        """
        data_path = "/opt/airflow/data/"
        stream_path = "/opt/airflow/data/stream"
        os.makedirs(stream_path, exist_ok=True)
        
        df_list = []
        try:
            for file in os.listdir(data_path):
                if not file.endswith(".parquet"):
                    continue
                
                df = pd.read_parquet(os.path.join(data_path, file))
                df.dropna(inplace=True)

                if df.shape[0] < 10000:
                    continue
                
                df_sampled = df.sample(n=10000)
                df_sampled["content"] = file.split("_")[0]
                df_list.append(df_sampled)
            
            if df_list:
                final_df = pd.concat(df_list)
                final_df.to_parquet(os.path.join(stream_path, "stream.parquet"))
                print("Streaming data created successfully.")
        
        except Exception as e:
            print(f"Error creating streaming data: {e}")

    # Defining the task dependencies
    (
        grant_permissions
        >> [download_data("yellow"), download_data("green")]
        >> process_parquet_files("fix_data_type")
        >> process_parquet_files("transform")
        >> process_parquet_files("drop_missing")
        >> create_streaming_data()
    )
