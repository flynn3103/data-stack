import os
import json
import requests
from minio import Minio
from pyiceberg.schema import Schema
from pyiceberg.types import StructType, StringType, NestedField
import trino
import daft

# Define configurations
MINIO_CONFIG = {
    "endpoint": "172.17.0.2:9000",
    "access_key": "minio-user",
    "secret_key": "minio-password",
    "bucket_name": "warehouse",
}

TRINO_CONFIG = {
    "host": "localhost",
    "port": 8082,
    "user": "admin",
    "catalog": "lakehouse",
    "schema": "dev",
    "uri": "http://localhost:8082",
}

MOCK_API_URL = "https://61e67a17ce3a2d0017359174.mockapi.io/web-logs/web"
CONFIG_FILE_PATH = "config.properties"


# Create MinIO client
def create_minio_client(minio_config):
    client = Minio(
        minio_config["endpoint"],
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
        secure=False,
    )
    return client


# Create a bucket if it doesn't exist
def create_bucket(client, bucket_name):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")


# Fetch data from the mock API
def fetch_mock_data(api_url):
    response = requests.get(api_url)
    response.raise_for_status()  # Raise an error if the request fails
    data = response.json()
    daft_df = daft.from_pylist(data)
    return daft_df


# Save data to MinIO as Iceberg using Daft's write_iceberg function
def save_to_minio_as_iceberg(daft_df, minio_config, table_name="web_logs"):
    # # Define the Iceberg table location
    # iceberg_path = f"s3a://{minio_config['bucket_name']}/{table_name}"
    # Access a PyIceberg table as per normal
    from pyiceberg.catalog import load_catalog

    # Load the Iceberg catalog using the appropriate URI
    catalog = load_catalog(
        "lakehouse",
        **{
            "uri": "postgresql+psycopg2://postgres-user:postgres-password@localhost/iceberg",
            "type": "sql",
            "s3.endpoint": "http://172.17.0.2:9000", #add HTTP for bypassing SSL error
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.access-key-id": "minio-user",
            "s3.secret-access-key": "minio-password",
            "s3.path-style-access": False,
            "s3.ssl.enabled": False,
            "allow-drop-table": True,
            "echo": True
        }
    )
    ns = catalog.list_namespaces()
    os.environ["PYICEBERG_CATALOG__TRINO__FS_S3A_CONNECTION_SSL_ENABLED"] = "false"
    table = catalog.load_table("dev.weblogs")

    # Use the write_iceberg method in daft_df to write the data directly to the Iceberg table
    daft_df.write_iceberg(
        table=table,
        mode="overwrite",  # Choose 'overwrite' to replace existing data or 'append' to add new data
    )

    print(f"Data written to Iceberg table '{table_name}' in MinIO using Daft.")

if __name__ == "__main__":
    # # Write the configuration file
    # write_config_file(MINIO_CONFIG, TRINO_CONFIG, CONFIG_FILE_PATH)

    # Create MinIO client and bucket
    minio_client = create_minio_client(MINIO_CONFIG)
    create_bucket(minio_client, MINIO_CONFIG["bucket_name"])

    # Fetch data from the mock API and convert it to Daft DataFrame
    daft_df = fetch_mock_data(MOCK_API_URL)
    print("Data fetched from mock API:")
    print(daft_df.show())

    # Save data to MinIO as Iceberg table using Daft
    save_to_minio_as_iceberg(daft_df, MINIO_CONFIG, table_name="web_logs")
