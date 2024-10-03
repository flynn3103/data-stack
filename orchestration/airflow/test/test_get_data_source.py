import os
import sys
import pytest
from airflow.models import DagBag
import requests
import pandas as pd
from unittest.mock import patch, MagicMock

# Add the 'dags' directory to the Python path
current_dir = os.path.dirname(
    os.path.abspath(__file__)
)  # Get the current directory of this test file
# Construct the absolute path to the dags directory
dags_dir = os.path.abspath(os.path.join(current_dir, "../dags"))
sys.path.append(os.path.dirname(dags_dir))  # Append parent path to sys path

from dags.get_data_source import download_data_task, process_parquet_files_task


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="/opt/airflow/dags", include_examples=False)


def test_dag_loaded(dagbag):
    """Test that the DAG is correctly loaded"""
    dag_id = "get_data_from_source"
    dag = dagbag.get_dag(dag_id)
    assert dagbag.import_errors == {}, "There shouldn't be any DAG import errors"
    assert dag is not None, f"DAG '{dag_id}' should be loaded correctly"


def test_task_count(dagbag):
    """Test that the DAG contains the correct number of tasks"""
    dag_id = "get_data_from_source"
    dag = dagbag.get_dag(dag_id)
    assert len(dag.tasks) == 7, "The DAG should have 5 tasks"


# Mock the DATA_DIR path used in the functions
DATA_DIR = "/opt/airflow/data/"


# Fixture to create the data directory for testing
@pytest.fixture(scope="module", autouse=True)
def create_data_directory():
    os.makedirs(DATA_DIR, exist_ok=True)
    yield
    # Cleanup after tests
    for file in os.listdir(DATA_DIR):
        if file.endswith(".parquet"):
            os.remove(os.path.join(DATA_DIR, file))


@patch("requests.get")
def test_download_data(mock_get):
    # Mock the response from requests.get
    mock_response = requests.Response()
    mock_response.status_code = 200
    mock_response._content = b"Test content"  # Simulate binary content
    mock_get.return_value = mock_response

    # Call the function
    download_data_task("yellow")

    # Check that the file was created
    expected_file_path = os.path.join(DATA_DIR, "yellow_tripdata_2021-01.parquet")
    assert os.path.exists(
        expected_file_path
    ), "The data file should be downloaded and saved"
