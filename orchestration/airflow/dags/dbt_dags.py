from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
}

with DAG('dbt_workflow', default_args=default_args, schedule_interval='@daily') as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run --profiles-dir $DBT_PROFILES_DIR --project-dir /opt/airflow/dbt',
        env={
            'DBT_USER': '{{ var.value.get("db_user") }}',
            'DBT_PASSWORD': '{{ var.value.get("db_password") }}',
            # Add other environment variables as needed
        }
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test --profiles-dir $DBT_PROFILES_DIR --project-dir /opt/airflow/dbt',
    )

    dbt_run >> dbt_test
