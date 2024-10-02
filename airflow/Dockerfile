FROM apache/airflow:2.7.3-python3.9


USER root
WORKDIR /opt/airflow
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    build-essential \
    libpq-dev \
    python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
ENV DBT_PROFILES_DIR=/opt/airflow/dbt
ENV DBT_USER='dbt_admin'
ENV DBT_PASSWORD='dbt_password'
# Copy requirements.txt into the Docker image
# add dags
COPY ./dags/ /opt/airflow/dags
# Copy dbt project
COPY ./dbt/ /opt/airflow/dbt/

COPY requirements.txt requirements.txt

# This is to fix a bug in Airflow with PostgreSQL connection
RUN pip install --no-cache-dir -r requirements.txt
# # Requirement for running Docker Operator
# RUN pip install apache-airflow-providers-docker==3.7.5

# RUN chown -R airflow:airflow /opt/airflow/dbt
