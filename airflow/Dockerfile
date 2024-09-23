FROM apache/airflow:2.7.3-python3.9

USER root
RUN apt-get update && \
    apt-get install -y git && \
    apt-get install -y libgomp1

USER airflow
# Copy requirements.txt into the Docker image
COPY requirements.txt /requirements.txt

# This is to fix a bug in Airflow with PostgreSQL connection
RUN pip install -r /requirements.txt
# # Requirement for running Docker Operator
# RUN pip install apache-airflow-providers-docker==3.7.5