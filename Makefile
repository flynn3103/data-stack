airflow_up:
	docker build -f airflow/socat.Dockerfile -t socat-proxy:latest .
	docker compose -f airflow/airflow-docker-compose.yaml up -d
airflow_build:
	docker compose -f airflow/airflow-docker-compose.yaml up --build -d
