airflow_up:
	docker build -f airflow/socat.Dockerfile -t socat-proxy:latest .
	docker compose -f airflow/airflow-docker-compose.yaml up -d
airflow_build:
	docker compose -f airflow/airflow-docker-compose.yaml up --build -d
trino_up:
	docker compose -f trino/docker-compose.yml up -d
trino_down:
	docker compose -f trino/docker-compose.yml down
trino_restart:
	docker compose -f trino/docker-compose.yml down
	docker compose -f trino/docker-compose.yml up -d