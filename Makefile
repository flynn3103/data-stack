airflow_up:
	docker build -f airflow/socat.Dockerfile -t socat-proxy:latest .
	docker compose -f airflow/docker-compose.yaml up -d
airflow_build:
	docker compose -f airflow/docker-compose.yaml up --build -d
airflow_down:
	docker compose -f airflow/docker-compose.yaml down
trino_up:
	docker compose -f trino/docker-compose.yml up -d
trino_down:
	docker compose -f trino/docker-compose.yml down
trino_restart:
	docker compose -f trino/docker-compose.yml down
	docker compose -f trino/docker-compose.yml up -d
kafka_up:
	docker compose -f streaming/docker-compose.yml up -d
kafka_down:
	docker compose -f streaming/docker-compose.yml down
spark_up:
	docker build -f spark/Dockerfile -t custom-apache-spark:3.5.1 .
	docker compose -f spark/docker-compose.yml up -d
spark_down:
	docker compose -f spark/docker-compose.yml down