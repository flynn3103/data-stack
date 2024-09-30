## Airflow

+ Airflow is a service to manage and schedule data pipeline
+ In this repo, airflow is run data pipeline (download data, transform data, insert data, check expectations,...)


Note: using commnad `sudo chmod 777 -R data` if error happen 

docker buildx build --platform linux/amd64,linux/arm64 -t custom-airflow:2.7.3-python3.9 --push .

helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm repo list
helm search repo airflow --versions
helm show values apache-airflow/airflow --version=1.0.0 > values.yaml

kubectl create namespace airflow

kubectl apply -f variables.yaml
helm list --namespace airflow
helm upgrade airflow apache-airflow/airflow --namespace airflow -f values.yaml --version=1.0.0 \
--set airflowVersion=2.7.3 \
--set defaultAirflowTag=2.7.3 \
--set executor=CeleryExecutor \
--set redis.enabled=True \
--set flower.enabled=True \
--set webserver.service.type=ClusterIP \
--set webserver.replicas=2 \
--set scheduler.replicas=2 \
--set pgbouncer.enabled=True \
--set pgbouncer.maxClientConn=150 \
--set pgbouncer.metadataPoolSize=10 \
--set pgbouncer.resultBackendPoolSize=5 \
--set workers.replicas=5 \
--set workers.persistence.enabled=True \
--set workers.terminationGracePeriodSeconds=600 \
--set workers.persistence.size=10Gi


Deploy new DAG

