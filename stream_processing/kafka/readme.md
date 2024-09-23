docker build -t nyc_producer:latest .
docker image tag nyc_producer:latest data_stack/nyc_producer:latest
docker push data_stack/nyc_producer:latest