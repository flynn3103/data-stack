docker buildx build --platform linux/amd64,linux/arm64 -t custom-airflow:2.7.3-python3.9 --push .

docker buildx build --platform linux/arm64 -t custom-airflow:2.7.3-python3.9 --load .
