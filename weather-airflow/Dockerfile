FROM apache/airflow:3.2.2

USER root

RUN apt-get update && apt-get install -y ca-certificates

RUN pip install --no-cache-dir \
    pandas \
    numpy \
    boto3 \
    s3fs \
    apache-airflow-providers-http \
    pymongo \
    certifi

USER airflow