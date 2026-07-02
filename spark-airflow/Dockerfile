FROM apache/airflow:3.2.2

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget tar

ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
 && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt \
 && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
 && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

USER airflow

COPY requirements.txt /

RUN pip install --no-cache-dir -r /requirements.txt