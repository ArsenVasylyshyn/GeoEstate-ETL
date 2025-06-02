FROM apache/airflow:2.9.1-python3.9

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    openjdk-17-jre-headless \
    curl \
    unzip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    pyspark \
    psycopg2-binary \
    clickhouse-driver \
    tabulate 
    