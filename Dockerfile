# FROM apache/airflow:2.9.3-python3.11
FROM apache/airflow:2.9.3-python3.10

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        git \
        openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==2.9.3" \
    "apache-airflow-providers-apache-spark==4.8.2" \
    "pyspark==3.5.1" \
    # --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt"

# Install dbt Snowflake separately
RUN pip install --no-cache-dir \
    "dbt-snowflake==1.8.4"

# ijson: streaming JSON parser used by the fetch scripts to sample large
# landed files without loading them fully into memory
RUN pip install --no-cache-dir "ijson==3.3.0"