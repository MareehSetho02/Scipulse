# Base commune aux tâches Airflow et au job Spark.
FROM apache/spark:3.5.7 AS spark
FROM apache/airflow:2.9.3-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && rm -rf /var/lib/apt/lists/*
COPY --from=spark /opt/spark /opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 SPARK_HOME=/opt/spark PYSPARK_PYTHON=/home/airflow/.local/bin/python PYTHONPATH=/opt/airflow
RUN ln -sf /usr/local/bin/python /home/airflow/.local/bin/python && mkdir -p /opt/spark-extra && chown -R airflow:0 /opt/spark-extra
COPY docker/runtime/requirements.txt /tmp/project-requirements.txt
USER airflow
RUN pip install --no-cache-dir "apache-airflow==2.9.3" -r /tmp/project-requirements.txt
RUN python -m venv /home/airflow/dbt-env && /home/airflow/dbt-env/bin/pip install --no-cache-dir dbt-postgres==1.9.1
RUN curl -fSL --retry 3 https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark-extra/hadoop-aws.jar && curl -fSL --retry 3 https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark-extra/aws-sdk.jar && curl -fSL --retry 3 https://repo.maven.apache.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.17.0/elasticsearch-spark-30_2.12-8.17.0.jar -o /opt/spark-extra/elasticsearch-spark.jar
WORKDIR /opt/airflow
