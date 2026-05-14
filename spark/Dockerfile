FROM apache/spark:3.5.5

USER root

RUN apt-get update && apt-get install -y --no-install-recommends python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir requests pandas delta-spark==3.2.0 pydeequ

# Download PostgreSQL JDBC driver for Gold layer writes
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/spark/jars/postgresql-42.7.3.jar
# Download Kafka client JARs for Structured Streaming
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar /opt/spark/jars/kafka-clients-3.6.1.jar
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar /opt/spark/jars/commons-pool2-2.12.0.jar
# Download Delta Lake JARs for Lakehouse table format
ADD https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar /opt/spark/jars/delta-spark_2.12-3.2.0.jar
ADD https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar /opt/spark/jars/delta-storage-3.2.0.jar

ENV SPARK_VERSION=3.5.5

RUN chown -R spark:spark /opt/spark/jars/

USER spark
