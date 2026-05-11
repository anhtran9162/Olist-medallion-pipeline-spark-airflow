FROM bitnami/spark:3.5

USER root

# Install Python packages needed by Spark jobs
RUN pip install --no-cache-dir requests pandas

# Download PostgreSQL JDBC driver for Gold layer writes
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/bitnami/spark/jars/postgresql-42.7.3.jar

# Download Kafka client JARs for Structured Streaming
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar /opt/bitnami/spark/jars/kafka-clients-3.6.1.jar
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar /opt/bitnami/spark/jars/commons-pool2-2.12.0.jar

RUN chown -R 1001:1001 /opt/bitnami/spark/jars/

USER 1001
