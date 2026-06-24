FROM apache/spark:3.5.1-python3

USER root

# Installer elasticsearch-py
RUN pip install --no-cache-dir elasticsearch==7.17.9

# Télécharger les JARs directement dans le dossier jars de Spark
RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -L -o /opt/spark/jars/elasticsearch-spark-30_2.12-7.17.18.jar \
    https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/7.17.18/elasticsearch-spark-30_2.12-7.17.18.jar && \
    curl -L -o /opt/spark/jars/scala-reflect-2.12.8.jar \
    https://repo1.maven.org/maven2/org/scala-lang/scala-reflect/2.12.8/scala-reflect-2.12.8.jar && \
    curl -L -o /opt/spark/jars/lz4-java-1.8.0.jar \
    https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar && \
    curl -L -o /opt/spark/jars/snappy-java-1.1.10.3.jar \
    https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.3/snappy-java-1.1.10.3.jar

USER spark