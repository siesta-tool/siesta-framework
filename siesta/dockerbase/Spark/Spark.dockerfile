FROM apache/spark:4.0.1-python3 AS spark-aws

USER root

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

RUN curl -L -o /opt/spark/jars/hadoop-aws-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.540.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.540/aws-java-sdk-bundle-1.12.540.jar && \
    curl -L -o /opt/spark/jars/bundle-2.29.51.jar \
      https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.51/bundle-2.29.51.jar


FROM spark-aws AS spark-delta

RUN curl -L -o /opt/spark/jars/delta-spark_2.13-4.0.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar && \
    curl -L -o /opt/spark/jars/delta-storage-4.0.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar


FROM spark-delta AS spark-kafka

RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.0/spark-sql-kafka-0-10_2.13-4.0.0.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.7.0.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.0.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.0/spark-token-provider-kafka-0-10_2.13-4.0.0.jar


FROM spark-kafka AS final

# Install Python 3.12 (required for PEP 695 `type X = Y` syntax used in the codebase)
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository -y ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.12 python3.12-dev && \
    rm -rf /var/lib/apt/lists/*

RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.12

RUN python3.12 -m pip install --no-cache-dir \
    "boto3>=1.26.0" \
    "kafka-python>=2.0.0" \
    "pyspark>=4.0.1" \
    "python-multipart==0.0.22" \
    "fastapi[standard]==0.128" \
    "xxhash==3.6.0" \
    "pyarrow>=11.0.0" \
    numpy \
    pandas

ENV SPARK_CONF_DIR=/opt/spark/conf
RUN mkdir -p $SPARK_CONF_DIR && \
    echo "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" >> $SPARK_CONF_DIR/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" >> $SPARK_CONF_DIR/spark-defaults.conf
    
COPY log4j2.properties /opt/spark/conf/log4j2.properties
RUN chmod 0644 /opt/spark/conf/log4j2.properties

USER spark