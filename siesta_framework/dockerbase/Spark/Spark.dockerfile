FROM public.ecr.aws/bitnami/spark:4.0.1 AS spark-aws

USER root

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -L -o /opt/bitnami/spark/jars/hadoop-aws-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.540.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.540/aws-java-sdk-bundle-1.12.540.jar && \
    curl -L -o /opt/bitnami/spark/jars/bundle-2.29.51.jar \
      https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.51/bundle-2.29.51.jar


FROM spark-aws AS spark-delta

RUN curl -L -O https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar && \
    curl -L -O https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar


FROM spark-delta AS spark-kafka

RUN curl -L -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.0/spark-sql-kafka-0-10_2.13-4.0.0.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.0/spark-token-provider-kafka-0-10_2.13-4.0.0.jar


FROM spark-kafka AS final

# Create symlink so /usr/bin/python3 points to the Bitnami Python
RUN ln -sf /opt/bitnami/python/bin/python3 /usr/bin/python3

# Install Python dependencies required by siesta_framework
# If many dependencies are finally needed to exist in workers, 
# use the following (update context in compose with ".") instead
# RUN /opt/bitnami/python/bin/pip install --no-cache-dir --upgrade pip && \
#     find /tmp/siesta_framework_build -name "requirements.txt" -type f -exec \
#     /opt/bitnami/python/bin/pip install --no-cache-dir -r {} \; && \
#     rm -rf /tmp/siesta_framework_build
RUN /opt/bitnami/python/bin/pip install --no-cache-dir \
    boto3>=1.26.0 \
    kafka-python>=2.0.0 \
    pyspark>=4.0.1 \
    python-multipart==0.0.22 \
    fastapi[standard]==0.128 \
    xxhash==3.6.0 \
    pandas \
    pyarrow

ENV SPARK_CONF_DIR=/opt/bitnami/spark/conf
RUN echo "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" >> $SPARK_CONF_DIR/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" >> $SPARK_CONF_DIR/spark-defaults.conf

COPY log4j2.properties /opt/bitnami/spark/conf/log4j2.properties
RUN chmod 0644 /opt/bitnami/spark/conf/log4j2.properties

USER 1001