FROM public.ecr.aws/bitnami/spark:4.0.0

USER root

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -L -o /opt/bitnami/spark/jars/hadoop-aws-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.540.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.540/aws-java-sdk-bundle-1.12.540.jar && \
    curl -L -o /opt/bitnami/spark/jars/bundle-2.29.51.jar \
      https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.51/bundle-2.29.51.jar


# Create symlink so /usr/bin/python3 points to the Bitnami Python
RUN ln -sf /opt/bitnami/python/bin/python3 /usr/bin/python3

COPY log4j2.properties /opt/bitnami/spark/conf/log4j2.properties
RUN chmod 0644 /opt/bitnami/spark/conf/log4j2.properties

USER 1001