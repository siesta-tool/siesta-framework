FROM public.ecr.aws/bitnami/spark:4.0.0

USER root

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -L -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

COPY log4j2.properties /opt/bitnami/spark/conf/log4j2.properties
RUN chmod 0644 /opt/bitnami/spark/conf/log4j2.properties

USER 1001