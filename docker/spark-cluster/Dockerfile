FROM apache/spark:4.0.2-java21
USER root
ARG SPARK_VERSION=4.1.0
RUN apt-get update && apt-get install -y curl && \
    curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
      | tar -xz -C /tmp && \
    rm -rf /opt/spark/* && \
    mv /tmp/spark-${SPARK_VERSION}-bin-hadoop3/* /opt/spark/ && \
    rm -rf /tmp/spark-${SPARK_VERSION}-bin-hadoop3 && \
    apt-get remove -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/strongbow/jars
USER spark
