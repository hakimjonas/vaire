FROM apache/spark:4.0.2-java21
USER root
ARG SPARK_VERSION=4.1.3
ARG COMET_VERSION=1.1.0
ARG COMET_SHA1=ed6a6af4d6140ac2babea18b907eb0679c1161ce
RUN apt-get update && apt-get install -y curl && \
    curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
      | tar -xz -C /tmp && \
    rm -rf /opt/spark/* && \
    mv /tmp/spark-${SPARK_VERSION}-bin-hadoop3/* /opt/spark/ && \
    rm -rf /tmp/spark-${SPARK_VERSION}-bin-hadoop3 && \
    mkdir -p /opt/comet && \
    curl -fSL -o "/opt/comet/comet-spark-spark4.1_2.13-${COMET_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark4.1_2.13/${COMET_VERSION}/comet-spark-spark4.1_2.13-${COMET_VERSION}.jar" && \
    echo "${COMET_SHA1}  /opt/comet/comet-spark-spark4.1_2.13-${COMET_VERSION}.jar" | sha1sum -c - && \
    apt-get remove -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/vaire/jars
USER spark
