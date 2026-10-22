FROM apache/spark:4.2.0-java17
USER root
ARG COMET_VERSION=1.2.0-20261020.040418-9
ARG COMET_SHA1=fc71728177ff7293f21c6a01f50ace357e1b62c7
ARG COMET_REPO=https://repository.apache.org/content/repositories/snapshots/org/apache/datafusion/comet-spark-spark4.2_2.13/1.2.0-SNAPSHOT
RUN apt-get update && apt-get install -y curl && \
    mkdir -p /opt/comet && \
    curl -fSL -o "/opt/comet/comet-spark-spark4.2_2.13-${COMET_VERSION}.jar" \
      "${COMET_REPO}/comet-spark-spark4.2_2.13-${COMET_VERSION}.jar" && \
    echo "${COMET_SHA1}  /opt/comet/comet-spark-spark4.2_2.13-${COMET_VERSION}.jar" | sha1sum -c - && \
    apt-get remove -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/vaire/jars
USER spark
