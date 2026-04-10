# Runtime stage
FROM eclipse-temurin:8-jre

ENV SPARK_VERSION=3.5.0 \
    SPARK_HOME=/opt/spark \
    PATH="${PATH}:${SPARK_HOME}/bin"

WORKDIR /opt

# Install Spark
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    && wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    && tar xzf "spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    && mv "spark-${SPARK_VERSION}-bin-hadoop3" spark \
    && rm "spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    && rm -rf /var/lib/apt/lists/*

WORKDIR ${SPARK_HOME}

CMD ["bash"]
