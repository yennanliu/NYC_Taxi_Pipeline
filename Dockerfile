# Build stage
FROM eclipse-temurin:8-jdk as builder

WORKDIR /build

# Install sbt
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && curl -fsSL "https://github.com/sbt/sbt/releases/download/v1.9.0/sbt-1.9.0.tgz" | tar xfz - -C /usr/local \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/usr/local/sbt/bin:${PATH}"

# Copy project files
COPY build.sbt .
COPY project/ project/
COPY src/ src/

# Build the project
RUN sbt clean compile assembly

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

# Copy built JAR from builder
COPY --from=builder /build/target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar ${SPARK_HOME}/jars/

WORKDIR ${SPARK_HOME}

CMD ["bash"]
