# Docker Setup for NYC Taxi Pipeline

## Build the Docker Image

```bash
docker build -t spark-pipeline:latest .
```

## Run Spark Commands

### Check Spark version
```bash
docker run --rm spark-pipeline:latest spark-shell --version
```

### Run Spark-Submit
```bash
docker run --rm spark-pipeline:latest spark-submit --version
```

### Run Scala Job
```bash
docker run --rm spark-pipeline:latest spark-submit \
  --class DataLoad.LoadReferenceData \
  /opt/spark/jars/nyc_taxi_pipeline_2.11-1.0.jar
```

### Interactive Spark Shell
```bash
docker run -it --rm spark-pipeline:latest spark-shell
```

## Docker Image Details

- **Base Image**: eclipse-temurin:8-jre (Java 8)
- **Spark Version**: 3.5.0
- **Build**: Multi-stage build for smaller final image
- **JAR**: NYC Taxi Pipeline JAR is included at `/opt/spark/jars/nyc_taxi_pipeline_2.11-1.0.jar`

## Local Build & Test

The GitHub Actions workflow (`spark-docker.yml`) automatically:
1. Builds the Docker image
2. Tests Spark installation
3. Verifies the compiled JAR exists
