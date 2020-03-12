#!/bin/sh

echo '>>>> DOCKER BUILD...'
docker build . -t spark_env
echo '>>>> RUN SCALA SPARK ETL PIPELINE BUILD...'
docker run  --mount \
type=bind,\
source="$(pwd)"/.,\
target=/spark_env \
-i -t spark_env \
/bin/bash  -c "cd ../spark_env && sbt package"