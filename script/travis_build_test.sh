#!/bin/sh

echo '>>>> DOCKER BUILD...'
docker build . -t spark_env
echo '>>>> RUN SCALA SPARK ETL PIPELINE BUILD...'
docker run  --mount \
type=bind,\
source="$(pwd)"/.,\
target=/spark_env \
-i -t spark_env \
/bin/bash  -c "cd ../spark_env && sbt package && spark-submit \
 --class DataLoad.LoadReferenceData \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar && spark-submit \
 --class DataLoad.LoadGreenTripData \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar && spark-submit \
 --class DataTransform.TransformGreenTaxiData \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar && spark-submit \
 --class CreateView.CreateMaterializedView \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar"