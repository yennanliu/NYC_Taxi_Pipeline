#!/bin/sh

git clone https://github.com/yennanliu/NYC_Taxi_Pipeline.git
cd NYC_Taxi_Pipeline
docker build . -t spark_env
docker run  --mount \
type=bind,\
source="$(pwd)"/.,\
target=/NYC_Taxi_Pipeline \
-i -t spark_env \
/bin/bash 