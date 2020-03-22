<p align="center"><img src ="https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/doc/pic/taxi_logo_V2.svg" width="2000" height="300"></p>

<h1 align="center">NYC Taxi Pipeline</a></h1>

<p align="center">
<!--- travis -->
<a href="https://travis-ci.org/yennanliu/NYC_Taxi_Pipeline"><img src="https://travis-ci.org/yennanliu/NYC_Taxi_Pipeline.svg?branch=master"></a>
<!--- PR -->
<a href="https://github.com/yennanliu/NYC_Taxi_Pipeline/pulls"><img src="https://img.shields.io/badge/PRs-welcome-6574cd.svg"></a>
<!--- notebooks mybinder -->
<a href="https://mybinder.org/v2/gh/yennanliu/NYC_Taxi_Pipeline/master"><img src="https://img.shields.io/badge/launch-Jupyter-5eba00.svg"></a>
</p>

## INTRO
> Architect `batch/stream` data processing systems from [nyc-tlc-trip-records-data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page), via the `ETL process`  :
E (extract : tlc-trip-record-data.page -> S3 ) -> T (transform : S3 -> Spark) -> L (load : Spark -> Mysql) & `stream process` : Event -> Event digest -> Event storage. The system then can support calculation such as `Supply VS Demand ratio` for `Surging price`, `latest-top-driver`, `current-busy-areas`.

> Batch data is from [nyc-tlc-trip-records-data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page); while the stream data is from various sorces : [Taxi-fake-event](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/src/main/scala/TaxiEvent), `load file as stream`.

* Tech : Spark, Hadoop, Hive, EMR, S3, MySQL, Fluentd, Kinesis, DynamoDB , Scala, Python 
* Download sample data : [download_sample_data.sh](https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/script/download_sample_data.sh)
* Batch pipeline : [DataLoad](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/src/main/scala/DataLoad) -> [DataTransform](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/src/main/scala/DataTransform) -> [CreateView](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/src/main/scala/CreateView) -> [SaveToDB](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/src/main/scala/SaveToDB) -> [SaveToHive](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/src/main/scala/SaveToHive)
	* Batch data : [transactional-data](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/data/staging/transactional-data), [reference-data](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/data/staging/reference-data) -> [processed-data](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/data/processed) -> [output-transactions](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/data/output/transactions) -> [output-materializedview](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/data/output/materializedview)
* Stream pipeline : [TaxiEvent](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/src/main/scala/TaxiEvent) -> [EventLoad](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/src/main/scala/EventLoad) -> [KafkaEventLoad](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/src/main/scala/KafkaEventLoad)

> Please also check [NYC_Taxi_Trip_Duration](https://github.com/yennanliu/NYC_Taxi_Trip_Duration) in case you are interested in the data science projects with similar taxi dataset. 

## Architecture 
<img src ="https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/doc/pic/batch_architecture.svg" width="800" height="400">
<img src ="https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/doc/pic/stream_architecture_V2.svg" width="800" height="400">

- Architecture idea (Batch):
- Architecture idea (Stream):

## File structure 
```
├── Dockerfile    : Scala spark Dockerfile
├── build.sbt     : Scala sbt build file
├── config        : configuration files for DB/Kafka/AWS..
├── data          : Raw/processed/output data
├── doc           : All repo reference/doc/pic
├── fluentd       : Fluentd help scripts
├── kafka         : Kafka help scripts
├── pyspark       : Legacy pipeline code (Python)
├── requirements.txt
├── script        : Help scripts (env/services) 
├── src           : Batch/stream process scripts (Scala)
└── utility       : Help scripts (pipeline)
```

## Prerequisites
<details>
<summary>Prerequisites</summary>

- Install 
	- Spark 2.4.3
	- Java 1.8.0_11 (java 8)
	- Scala 2.11.12
	- sbt 1.3.5
	- Zoopkeeper
	- Kafka
	- Mysql
	- Elasitic search (optional)
	- Hive (optional)
	- Hadoop (optional)
	- Fluentd (optional)
	- Python 3  (optional)
	- Pyspark (optional)

- Set up 
	- AWS account and get `key_pair` for access below services:
		- EMR
		- EC2
		- S3
		- DYNAMODB
		- Kinesis
- Config
	- update [config](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/config) with your creds  

</details>

## Quick start 
<details>
<summary>Quick-Start-Batch-Pipeline-Manually</summary>

```bash 
# STEP 1) Download the dataset
bash script/download_sample_data.sh

# STEP 2) sbt package 
sbt package

# STEP 3) Load data 
spark-submit \
 --class DataLoad.LoadReferenceData \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar

spark-submit \
 --class DataLoad.LoadGreenTripData \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar

spark-submit \
 --class DataLoad.LoadYellowTripData \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar

# STEP 4) Transform data 
spark-submit \
 --class DataTransform.TransformGreenTaxiData \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar

spark-submit \
 --class DataTransform.TransformYellowTaxiData \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar

# STEP 5) Create view 
spark-submit \
 --class CreateView.CreateMaterializedView \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar

# STEP 6) Save to JDBC (mysql)
spark-submit \
 --class SaveToDB.JDBCToMysql \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar

# STEP 7) Save to Hive
spark-submit \
 --class SaveToHive.SaveMaterializedviewToHive \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar

```

</details>

<details>
<summary>Quick-Start-Stream-Pipeline-Manually</summary>

```bash 
# STEP 1) sbt package 
sbt package

# STEP 2) Create Taxi event
spark-submit \
 --class TaxiEvent.CreateBasicTaxiEvent \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar

# check the event
curl localhost:44444

# STEP 3) Process Taxi event
spark-submit \
 --class EventLoad.SparkStream_demo_LoadTaxiEvent \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar

# STEP 4) Send Taxi event to Kafaka
# start zookeeper, kafka
brew services start zookeeper
brew services start kafka

# create kafka topic
kafka-topics --create -zookeeper localhost:2181 --replication-factor 1  --partitions 1 --topic first_topic
kafka-topics --create -zookeeper localhost:2181 --replication-factor 1  --partitions 1 --topic streams-taxi

# curl event to kafka producer
curl localhost:44444 | kafka-console-producer  --broker-list  127.0.0.1:9092 --topic first_topic

# STEP 5) Spark process kafka stream
spark-submit \
 --class KafkaEventLoad.LoadKafkaEventExample \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar

# STEP 6) Spark process kafka stream
spark-submit \
 --class KafkaEventLoad.LoadTaxiKafkaEventWriteToKafka \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar


# STEP 7) Run elsacsearch, kibana, logstach
cd ~ 
kibana-7.6.1-darwin-x86_64/bin/kibana
elasticsearch-7.6.1/bin/elasticsearch
logstash-7.6.1/bin/logstash -f config

# test insert toy data to logstash 
# (logstash config: elk/logstash.conf)
nc 127.0.0.1 5000 < data/event_sample.json

# then visit kibana UI : localhost:5601
# then visit "management" -> "index_patterns" -> "Create index pattern" 
# create new index : logstash-* (not select timestamp as filter)
# then visit the "discover" tag and check the data

```
</details>

### Dependency 
<details>
<summary>Dependency</summary>

1. Spark 2.4.3 
2. Java 8
3. Apache Hadoop 2.7
4. Jars 
	- [aws-java-sdk-1.7.4](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk/1.7.4)
	- [hadoop-aws-2.7.6](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/2.7.6)
	- [spark-streaming-kafka-0-8-assembly_2.11-2.4.3.jar](https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-8-assembly_2.11/2.4.3)
	- [mysql-connector-java-8.0.15.jar](https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.15)

5. [build.sbt](https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/build.sbt)

</details>

### Ref
<details>
<summary>Ref</summary>

- [ref.md](https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/doc/ref.md) - dataset link ref, code ref, other ref
- [doc](https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/doc) - All ref docs


</details>

### TODO 
<details>
<summary>TODO</summary>

```
# 1. Tune the main pipeline for large scale data (to process whole nyc-tlc-trip data)
# 2. Add front-end UI (flask to visualize supply & demand and surging price)
# 3. Add test 
# 4. Dockerize the project 
# 5. Tune the spark batch/stream code 
# 6. Tune the kafka, zoopkeeper cluster setting 
# 7. Travis CI/CD 
# 8. Use Airflow to schedule batch pipeline 
```
</details>
