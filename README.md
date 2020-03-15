<p align="center"><img src ="https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/doc/pic/taxi_logo.png" width="600" height="200"></p>

# NYC_Taxi_Pipeline

## INTRO
> Set up the pipelines (batch/stream) from [nyc-tlc-trip-records-data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page), via the ETL process :
E (extract : tlc-trip-record-data.page -> S3 ) -> T (transform : S3 -> Spark) -> L (load : Spark -> Mysql), then calculate the `Supply VS Demand ratio` for `Surging price` application. 

* Tech : Spark, Hadoop, Hive, EMR, S3, MySQL, Fluentd, Kinesis, DynamoDB , Scala, Python 
* Download sample data : [download_sample_data.sh](https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/script/download_sample_data.sh)
* Batch pipeline : [DataLoad](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/src/main/scala/DataLoad) -> [DataTransform](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/src/main/scala/DataTransform) -> [CreateView](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/src/main/scala/CreateView) -> [SaveToDB](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/src/main/scala/SaveToDB) -> [SaveToHive](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/src/main/scala/SaveToHive)
* Stream pipeline : [TaxiEvent](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/src/main/scala/TaxiEvent) -> [EventLoad](https://github.com/yennanliu/NYC_Taxi_Pipeline/tree/master/src/main/scala/EventLoad)

> Please also check [NYC_Taxi_Trip_Duration](https://github.com/yennanliu/NYC_Taxi_Trip_Duration) in case you are interested in the data science projects with similar taxi dataset. 

## Architecture 
<img src ="https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/doc/pic/batch_architecture.svg" width="800" height="400">
<img src ="https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/doc/pic/stream_architecture.svg" width="800" height="400">

- Architecture idea (Batch):
- Architecture idea (Stream):

## File structure 
```
├── README.md
├── batch             : scripts for batch pipeline 
├── config            : configuration files 
├── data              : saved NYC nyc-tlc-trip-records-pds data / sample data 
├── doc               : reference 
├── kafka             : scripts for kafka 
├── requirements.txt  : needed python libraries 
├── script            : help scripts (env/services) 
├── src               : Main working script (Scala/Python)
├── stream            : scripts for stream pipeline 
└── utility           : help scripts (pipeline)
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
# curl event to kafka producer
curl localhost:44444 | kafka-console-producer  --broker-list  127.0.0.1:9092 --topic first_topic

# STEP 5) Spark process kafka stream
spark-submit \
 --class KafkaEventLoad.LoadKafkaEventExample \
 target/scala-2.11/nyc_taxi_pipeline_2.11-1.0.jar

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
