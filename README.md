# NYC_Taxi_Pipeline

> Set up the pipelines (batch/stream) from [nyc-tlc-trip-records-data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page), via the ETL process :
E (extract : tlc-trip-record-data.page -> S3 ) -> T (transform : S3 -> Spark) -> L (load : Spark -> Mysql), then calculate the `Supply VS Demand ratio` for `Surging price` application. 

* Tech : Spark, Kafka, S3, Mysql, Python 
* Batch pipeline : [batch_pipeline.py](https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/batch/batch_pipeline.py)
* Stream pipeline : [stream_pipeline.py](https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/stream/stream_pipeline.py)
* S3 data to stream : [s3_csv_2_stream.py](https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/stream/s3_csv_2_stream.py)

## Framework  
<img src ="https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/doc/framework.png" width="800" height="400">


## File structure 
```
├── README.md
├── batch             : scripts for batch pipeline 
├── config            : configuration files 
├── data              : saved NYC nyc-tlc-trip-records-pds data / sample data 
├── doc               : reference 
├── kafka             : scripts for kafka 
├── requirements.txt  : needed python libraries 
├── script            : scripts help set up env/services 
├── stream            : scripts for stream pipeline 
└── utility           : help scripts for pipeline

```

## Prerequisites
<details>
<summary>Prerequisites</summary>

```
# 1. Install spark, Java 8, zoopkeeper, and kafka, Mysql
# 2. Set up AWS account and launch S3 service
# 3. Get AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY 
```
</details>

## Quick start 
<details>
<summary>Quick-Start-Batch-Pipeline-Manually</summary>

```bash 
$ git clone https://github.com/yennanliu/NYC_Taxi_Pipeline.git
$ cd NYC_Taxi_Pipeline
$ pip install -r requirements.txt 
$ export AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
$ export AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
# download and upload data to s3
# currently use sample texi trip data at s3 nyctaxi bucket yellow_trip folder  
$ bash script/download_2_s3.sh
# init mysql 
# run mysql server local 
# create new user "mysql_user" with password "0000" 
$ mysql --user="mysql_user" --database="taxi" --password="0000" < "script/create_table.sql"  # create db 
# enter mysql password for <mysql_user>
$ mysql -u mysql_user -p  taxi < script/create_table.sql  # create tables 
$ export SPARK_HOME=/Users/$USER/spark && export PATH=$SPARK_HOME/bin:$PATH
$ spark-submit batch/batch_pipeline_manually.py 
# check the results (mysql)
# mysql> SELECT pickup.*,
#     ->        dropoff.*
#     -> FROM
#     ->   (SELECT DATE_FORMAT(MIN(Trip_Pickup_DateTime), '%d/%m/%Y %H:%i:00') AS pickup_tmstamp,
#     ->           geo_hash_id as pickup_geo_hash_id,
#     ->           COUNT(id) AS count_in_time_interval
#     ->    FROM pickup_geo_hash
#     ->    GROUP BY ROUND(UNIX_TIMESTAMP(Trip_Pickup_DateTime) / 300),
#     ->             geo_hash_id) AS pickup
#     -> INNER JOIN
#     ->   (SELECT DATE_FORMAT(MIN(Trip_Dropoff_DateTime), '%d/%m/%Y %H:%i:00') AS dropoff_tmstamp,
#     ->           geo_hash_id as dropoff_geo_hash_id,
#     ->           COUNT(id) AS count_in_time_interval
#     ->    FROM dropoff_geo_hash
#     ->    GROUP BY ROUND(UNIX_TIMESTAMP(Trip_Dropoff_DateTime) / 300),
#     ->             geo_hash_id) AS dropoff ON pickup.pickup_tmstamp = dropoff.dropoff_tmstamp
#     -> AND pickup.pickup_geo_hash_id = dropoff.dropoff_geo_hash_id;
# +---------------------+--------------------+------------------------+---------------------+---------------------+------------------------+
# | pickup_tmstamp      | pickup_geo_hash_id | count_in_time_interval | dropoff_tmstamp     | dropoff_geo_hash_id | count_in_time_interval |
# +---------------------+--------------------+------------------------+---------------------+---------------------+------------------------+
# | 14/01/2009 07:29:00 | dr72h8e            |                      1 | 14/01/2009 07:29:00 | dr72h8e             |                      1 |
# | 14/01/2009 07:33:00 | dr5rsnk            |                      1 | 14/01/2009 07:33:00 | dr5rsnk             |                      1 |
# | 24/01/2009 11:16:00 | dr5ru53            |                      1 | 24/01/2009 11:16:00 | dr5ru53             |                      1 |
# +---------------------+--------------------+------------------------+---------------------+---------------------+------------------------+
# 3 rows in set (0.01 sec)

# mysql> 

```
</details>

<details>
<summary>Quick-Start-Stream-Pipeline-Manually</summary>

```bash 
$ git clone https://github.com/yennanliu/NYC_Taxi_Pipeline.git
$ cd NYC_Taxi_Pipeline
$ pip install -r requirements.txt 
$ export AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
$ export AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
# download and upload data to s3
# currently use sample texi trip data at s3 nyctaxi bucket yellow_trip folder  
$ bash script/download_2_s3.sh
# start kafka, zookeeper 
$ bash kafka/start_kafka.sh

# open one termainl 
# stream data from s3 
$ python stream/s3_csv_2_stream.py
# >>>>>>>> output 
# "b',vendor_name,Trip_Pickup_DateTime,Trip_Dropoff_DateTime,Passenger_Count,Trip_Distance,Start_Lon,Start_Lat,Rate_Code,store_and_forward,End_Lon,End_Lat,Payment_Type,Fare_Amt,surcharge,mta_tax,Tip_Amt,Tolls_Amt,Total_Amt"
# "0,VTS,2009-01-04 02:52:00,2009-01-04 03:02:00,1.0,2.63,-73.991957,40.721567,,,-73.993803,40.695922,CASH,8.9,0.5,,0.0,0.0,9.4"
# "1,VTS,2009-01-04 03:31:00,2009-01-04 03:38:00,3.0,4.55,-73.982102,40.73629,,,-73.95585,40.76803,Credit,12.1,0.5,,2.0,0.0,14.6"
#....

# open the other terminal 
# run the stream pipeline 
$ spark-submit --jars /Users/$USER/spark/jars/spark-streaming-kafka-0-8-assembly_2.11-2.4.3.jar stream/stream_pipeline.py localhost:9092 new_topic
# >>>>>>>> output 
# ========= 2019-08-26 10:37:16 =========
# >>>>>>>> RESULT OF wordCountsDataFrame
# +-----------+--------------------+---------------------+---------------+-------------+------------------+---------+---------+-----------------+------------------+---------+------------+--------+---------+-------+-------+---------+---------+
# |vendor_name|Trip_Pickup_DateTime|Trip_Dropoff_DateTime|Passenger_Count|Trip_Distance|         Start_Lon|Start_Lat|Rate_Code|store_and_forward|           End_Lon|  End_Lat|Payment_Type|Fare_Amt|surcharge|mta_tax|Tip_Amt|Tolls_Amt|Total_Amt|
# +-----------+--------------------+---------------------+---------------+-------------+------------------+---------+---------+-----------------+------------------+---------+------------+--------+---------+-------+-------+---------+---------+
# |        CMT| 2009-01-06 07:26:38|  2009-01-06 07:33:34|            1.0|          0.8|        -73.991011|40.755011|         |                 |        -73.991011|40.755011|        Cash|     5.7|      0.0|       |    0.0|      0.0|      5.7|
# |        CMT| 2009-01-05 19:48:40|  2009-01-05 19:52:46|            1.0|          0.8|        -73.963949|40.770452|         |                 |        -73.959548|40.780016|        Cash|     5.5|      0.0|       |    0.0|      0.0|      5.5|
# |        CMT| 2009-01-05 20:37:46|  2009-01-05 20:57:20|            1.0|          5.6|        -73.979883|40.748998|         |                 |        -73.974006|40.677958|        Cash|    17.0|      0.0|       |    0.0|      0.0|     17.0|
# |        CMT| 2009-01-05 20:49:46|  2009-01-05 21:00:34|            2.0|          2.0|        -73.993437|40.751452|         |                 |
# taxidf : [Row(vendor_name='CMT', Trip_Pickup_DateTime=datetime.datetime(2009, 1, 6, 7, 26, 38), Trip_Dropoff_DateTime=datetime.datetime(2009, 1, 6, 7, 33, 34), Passenger_Count=1, Trip_Distance=0.800000011920929, Start_Lon=-73.99101257324219, Start_Lat=40.75501251220703, Rate_Code=''
# pickup_geohash : [Row(Trip_Pickup_DateTime=datetime.datetime(2009, 1, 6, 7, 26, 38), Start_Lat=40.75501251220703, Start_Lon=-73.99101257324219, geo_hash_id='dr5ru71'), Row(Trip_Pickup_DateTime=datetime.datetime(2009, 1, 5, 19, 48, 40), Start_Lat=40.770450592041016, Start_Lon=-73.96395111083984, geo_hash_id=
# dropoff_geohash : [Row(Trip_Dropoff_DateTime=datetime.datetime(2009, 1, 6, 7, 33, 34), End_Lat=40.75501251220703, End_Lon=-73.99101257324219, geo_hash_id='dr5ru71'), Row(Trip_Dropoff_DateTime=datetime.datetime(2009, 1, 5, 19, 52, 46), End_Lat=40.7800178527832, End_Lon=-73.95954895019531, geo_hash_id='

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
