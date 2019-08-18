# NYC_Taxi_Pipeline

> Set up the pipelines (batch/stream) from [nyc-tlc-trip-records-data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page), via the ETL process :
E (extract : tlc-trip-record-data.page -> S3 ) -> T (transform : S3 -> Spark) -> L (load : Spark -> Mysql), then calculate the `Supply VS Demand ratio` for `Surging price` application. 

* Tech : Spark, Kafka, S3, Mysql, Python 



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
$ echo "create database `taxi`" | mysql -u <user_name> -p <mysql_password>
$ mysql -u <mysql_user_name> -p <mysql_password> taxi < batch/create_table.sql
$ export SPARK_HOME=/Users/$USER/spark && export PATH=$SPARK_HOME/bin:$PATH
$ spark-submit batch/batch_pipeline.py 
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
# start streaming data with kafka 
$ bash script/kafka-run.sh --produce
$ bash stream/spark-run.sh --stream
```
</details>

### TODO 
<details>
<summary>TODO</summary>

```
# 1. Tune the main pipeline for large scale data (to process whole nyc-tlc-trip data)
# 2. Add front-end UI (flask to visualize supply & demand and surging price)
# 2. Add test 
# 3. Dockerize the project 
# 4. Tune the spark batch/stream code 
# 5. Tune the kafka, zoopkeeper cluster setting 
# 6. Travis CI/CD 
# 7. Use Airflow to schedule batch pipeline 
```
</details>
