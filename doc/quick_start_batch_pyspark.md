# Quick-Start-Batch-Pipeline-Manually - PYSPARK


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