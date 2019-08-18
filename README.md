# NYC_Taxi_Pipeline


### Process 
```
                             --> Stream pre-process --> stream dataset -->  stream pipeline --> real-time report 
NYC tlc-trip-record-data ---> 

                             --> Storage to DB/csv --> batch pipeline --> historical report  

```

### Quick start 
<details>
<summary>Quick-Start-Batch-Pipeline-Manually</summary>

```bash 
$ git clone https://github.com/yennanliu/NYC_Taxi_Pipeline.git
$ cd NYC_Taxi_Pipeline
# download and upload data to s3 
# init mysql 
# run mysql server local 
$ echo "create database `taxi`" | mysql -u <user_name> -p <mysql_password>
$ mysql -u <mysql_user_name> -p <mysql_password> taxi < batch/create_table.sql
$ export SPARK_HOME=/Users/$USER/spark && export PATH=$SPARK_HOME/bin:$PATH
$ spark-submit batch/batch_spark_test.py 
# check the results (mysql)
# mysql> 
# mysql> SELECT pickup.*,
#     ->        dropoff.*
#     -> FROM
#     ->   (SELECT DATE_FORMAT(MIN(Trip_Pickup_DateTime), '%d/%m/%Y %H:%i:00') AS tmstamp,
#     ->           geo_hash_id,
#     ->           COUNT(id) AS count_in_time_interval
#     ->    FROM pickup_geo_hash
#     ->    GROUP BY ROUND(UNIX_TIMESTAMP(Trip_Pickup_DateTime) / 300),
#     ->             geo_hash_id) AS pickup
#     -> INNER JOIN
#     ->   (SELECT DATE_FORMAT(MIN(Trip_Dropoff_DateTime), '%d/%m/%Y %H:%i:00') AS tmstamp,
#     ->           geo_hash_id,
#     ->           COUNT(id) AS count_in_time_interval
#     ->    FROM dropoff_geo_hash
#     ->    GROUP BY ROUND(UNIX_TIMESTAMP(Trip_Dropoff_DateTime) / 300),
#     ->             geo_hash_id) AS dropoff ON pickup.tmstamp = dropoff.tmstamp
#     -> AND pickup.geo_hash_id = dropoff.geo_hash_id;
# +---------------------+-------------+------------------------+---------------------+-------------+------------------------+
# | tmstamp             | geo_hash_id | count_in_time_interval | tmstamp             | geo_hash_id | count_in_time_interval |
# +---------------------+-------------+------------------------+---------------------+-------------+------------------------+
# | 14/01/2009 07:29:00 | dr72h8e     |                      1 | 14/01/2009 07:29:00 | dr72h8e     |                      1 |
# | 14/01/2009 07:33:00 | dr5rsnk     |                      1 | 14/01/2009 07:33:00 | dr5rsnk     |                      1 |
# | 24/01/2009 11:16:00 | dr5ru53     |                      1 | 24/01/2009 11:16:00 | dr5ru53     |                      1 |
# +---------------------+-------------+------------------------+---------------------+-------------+------------------------+
# 3 rows in set (0.01 sec)

# mysql> 
 
```
</details>
