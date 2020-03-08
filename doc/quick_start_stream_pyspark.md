# Quick-Start-Stream-Pipeline-Manually - PYSPARK

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