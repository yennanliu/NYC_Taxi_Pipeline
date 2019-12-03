import sys
sys.path.append("./utility/")
import os
import pyspark
import math 
import pygeohash as pgh
from datetime import datetime
from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark.sql import functions as F
# UDF 
from utility import * 

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.7.4,org.apache.hadoop:hadoop-aws:2.7.6 pyspark-shell'
aws_creds = parse_config('config/aws_creds.config')
AWSAccessKeyId = aws_creds['AWSAccessKeyId']
AWSSecretKey = aws_creds['AWSSecretKey']

sc = SparkContext.getOrCreate()
sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', AWSAccessKeyId)
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', AWSSecretKey)
sqlContext = pyspark.sql.SQLContext(sc)

# file url
s3_samplefile = parse_config('config/s3_sample_file.config') 
green_trip_filename =s3_samplefile['green_trip_file']
yellow_trip_filename = s3_samplefile['yellow_trip_file']


##################################################################################
# green trip data 
##################################################################################


def load_s3_greentrip_data(filename=green_trip_filename):
    data = sc.textFile(filename).map(lambda line: line.split(","))
    headers = data.first()
    data_ = data.filter(lambda row: row != headers and row != [''])  # fix null data 
    dataFrame = sqlContext.createDataFrame(data_,
                ['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime',
                'store_and_fwd_flag', 'RatecodeID', 'PULocationID', 'DOLocationID',
                'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax',
                'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge',
                'total_amount', 'payment_type', 'trip_type', 'congestion_surcharge'])
    dataFrame = dataFrame.withColumn("VendorID", dataFrame["VendorID"].cast("float"))    
    dataFrame = dataFrame.withColumn("lpep_pickup_datetime", dataFrame["lpep_pickup_datetime"].cast("timestamp"))
    dataFrame = dataFrame.withColumn("lpep_dropoff_datetime", dataFrame["lpep_dropoff_datetime"].cast("timestamp"))
    dataFrame = dataFrame.withColumn("store_and_fwd_flag", dataFrame["store_and_fwd_flag"].cast("string"))
    dataFrame = dataFrame.withColumn("RatecodeID", dataFrame["RatecodeID"].cast("float"))
    dataFrame = dataFrame.withColumn("PULocationID", dataFrame["PULocationID"].cast("float"))
    dataFrame = dataFrame.withColumn("DOLocationID", dataFrame["DOLocationID"].cast("float"))
    dataFrame = dataFrame.withColumn("passenger_count", dataFrame["passenger_count"].cast("integer"))
    dataFrame = dataFrame.withColumn("trip_distance", dataFrame["trip_distance"].cast("float"))
    dataFrame = dataFrame.withColumn("fare_amount", dataFrame["fare_amount"].cast("float"))
    dataFrame = dataFrame.withColumn("extra", dataFrame["extra"].cast("float"))
    dataFrame = dataFrame.withColumn("trip_distance", dataFrame["trip_distance"].cast("float"))
    dataFrame = dataFrame.withColumn("mta_tax", dataFrame["mta_tax"].cast("float"))
    dataFrame = dataFrame.withColumn("tip_amount", dataFrame["tip_amount"].cast("float"))
    dataFrame = dataFrame.withColumn("tolls_amount", dataFrame["tolls_amount"].cast("float"))
    dataFrame = dataFrame.withColumn("ehail_fee", dataFrame["ehail_fee"].cast("float"))
    dataFrame = dataFrame.withColumn("improvement_surcharge", dataFrame["improvement_surcharge"].cast("float"))
    dataFrame = dataFrame.withColumn("total_amount", dataFrame["total_amount"].cast("float"))
    dataFrame = dataFrame.withColumn("payment_type", dataFrame["payment_type"].cast("integer"))
    dataFrame = dataFrame.withColumn("trip_type", dataFrame["trip_type"].cast("integer"))
    dataFrame = dataFrame.withColumn("congestion_surcharge", dataFrame["congestion_surcharge"].cast("string"))
    return dataFrame


##################################################################################
# yellow trip data 
##################################################################################

def load_s3_yellowtrip_data(filename=yellow_trip_filename):
    data = sc.textFile(filename).map(lambda line: line.split(","))
    headers = data.first()
    #data_ = data.filter(lambda row: row != headers and row != [''])  # fix null data 
    data_ = data.filter(lambda row: row != headers )  # fix null data 
    dataFrame = sqlContext.createDataFrame(data_,
                ['id','vendor_name','Trip_Pickup_DateTime', 'Trip_Dropoff_DateTime',
                'Passenger_Count','Trip_Distance','Start_Lon',
                'Start_Lat','Rate_Code','store_and_forward',
                'End_Lon','End_Lat','Payment_Type',
                'Fare_Amt','surcharge','mta_tax',
                'Tip_Amt','Tolls_Amt','Total_Amt'])
    dataFrame = dataFrame.withColumn("id", dataFrame["id"].cast("float")) 
    dataFrame = dataFrame.withColumn("vendor_name", dataFrame["vendor_name"].cast("string"))    
    dataFrame = dataFrame.withColumn("Trip_Pickup_DateTime", dataFrame["Trip_Pickup_DateTime"].cast("timestamp"))
    dataFrame = dataFrame.withColumn("Trip_Dropoff_DateTime", dataFrame["Trip_Dropoff_DateTime"].cast("timestamp"))
    dataFrame = dataFrame.withColumn("Passenger_Count", dataFrame["Passenger_Count"].cast("integer"))
    dataFrame = dataFrame.withColumn("Trip_Distance", dataFrame["Trip_Distance"].cast("float"))
    dataFrame = dataFrame.withColumn("Start_Lon", dataFrame["Start_Lon"].cast("float"))
    dataFrame = dataFrame.withColumn("Start_Lat", dataFrame["Start_Lat"].cast("float"))
    dataFrame = dataFrame.withColumn("Rate_Code", dataFrame["Rate_Code"].cast("string"))
    dataFrame = dataFrame.withColumn("store_and_forward", dataFrame["store_and_forward"].cast("string"))
    dataFrame = dataFrame.withColumn("End_Lon", dataFrame["End_Lon"].cast("float"))
    dataFrame = dataFrame.withColumn("End_Lat", dataFrame["End_Lat"].cast("float"))
    dataFrame = dataFrame.withColumn("Payment_Type", dataFrame["Payment_Type"].cast("string"))
    dataFrame = dataFrame.withColumn("Fare_Amt", dataFrame["Fare_Amt"].cast("float"))
    dataFrame = dataFrame.withColumn("surcharge", dataFrame["surcharge"].cast("float"))
    dataFrame = dataFrame.withColumn("mta_tax", dataFrame["mta_tax"].cast("float"))
    dataFrame = dataFrame.withColumn("Tip_Amt", dataFrame["Tip_Amt"].cast("float"))
    dataFrame = dataFrame.withColumn("Tolls_Amt", dataFrame["Tolls_Amt"].cast("float"))
    dataFrame = dataFrame.withColumn("Total_Amt", dataFrame["Total_Amt"].cast("float"))
    return dataFrame


def get_timeslot(dataFrame):
    dataFrame_ = dataFrame.rdd.map(
                      lambda x: Row(
                      vendor_name = x['vendor_name'],
                      Start_Lat = x['Start_Lat'],
                      time=((datetime.strptime(str(x['Trip_Dropoff_DateTime']), "%Y-%m-%d %H:%M:%S").hour*60 +
                             datetime.strptime(str(x['Trip_Dropoff_DateTime']), "%Y-%m-%d %H:%M:%S").minute)/10),
                      timestamp=x['Trip_Dropoff_DateTime'].strftime('%Y-%m-%d')))
    return dataFrame_ 


def spark_transform(dataFrame):
    dataFrame_ = dataFrame.rdd.map(
                  lambda x: Row(
                  small_block_id = str(tuple(map(lambda y: int(math.floor(y/0.00025)), [(x['Start_Lon']+74.25), (x['Start_Lat']-40.5)]))),
                  large_block_id = str(tuple(map( lambda z: z/20, map(lambda y: int(math.floor(y/0.00025)), [(x['Start_Lon']+74.25), (x['Start_Lat']-40.5)])))),
                  time_slot=((datetime.strptime(str(x['Trip_Dropoff_DateTime']), "%Y-%m-%d %H:%M:%S").hour*60 +
                         datetime.strptime(str(x['Trip_Dropoff_DateTime']), "%Y-%m-%d %H:%M:%S").minute)/10),
                  timestamp=x['Trip_Dropoff_DateTime'].strftime('%Y-%m-%d')))\
                  .filter(lambda x: x is not None)
    return dataFrame_


def get_geohash_id(dataFrame):
    udf_geohash = F.udf(lambda x,y: pgh.encode(x,y,precision=7))
    pickup_geohash = dataFrame.select('Trip_Pickup_DateTime','Start_Lat','Start_Lon',udf_geohash('Start_Lat','Start_Lon').alias('geo_hash_id'))
    dropoff_geohash = dataFrame.select('Trip_Dropoff_DateTime','End_Lat','End_Lon',udf_geohash('End_Lat','End_Lon').alias('geo_hash_id'))
    return pickup_geohash, dropoff_geohash


def save_to_mysql(dataFrame, table_name):
    mysql_config= parse_config('config/mysql.config')
    dataFrame.write.format('jdbc').options(
        url=mysql_config['url'],
        driver=mysql_config['driver'],
        dbtable=table_name,
        user=mysql_config['user'],
        password=mysql_config['password']).mode('append').save()

def main():
    df_yellow = load_s3_yellowtrip_data()
    pickup_geohash, dropoff_geohash = get_geohash_id(df_yellow)
    save_to_mysql(df_yellow, 'yellow_trip')
    save_to_mysql(pickup_geohash, 'pickup_geo_hash')
    save_to_mysql(dropoff_geohash, 'dropoff_geo_hash')


if __name__ == '__main__':
    main()

