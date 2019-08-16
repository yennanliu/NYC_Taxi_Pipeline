from pyspark.sql.types import FloatType, IntegerType
from pyspark.sql.functions import col
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
from operator import add
import pandas as pd
from datetime import datetime

conf = SparkConf().setAppName("taxi-data-proprocess")
sc = SparkContext(conf=conf)
sqlCtx = SQLContext(sc)
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

def load_csv():
    # load the csv data as spark df 
    data = sc.textFile("data/green_tripdata_sample.csv").map(lambda line: line.split(","))
    headers = data.first()
    traindata = data.filter(lambda row: row != headers)
    sqlContext = SQLContext(sc)
    dataFrame = sqlContext.createDataFrame(traindata,
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
    dataFrame.show()
    print (dataFrame.printSchema())
    return dataFrame
