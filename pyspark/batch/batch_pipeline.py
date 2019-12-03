import sys
sys.path.append("./utility/")
import os
import pyspark
import math 
import pygeohash as pgh
from datetime import datetime
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
# UDF 
from utility import * 

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.7.4,org.apache.hadoop:hadoop-aws:2.7.6 pyspark-shell'

class BatchPipeline:
    """
    class that set up the main batch pipeline class 
    1) load s3 data 2) data transform 3) save to mysql 
    """
    def __init__(self):
        """
        init func that set up needed objects for following spark op 
        """
        self.aws_creds = parse_config('config/aws_creds.config')
        self.s3_samplefile = parse_config('config/s3_sample_file.config') 
        self.sc = pyspark.SparkContext.getOrCreate()
        self.sc.setLogLevel("ERROR")
        self.sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', self.aws_creds['AWSAccessKeyId'])
        self.sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', self.aws_creds['AWSSecretKey'])

    def read_from_s3(self):
        """
        read file from s3 then process to spark RDD 
        """
        self.filename = self.s3_samplefile['yellow_trip_file']
        self.headers = self.sc.textFile(self.filename).map(lambda line: line.split(",")).first()
        self.data = (self.sc.textFile(self.filename)
                          .map(lambda line: line.split(","))
                          .filter(lambda row: row != self.headers))
        print (self.sc.broadcast(self.data.collect()))

    def rdd_to_df(self):
        self.dataFrame = pyspark.sql.SQLContext(self.sc).createDataFrame(self.data,
                ['id','vendor_name','Trip_Pickup_DateTime', 'Trip_Dropoff_DateTime',
                'Passenger_Count','Trip_Distance','Start_Lon',
                'Start_Lat','Rate_Code','store_and_forward',
                'End_Lon','End_Lat','Payment_Type',
                'Fare_Amt','surcharge','mta_tax',
                'Tip_Amt','Tolls_Amt','Total_Amt'])
        self.dataFrame = self.dataFrame.withColumn("id", self.dataFrame["id"].cast("float")) 
        self.dataFrame = self.dataFrame.withColumn("vendor_name", self.dataFrame["vendor_name"].cast("string"))    
        self.dataFrame = self.dataFrame.withColumn("Trip_Pickup_DateTime", self.dataFrame["Trip_Pickup_DateTime"].cast("timestamp"))
        self.dataFrame = self.dataFrame.withColumn("Trip_Dropoff_DateTime", self.dataFrame["Trip_Dropoff_DateTime"].cast("timestamp"))
        self.dataFrame = self.dataFrame.withColumn("Passenger_Count", self.dataFrame["Passenger_Count"].cast("integer"))
        self.dataFrame = self.dataFrame.withColumn("Trip_Distance", self.dataFrame["Trip_Distance"].cast("float"))
        self.dataFrame = self.dataFrame.withColumn("Start_Lon", self.dataFrame["Start_Lon"].cast("float"))
        self.dataFrame = self.dataFrame.withColumn("Start_Lat", self.dataFrame["Start_Lat"].cast("float"))
        self.dataFrame = self.dataFrame.withColumn("Rate_Code", self.dataFrame["Rate_Code"].cast("string"))
        self.dataFrame = self.dataFrame.withColumn("store_and_forward", self.dataFrame["store_and_forward"].cast("string"))
        self.dataFrame = self.dataFrame.withColumn("End_Lon", self.dataFrame["End_Lon"].cast("float"))
        self.dataFrame = self.dataFrame.withColumn("End_Lat", self.dataFrame["End_Lat"].cast("float"))
        self.dataFrame = self.dataFrame.withColumn("Payment_Type", self.dataFrame["Payment_Type"].cast("string"))
        self.dataFrame = self.dataFrame.withColumn("Fare_Amt", self.dataFrame["Fare_Amt"].cast("float"))
        self.dataFrame = self.dataFrame.withColumn("surcharge", self.dataFrame["surcharge"].cast("float"))
        self.dataFrame = self.dataFrame.withColumn("mta_tax", self.dataFrame["mta_tax"].cast("float"))
        self.dataFrame = self.dataFrame.withColumn("Tip_Amt", self.dataFrame["Tip_Amt"].cast("float"))
        self.dataFrame = self.dataFrame.withColumn("Tolls_Amt", self.dataFrame["Tolls_Amt"].cast("float"))
        self.dataFrame = self.dataFrame.withColumn("Total_Amt", self.dataFrame["Total_Amt"].cast("float"))

    def run(self):
        self.read_from_s3()
        #self.rdd_to_df()

if __name__ == '__main__':
    batchpipeline = BatchPipeline()
    batchpipeline.run()
