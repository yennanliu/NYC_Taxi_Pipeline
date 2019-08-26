import sys
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
import pygeohash as pgh
from pyspark.sql import functions as F

def get_geohash_id(taxidf):
    udf_geohash = F.udf(lambda x,y: pgh.encode(x,y,precision=7))
    pickup_geohash = taxidf.select('Trip_Pickup_DateTime','Start_Lat','Start_Lon',udf_geohash('Start_Lat','Start_Lon').alias('geo_hash_id'))
    dropoff_geohash = taxidf.select('Trip_Dropoff_DateTime','End_Lat','End_Lon',udf_geohash('End_Lat','End_Lon').alias('geo_hash_id'))
    return pickup_geohash, dropoff_geohash

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def stream_2_sql_2_df(time, rdd):
    """
    https://spark.apache.org/docs/2.1.1/streaming-programming-guide.html
    https://github.com/apache/spark/blob/v2.1.1/examples/src/main/python/streaming/sql_network_wordcount.py
    """
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w: Row(word=[ i for i in w.split(',')[1:]]))
        taxiDataFrame = spark.createDataFrame(rowRdd)

        # Creates a temporary view using the DataFrame
        taxiDataFrame.createOrReplaceTempView("taxi")

        # Do word count on table using SQL and print it
        query="""
        SELECT word[0] AS vendor_name,
               word[1] AS Trip_Pickup_DateTime,
               word[2] AS Trip_Dropoff_DateTime,
               word[3] AS Passenger_Count,
               word[4] AS Trip_Distance,
               word[5] AS Start_Lon,
               word[6] AS Start_Lat,
               word[7] AS Rate_Code,
               word[8] AS store_and_forward,
               word[9] AS End_Lon,
               word[10] AS End_Lat,
               word[11] AS Payment_Type,
               word[12] AS Fare_Amt,
               word[13] AS surcharge,
               word[14] AS mta_tax,
               word[15] AS Tip_Amt,
               word[16] AS Tolls_Amt,
               word[17] AS Total_Amt
        FROM taxi
        """
        taxiQueryDataFrame = spark.sql(query)
        print (">>>>>>>> RESULT OF wordCountsDataFrame")
        taxiQueryDataFrame.show()
        taxidf =taxiQueryDataFrame.rdd.map(lambda x:x).toDF(
                ['vendor_name','Trip_Pickup_DateTime', 'Trip_Dropoff_DateTime',
                'Passenger_Count','Trip_Distance','Start_Lon',
                'Start_Lat','Rate_Code','store_and_forward',
                'End_Lon','End_Lat','Payment_Type',
                'Fare_Amt','surcharge','mta_tax',
                'Tip_Amt','Tolls_Amt','Total_Amt'])
        taxidf = taxidf.withColumn("vendor_name", taxidf["vendor_name"].cast("string"))    
        taxidf = taxidf.withColumn("Trip_Pickup_DateTime", taxidf["Trip_Pickup_DateTime"].cast("timestamp"))
        taxidf = taxidf.withColumn("Trip_Dropoff_DateTime", taxidf["Trip_Dropoff_DateTime"].cast("timestamp"))
        taxidf = taxidf.withColumn("Passenger_Count", taxidf["Passenger_Count"].cast("integer"))
        taxidf = taxidf.withColumn("Trip_Distance", taxidf["Trip_Distance"].cast("float"))
        taxidf = taxidf.withColumn("Start_Lon", taxidf["Start_Lon"].cast("float"))
        taxidf = taxidf.withColumn("Start_Lat", taxidf["Start_Lat"].cast("float"))
        taxidf = taxidf.withColumn("Rate_Code", taxidf["Rate_Code"].cast("string"))
        taxidf = taxidf.withColumn("store_and_forward", taxidf["store_and_forward"].cast("string"))
        taxidf = taxidf.withColumn("End_Lon", taxidf["End_Lon"].cast("float"))
        taxidf = taxidf.withColumn("End_Lat", taxidf["End_Lat"].cast("float"))
        taxidf = taxidf.withColumn("Payment_Type", taxidf["Payment_Type"].cast("string"))
        taxidf = taxidf.withColumn("Fare_Amt", taxidf["Fare_Amt"].cast("float"))
        taxidf = taxidf.withColumn("surcharge", taxidf["surcharge"].cast("float"))
        taxidf = taxidf.withColumn("mta_tax", taxidf["mta_tax"].cast("float"))
        taxidf = taxidf.withColumn("Tip_Amt", taxidf["Tip_Amt"].cast("float"))
        taxidf = taxidf.withColumn("Tolls_Amt", taxidf["Tolls_Amt"].cast("float"))
        taxidf = taxidf.withColumn("Total_Amt", taxidf["Total_Amt"].cast("float"))
        print('taxidf :', taxidf.collect())
        pickup_geohash, dropoff_geohash = get_geohash_id(taxidf)
        print('pickup_geohash :', pickup_geohash.collect())
        print('dropoff_geohash :', dropoff_geohash.collect())
        #return taxidf 
    except Exception as e:
        print ( str(e), 'sth goes wrong')
        pass

if __name__ == "__main__":
    # use conf to avoid lz4 exception when reading data from kafka using spark streaming
    # https://stackoverflow.com/questions/51479474/lz4-exception-when-reading-data-from-kafka-using-spark-streaming
    conf = SparkConf().setAppName("PythonStreamingDirectKafkaWordCount").set('spark.io.compression.codec','snappy')
    sc = SparkContext.getOrCreate(conf=conf)
    ssc = StreamingContext(sc, 2)
    spark = SparkSession(sc)
    brokers, topic = sys.argv[1:]
    kafkaStream = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})

    lines = kafkaStream.map(lambda x: x[1])
    lines_ = lines.flatMap(lambda line: line.split("\n"))
    lines_.foreachRDD(stream_2_sql_2_df)
    ssc.start()
    ssc.awaitTermination()