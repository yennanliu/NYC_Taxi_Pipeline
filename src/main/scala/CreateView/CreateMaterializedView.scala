package CreateView

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType, TimestampType}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration


object CreateMaterializedView {

  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "CreateMaterializedView")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = SparkSession
      .builder
      .appName("CreateMaterializedView")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .config("spark.network.timeout", "6000s") // https://stackoverflow.com/questions/48219169/3600-seconds-timeout-that-spark-worker-communicating-with-spark-driver-in-heartb
      .config("spark.executor.heartbeatInterval", "10000s")
      .config("spark.executor.memory", "10g")
      .getOrCreate()

    import spark.implicits._

    //Source, destination directories
    var srcDataDirRoot = "data/output/transactions"
    val destDataDirRoot = "data/output/materializedview/"

    // load the transformed data
    val green_taxi = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(srcDataDirRoot + "/green-taxi/*/*/" + "*.csv")

    val yellow_taxi = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(srcDataDirRoot + "/yellow-taxi/*/*/" + "*.csv")

    // register as temp view

    yellow_taxi.createOrReplaceTempView("yellow_taxi")
    green_taxi.createOrReplaceTempView("green_taxi")

    //Read source data
    val yellowTaxiDF = spark.sql(
      """
      SELECT 
          taxi_type,
          vendor_id,
          pickup_datetime,
          dropoff_datetime,
          store_and_fwd_flag,
          rate_code_id,
          pickup_location_id,
          dropoff_location_id,
          pickup_longitude,
          pickup_latitude,
          dropoff_longitude,
          dropoff_latitude,
          passenger_count,
          trip_distance,
          fare_amount,
          extra,
          mta_tax,
          tip_amount,
          tolls_amount,
          improvement_surcharge,
          total_amount,
          payment_type,
          vendor_abbreviation,
          vendor_description,
          month_name_short,
          month_name_full,
          payment_type_description,
          rate_code_description,
          pickup_borough,
          pickup_zone,
          pickup_service_zone,
          dropoff_borough,
          dropoff_zone,
          dropoff_service_zone,
          pickup_year,
          pickup_month,
          pickup_day,
          pickup_hour,
          pickup_minute,
          pickup_second,
          dropoff_year,
          dropoff_month,
          dropoff_day,
          dropoff_hour,
          dropoff_minute,
          dropoff_second
        FROM yellow_taxi 
      """)

    //Add extra columns
    val yellowTaxiDFHomogenized = yellowTaxiDF.withColumn("ehail_fee", lit(0.0))
      .withColumn("trip_type", lit(0))
      .withColumn("rate_code_description", lit("")).cache()
    //Materialize
    yellowTaxiDFHomogenized.count()

    //Register temporary view
    yellowTaxiDFHomogenized.createOrReplaceTempView("yellow_taxi_trips_unionable")


    yellowTaxiDFHomogenized.printSchema


    val matViewDF = spark.sql(
      """
        SELECT DISTINCT  
          taxi_type,
          vendor_id,
          pickup_datetime,
          dropoff_datetime,
          store_and_fwd_flag,
          rate_code_id,
          pickup_location_id,
          dropoff_location_id,
          pickup_longitude,
          pickup_latitude,
          dropoff_longitude,
          dropoff_latitude,
          passenger_count,
          trip_distance,
          fare_amount,
          extra,
          mta_tax,
          tip_amount,
          tolls_amount,
          ehail_fee,
          improvement_surcharge,
          total_amount,
          payment_type,
          trip_type,
          vendor_abbreviation,
          vendor_description,
          month_name_short,
          month_name_full,
          payment_type_description,
          rate_code_description,
          pickup_borough,
          pickup_zone,
          pickup_service_zone,
          dropoff_borough,
          dropoff_zone,
          dropoff_service_zone,
          pickup_year,
          pickup_month,
          pickup_day,
          pickup_hour,
          pickup_minute,
          pickup_second,
          dropoff_year,
          dropoff_month,
          dropoff_day,
          dropoff_hour,
          dropoff_minute,
          dropoff_second
        FROM yellow_taxi_trips_unionable 
      UNION ALL
        SELECT DISTINCT 
          taxi_type,
          vendor_id,
          pickup_datetime,
          dropoff_datetime,
          store_and_fwd_flag,
          rate_code_id,
          pickup_location_id,
          dropoff_location_id,
          pickup_longitude,
          pickup_latitude,
          dropoff_longitude,
          dropoff_latitude,
          passenger_count,
          trip_distance,
          fare_amount,
          extra,
          mta_tax,
          tip_amount,
          tolls_amount,
          ehail_fee,
          improvement_surcharge,
          total_amount,
          payment_type,
          trip_type,
          vendor_abbreviation,
          vendor_description,
          month_name_short,
          month_name_full,
          payment_type_description,
          rate_code_description,
          pickup_borough,
          pickup_zone,
          pickup_service_zone,
          dropoff_borough,
          dropoff_zone,
          dropoff_service_zone,
          pickup_year,
          pickup_month,
          pickup_day,
          pickup_hour,
          pickup_minute,
          pickup_second,
          dropoff_year,
          dropoff_month,
          dropoff_day,
          dropoff_hour,
          dropoff_minute,
          dropoff_second
        FROM green_taxi 
      """)

    // make duplicated columns : pickup_year, pickup_month, for preventing these columns been dropped out when "partitionby"
    val matViewDF_ = matViewDF.withColumn("_pickup_year", $"pickup_year").withColumn("_pickup_month", $"pickup_month") //.withColumn("_taxi_type", $"taxi_type")

    matViewDF.printSchema

    //Save as Delta
    matViewDF_
      .repartition(1) //save output in 1 csv by month by year, can do the "larger" repartition when work on the whole dataset
      .write
      .format("csv")
      .mode("append")
      .option("header", "true")
      .partitionBy("_pickup_year", "_pickup_month") //.partitionBy("_taxi_type","_pickup_year","_pick_month")
      .save(destDataDirRoot)

  }

}