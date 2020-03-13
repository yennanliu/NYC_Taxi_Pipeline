package EventLoad

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object LoadTaxiDataAsStream {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()

    val schema = StructType(
      Array( 
        StructField("taxi_type", StringType),
        StructField("vendor_id", StringType),
        StructField("pickup_datetime", StringType),
        StructField("dropoff_datetime", StringType),
        StructField("store_and_fwd_flag", StringType),
        StructField("rate_code_id", StringType),
        StructField("pickup_location_id", StringType),
        StructField("dropoff_location_id", StringType),
        StructField("pickup_longitude", StringType),
        StructField("pickup_latitude", StringType),
        StructField("dropoff_longitude", StringType),
        StructField("dropoff_latitude", StringType),
        StructField("passenger_count", StringType),
        StructField("trip_distance", StringType),
        StructField("fare_amount", StringType),
        StructField("extra", StringType),
        StructField("mta_tax", StringType),
        StructField("tip_amount", StringType),
        StructField("tolls_amount", StringType),
        StructField("ehail_fee", StringType),
        StructField("improvement_surcharge", StringType),
        StructField("total_amount", StringType),
        StructField("payment_type", StringType),
        StructField("trip_type", StringType),
        StructField("vendor_abbreviation", StringType),
        StructField("vendor_description", StringType),
        StructField("month_name_short", StringType),
        StructField("month_name_full", StringType),
        StructField("payment_type_description", StringType),
        StructField("rate_code_description", StringType),
        StructField("pickup_borough", StringType),
        StructField("pickup_zone", StringType),
        StructField("pickup_service_zone", StringType),
        StructField("dropoff_borough", StringType),
        StructField("dropoff_zone", StringType),
        StructField("dropoff_service_zone", StringType),
        StructField("pickup_year", StringType),
        StructField("pickup_month", StringType),
        StructField("pickup_day", StringType),
        StructField("pickup_hour", StringType),
        StructField("pickup_minute", StringType),
        StructField("pickup_second", StringType),
        StructField("dropoff_year", StringType),
        StructField("dropoff_month", StringType),
        StructField("dropoff_day", StringType),
        StructField("dropoff_hour", StringType),
        StructField("dropoff_minute", StringType),
        StructField("dropoff_second", StringType)
          )
    )

    //create stream from folder
    val fileStreamDf = sparkSession.readStream
      .option("header", "true")
      .schema(schema)
      .csv("data/output/materializedview/") /* <--- BE AWARE TO LOAD FILE IN THIS WAY */

    val query = fileStreamDf.writeStream
      .format("console")
      .outputMode(OutputMode.Append()).start()

      query.awaitTermination()

  }

}