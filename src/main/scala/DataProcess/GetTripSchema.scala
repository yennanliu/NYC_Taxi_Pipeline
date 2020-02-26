package DataProcess

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar

object GetTripSchema { 

    def getYellowTaxiSchema(tripYear: Int, tripMonth: Int): StructType = {
        var taxiSchema : StructType = null
            if(tripYear > 2008 && tripYear < 2015)
              taxiSchema = yellowTripSchemaPre2015
            else if(tripYear == 2016 && tripMonth > 6)
              taxiSchema = yellowTripSchema2016H2
            else if((tripYear == 2016 && tripMonth < 7) || (tripYear == 2015))
              taxiSchema = yellowTripSchema20152016H1
            else if(tripYear == 2017 && tripMonth < 7)
              taxiSchema = yellowTripSchema2017H1
        
        taxiSchema
        }

    def getYellowSchemaHomogenizedDataframe(sourceDF: org.apache.spark.sql.DataFrame,
                                            tripYear: Int, 
                                            tripMonth: Int): org.apache.spark.sql.DataFrame =
        {  

          if(tripYear > 2008 && tripYear < 2015)
          {
            sourceDF.withColumn("pickup_location_id", lit(0).cast(IntegerType))
                      .withColumn("dropoff_location_id", lit(0).cast(IntegerType))
                      .withColumn("improvement_surcharge",lit(0).cast(DoubleType))
                      .withColumn("junk1",lit(""))
                      .withColumn("junk2",lit(""))
                      .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                      .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                      .withColumn("taxi_type",lit("yellow"))
                      .withColumn("temp_pickup_longitude", col("pickup_longitude").cast(StringType))
                                              .drop("pickup_longitude").withColumnRenamed("temp_pickup_longitude", "pickup_longitude")
                      .withColumn("temp_dropoff_longitude", col("dropoff_longitude").cast(StringType))
                                              .drop("dropoff_longitude").withColumnRenamed("temp_dropoff_longitude", "dropoff_longitude")
                      .withColumn("temp_pickup_latitude", col("pickup_latitude").cast(StringType))
                                              .drop("pickup_latitude").withColumnRenamed("temp_pickup_latitude", "pickup_latitude")
                      .withColumn("temp_dropoff_latitude", col("dropoff_latitude").cast(StringType))
                                              .drop("dropoff_latitude").withColumnRenamed("temp_dropoff_latitude", "dropoff_latitude")
                      .withColumn("temp_payment_type", col("payment_type").cast(StringType)).drop("payment_type").withColumnRenamed("temp_payment_type", "payment_type")
          }
          else if((tripYear == 2016 && tripMonth < 7) || (tripYear == 2015))
          {
            sourceDF.withColumn("pickup_location_id", lit(0).cast(IntegerType))
                      .withColumn("dropoff_location_id", lit(0).cast(IntegerType))
                      .withColumn("junk1",lit(""))
                      .withColumn("junk2",lit(""))
                      .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                      .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                      .withColumn("taxi_type",lit("yellow"))
                      .withColumn("temp_vendor_id", col("vendor_id").cast(StringType)).drop("vendor_id").withColumnRenamed("temp_vendor_id", "vendor_id")
                      .withColumn("temp_pickup_longitude", col("pickup_longitude").cast(StringType))
                                              .drop("pickup_longitude").withColumnRenamed("temp_pickup_longitude", "pickup_longitude")
                      .withColumn("temp_dropoff_longitude", col("dropoff_longitude").cast(StringType))
                                              .drop("dropoff_longitude").withColumnRenamed("temp_dropoff_longitude", "dropoff_longitude")
                      .withColumn("temp_pickup_latitude", col("pickup_latitude").cast(StringType))
                                              .drop("pickup_latitude").withColumnRenamed("temp_pickup_latitude", "pickup_latitude")
                      .withColumn("temp_dropoff_latitude", col("dropoff_latitude").cast(StringType))
                                              .drop("dropoff_latitude").withColumnRenamed("temp_dropoff_latitude", "dropoff_latitude")
                      .withColumn("temp_payment_type", col("payment_type").cast(StringType)).drop("payment_type").withColumnRenamed("temp_payment_type", "payment_type")
          }
          else if(tripYear == 2016 && tripMonth > 6)
          {
            sourceDF.withColumn("pickup_longitude", lit(""))
                      .withColumn("pickup_latitude", lit(""))
                      .withColumn("dropoff_longitude", lit(""))
                      .withColumn("dropoff_latitude", lit(""))
                      .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                      .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                      .withColumn("taxi_type",lit("yellow"))
                      .withColumn("temp_vendor_id", col("vendor_id").cast(StringType)).drop("vendor_id").withColumnRenamed("temp_vendor_id", "vendor_id")
                      .withColumn("temp_payment_type", col("payment_type").cast(StringType)).drop("payment_type").withColumnRenamed("temp_payment_type", "payment_type")
          }
          else if(tripYear == 2017 && tripMonth < 7)
          {
            sourceDF.withColumn("pickup_longitude", lit(""))
                      .withColumn("pickup_latitude", lit(""))
                      .withColumn("dropoff_longitude", lit(""))
                      .withColumn("dropoff_latitude", lit(""))
                      .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                      .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                      .withColumn("taxi_type",lit("yellow"))
                      .withColumn("junk1",lit(""))
                      .withColumn("junk2",lit(""))
                      .withColumn("temp_vendor_id", col("vendor_id").cast(StringType)).drop("vendor_id").withColumnRenamed("temp_vendor_id", "vendor_id")
                      .withColumn("temp_payment_type", col("payment_type").cast(StringType)).drop("payment_type").withColumnRenamed("temp_payment_type", "payment_type")
          }
      else
        sourceDF
    }

    val getGreenTaxiSchema  = (tripYear: Int, tripMonth: Int) => {
        var taxiSchema : StructType = null

            if((tripYear == 2013 && tripMonth > 7) || tripYear == 2014)
              taxiSchema = greenTripSchemaPre2015
            else if(tripYear == 2015 && tripMonth < 7)
              taxiSchema = greenTripSchema2015H1
            else if((tripYear == 2015 && tripMonth > 6) || (tripYear == 2016 && tripMonth < 7))
              taxiSchema = greenTripSchema2015H22016H1
            else if(tripYear == 2016 && tripMonth > 6)
              taxiSchema = greenTripSchema2016H2
            else if(tripYear == 2017 && tripMonth < 7)
              taxiSchema = greenTripSchema2017H1
        
        taxiSchema
      }

    def getGreenSchemaHomogenizedDataframe(sourceDF: org.apache.spark.sql.DataFrame,
                                      tripYear: Int, 
                                      tripMonth: Int) 
                                      : org.apache.spark.sql.DataFrame =
    {  

          if((tripYear == 2013 && tripMonth > 7) || tripYear == 2014)
          {
            sourceDF.withColumn("pickup_location_id", lit(0).cast(IntegerType))
                      .withColumn("dropoff_location_id", lit(0).cast(IntegerType))
                      .withColumn("improvement_surcharge",lit(0).cast(DoubleType))
                      .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                      .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                      .withColumn("taxi_type",lit("green"))
                      .withColumn("temp_pickup_longitude", col("pickup_longitude").cast(StringType))
                                              .drop("pickup_longitude").withColumnRenamed("temp_pickup_longitude", "pickup_longitude")
                      .withColumn("temp_dropoff_longitude", col("dropoff_longitude").cast(StringType))
                                              .drop("dropoff_longitude").withColumnRenamed("temp_dropoff_longitude", "dropoff_longitude")
                      .withColumn("temp_pickup_latitude", col("pickup_latitude").cast(StringType))
                                              .drop("pickup_latitude").withColumnRenamed("temp_pickup_latitude", "pickup_latitude")
                      .withColumn("temp_dropoff_latitude", col("dropoff_latitude").cast(StringType))
                                              .drop("dropoff_latitude").withColumnRenamed("temp_dropoff_latitude", "dropoff_latitude")
          }
          else if(tripYear == 2015 && tripMonth < 7)
          {
            sourceDF.withColumn("pickup_location_id", lit(0).cast(IntegerType))
                      .withColumn("dropoff_location_id", lit(0).cast(IntegerType))
                      .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                      .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                      .withColumn("taxi_type",lit("green"))
                      .withColumn("temp_pickup_longitude", col("pickup_longitude").cast(StringType))
                                              .drop("pickup_longitude").withColumnRenamed("temp_pickup_longitude", "pickup_longitude")
                      .withColumn("temp_dropoff_longitude", col("dropoff_longitude").cast(StringType))
                                              .drop("dropoff_longitude").withColumnRenamed("temp_dropoff_longitude", "dropoff_longitude")
                      .withColumn("temp_pickup_latitude", col("pickup_latitude").cast(StringType))
                                              .drop("pickup_latitude").withColumnRenamed("temp_pickup_latitude", "pickup_latitude")
                      .withColumn("temp_dropoff_latitude", col("dropoff_latitude").cast(StringType))
                                              .drop("dropoff_latitude").withColumnRenamed("temp_dropoff_latitude", "dropoff_latitude")
          }
          else if((tripYear == 2015 && tripMonth > 6) || (tripYear == 2016 && tripMonth < 7))
          {
            sourceDF.withColumn("pickup_location_id", lit(0).cast(IntegerType))
                      .withColumn("dropoff_location_id", lit(0).cast(IntegerType))
                      .withColumn("junk1",lit(""))
                      .withColumn("junk2",lit(""))
                      .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                      .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                      .withColumn("taxi_type",lit("green"))
                      .withColumn("temp_pickup_longitude", col("pickup_longitude").cast(StringType))
                                              .drop("pickup_longitude").withColumnRenamed("temp_pickup_longitude", "pickup_longitude")
                      .withColumn("temp_dropoff_longitude", col("dropoff_longitude").cast(StringType))
                                              .drop("dropoff_longitude").withColumnRenamed("temp_dropoff_longitude", "dropoff_longitude")
                      .withColumn("temp_pickup_latitude", col("pickup_latitude").cast(StringType))
                                              .drop("pickup_latitude").withColumnRenamed("temp_pickup_latitude", "pickup_latitude")
                      .withColumn("temp_dropoff_latitude", col("dropoff_latitude").cast(StringType))
                                              .drop("dropoff_latitude").withColumnRenamed("temp_dropoff_latitude", "dropoff_latitude")
          }
          else if(tripYear == 2016 && tripMonth > 6)
          {
            sourceDF.withColumn("pickup_longitude", lit(""))
                      .withColumn("pickup_latitude", lit(""))
                      .withColumn("dropoff_longitude", lit(""))
                      .withColumn("dropoff_latitude", lit(""))
                      .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                      .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                      .withColumn("taxi_type",lit("green"))
          }
          else if(tripYear == 2017 && tripMonth < 7)
          {
            sourceDF.withColumn("pickup_longitude", lit(""))
                      .withColumn("pickup_latitude", lit(""))
                      .withColumn("dropoff_longitude", lit(""))
                      .withColumn("dropoff_latitude", lit(""))
                      .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                      .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                      .withColumn("taxi_type",lit("green"))
                      .withColumn("junk1",lit(""))
                      .withColumn("junk2",lit(""))
          }
      else
        sourceDF
    }

    // Yellow Trip 
    //Schema for data based on year and month
    //2017
    val yellowTripSchema2017H1 = StructType(Array(
        StructField("vendor_id", StringType, true),
        StructField("pickup_datetime", TimestampType, true),
        StructField("dropoff_datetime", TimestampType, true),
        StructField("passenger_count", IntegerType, true),
        StructField("trip_distance", DoubleType, true),
        StructField("rate_code_id", IntegerType, true),
        StructField("store_and_fwd_flag", StringType, true),
        StructField("pickup_location_id", IntegerType, true),
        StructField("dropoff_location_id", IntegerType, true),
        StructField("payment_type", StringType, true),
        StructField("fare_amount", DoubleType, true),
        StructField("extra", DoubleType, true),
        StructField("mta_tax", DoubleType, true),
        StructField("tip_amount", DoubleType, true),
        StructField("tolls_amount", DoubleType, true),
        StructField("improvement_surcharge", DoubleType, true),
        StructField("total_amount", DoubleType, true)))

    //Second half of 2016
    val yellowTripSchema2016H2 = StructType(Array(
        StructField("vendor_id", StringType, true),
        StructField("pickup_datetime", TimestampType, true),
        StructField("dropoff_datetime", TimestampType, true),
        StructField("passenger_count", IntegerType, true),
        StructField("trip_distance", DoubleType, true),
        StructField("rate_code_id", IntegerType, true),
        StructField("store_and_fwd_flag", StringType, true),
        StructField("pickup_location_id", IntegerType, true),
        StructField("dropoff_location_id", IntegerType, true),
        StructField("payment_type", StringType, true),
        StructField("fare_amount", DoubleType, true),
        StructField("extra", DoubleType, true),
        StructField("mta_tax", DoubleType, true),
        StructField("tip_amount", DoubleType, true),
        StructField("tolls_amount", DoubleType, true),
        StructField("improvement_surcharge", DoubleType, true),
        StructField("total_amount", DoubleType, true),
        StructField("junk1", StringType, true),
        StructField("junk2", StringType, true)))

    //2015 and 2016 first half of the year
    val yellowTripSchema20152016H1 = StructType(Array(
        StructField("vendor_id", StringType, true),
        StructField("pickup_datetime", TimestampType, true),
        StructField("dropoff_datetime", TimestampType, true),
        StructField("passenger_count", IntegerType, true),
        StructField("trip_distance", DoubleType, true),
        StructField("pickup_longitude", DoubleType, true),
        StructField("pickup_latitude", DoubleType, true),
        StructField("rate_code_id", IntegerType, true),
        StructField("store_and_fwd_flag", StringType, true),
        StructField("dropoff_longitude", DoubleType, true),
        StructField("dropoff_latitude", DoubleType, true),
        StructField("payment_type", StringType, true),
        StructField("fare_amount", DoubleType, true),
        StructField("extra", DoubleType, true),
        StructField("mta_tax", DoubleType, true),
        StructField("tip_amount", DoubleType, true),
        StructField("tolls_amount", DoubleType, true),
        StructField("improvement_surcharge", DoubleType, true),
        StructField("total_amount", DoubleType, true)))

    //2009 though 2014
    val yellowTripSchemaPre2015 = StructType(Array(
        StructField("vendor_id", StringType, true),
        StructField("pickup_datetime", TimestampType, true),
        StructField("dropoff_datetime", TimestampType, true),
        StructField("passenger_count", IntegerType, true),
        StructField("trip_distance", DoubleType, true),
        StructField("pickup_longitude", DoubleType, true),
        StructField("pickup_latitude", DoubleType, true),
        StructField("rate_code_id", IntegerType, true),
        StructField("store_and_fwd_flag", StringType, true),
        StructField("dropoff_longitude", DoubleType, true),
        StructField("dropoff_latitude", DoubleType, true),
        StructField("payment_type", StringType, true),
        StructField("fare_amount", DoubleType, true),
        StructField("extra", DoubleType, true),
        StructField("mta_tax", DoubleType, true),
        StructField("tip_amount", DoubleType, true),
        StructField("tolls_amount", DoubleType, true),
        StructField("total_amount", DoubleType, true)))

    // Green Trip 
    //Schema for data based on year and month
    //2017
    val greenTripSchema2017H1 = StructType(Array(
        StructField("vendor_id", IntegerType, true),
        StructField("pickup_datetime", TimestampType, true),
        StructField("dropoff_datetime", TimestampType, true),
        StructField("store_and_fwd_flag", StringType, true),
        StructField("rate_code_id", IntegerType, true),
        StructField("pickup_location_id", IntegerType, true),
        StructField("dropoff_location_id", IntegerType, true),
        StructField("passenger_count", IntegerType, true),
        StructField("trip_distance", DoubleType, true),
        StructField("fare_amount", DoubleType, true),
        StructField("extra", DoubleType, true),
        StructField("mta_tax", DoubleType, true),
        StructField("tip_amount", DoubleType, true),
        StructField("tolls_amount", DoubleType, true),
        StructField("ehail_fee", DoubleType, true),
        StructField("improvement_surcharge", DoubleType, true),
        StructField("total_amount", DoubleType, true),
        StructField("payment_type", IntegerType, true),
        StructField("trip_type", IntegerType, true)))

    //Second half of 2016
    val greenTripSchema2016H2 = StructType(Array(
        StructField("vendor_id", IntegerType, true),
        StructField("pickup_datetime", TimestampType, true),
        StructField("dropoff_datetime", TimestampType, true),
        StructField("store_and_fwd_flag", StringType, true),
        StructField("rate_code_id", IntegerType, true),
        StructField("pickup_location_id", IntegerType, true),
        StructField("dropoff_location_id", IntegerType, true),
        StructField("passenger_count", IntegerType, true),
        StructField("trip_distance", DoubleType, true),
        StructField("fare_amount", DoubleType, true),
        StructField("extra", DoubleType, true),
        StructField("mta_tax", DoubleType, true),
        StructField("tip_amount", DoubleType, true),
        StructField("tolls_amount", DoubleType, true),
        StructField("ehail_fee", DoubleType, true),
        StructField("improvement_surcharge", DoubleType, true),
        StructField("total_amount", DoubleType, true),
        StructField("payment_type", IntegerType, true),
        StructField("trip_type", IntegerType, true),
        StructField("junk1", StringType, true),
        StructField("junk2", StringType, true)))

    //2015 second half of the year and 2016 first half of the year
    val greenTripSchema2015H22016H1 = StructType(Array(
        StructField("vendor_id", IntegerType, true),
        StructField("pickup_datetime", TimestampType, true),
        StructField("dropoff_datetime", TimestampType, true),
        StructField("store_and_fwd_flag", StringType, true),
        StructField("rate_code_id", IntegerType, true),
        StructField("pickup_longitude", DoubleType, true),
        StructField("pickup_latitude", DoubleType, true),
        StructField("dropoff_longitude", DoubleType, true),
        StructField("dropoff_latitude", DoubleType, true),
        StructField("passenger_count", IntegerType, true),
        StructField("trip_distance", DoubleType, true),
        StructField("fare_amount", DoubleType, true),
        StructField("extra", DoubleType, true),
        StructField("mta_tax", DoubleType, true),
        StructField("tip_amount", DoubleType, true),
        StructField("tolls_amount", DoubleType, true),
        StructField("ehail_fee", DoubleType, true),
        StructField("improvement_surcharge", DoubleType, true),
        StructField("total_amount", DoubleType, true),
        StructField("payment_type", IntegerType, true),
        StructField("trip_type", IntegerType, true)))

    //2015 first half of the year
    val greenTripSchema2015H1 = StructType(Array(
        StructField("vendor_id", IntegerType, true),
        StructField("pickup_datetime", TimestampType, true),
        StructField("dropoff_datetime", TimestampType, true),
        StructField("store_and_fwd_flag", StringType, true),
        StructField("rate_code_id", IntegerType, true),
        StructField("pickup_longitude", DoubleType, true),
        StructField("pickup_latitude", DoubleType, true),
        StructField("dropoff_longitude", DoubleType, true),
        StructField("dropoff_latitude", DoubleType, true),
        StructField("passenger_count", IntegerType, true),
        StructField("trip_distance", DoubleType, true),
        StructField("fare_amount", DoubleType, true),
        StructField("extra", DoubleType, true),
        StructField("mta_tax", DoubleType, true),
        StructField("tip_amount", DoubleType, true),
        StructField("tolls_amount", DoubleType, true),
        StructField("ehail_fee", DoubleType, true),
        StructField("improvement_surcharge", DoubleType, true),
        StructField("total_amount", DoubleType, true),
        StructField("payment_type", IntegerType, true),
        StructField("trip_type", IntegerType, true),
        StructField("junk1", StringType, true),
        StructField("junk2", StringType, true)))

    //August 2013 through 2014
    val greenTripSchemaPre2015 = StructType(Array(
        StructField("vendor_id", IntegerType, true),
        StructField("pickup_datetime", TimestampType, true),
        StructField("dropoff_datetime", TimestampType, true),
        StructField("store_and_fwd_flag", StringType, true),
        StructField("rate_code_id", IntegerType, true),
        StructField("pickup_longitude", DoubleType, true),
        StructField("pickup_latitude", DoubleType, true),
        StructField("dropoff_longitude", DoubleType, true),
        StructField("dropoff_latitude", DoubleType, true),
        StructField("passenger_count", IntegerType, true),
        StructField("trip_distance", DoubleType, true),
        StructField("fare_amount", DoubleType, true),
        StructField("extra", DoubleType, true),
        StructField("mta_tax", DoubleType, true),
        StructField("tip_amount", DoubleType, true),
        StructField("tolls_amount", DoubleType, true),
        StructField("ehail_fee", DoubleType, true),
        StructField("total_amount", DoubleType, true),
        StructField("payment_type", IntegerType, true),
        StructField("trip_type", IntegerType, true),
        StructField("junk1", StringType, true),
        StructField("junk2", StringType, true)))
}