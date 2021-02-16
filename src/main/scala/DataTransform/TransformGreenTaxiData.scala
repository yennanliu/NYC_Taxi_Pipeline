package DataTransform

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType, TimestampType}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar

//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{ FileSystem, Path }


object TransformGreenTaxiData {

  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "TransformGreenTaxiData")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = SparkSession
      .builder
      .appName("TransformGreenTaxiData")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    import spark.implicits._

    //Destination directory

    val srcDataFile = "data/processed"
    val destDataDirRoot = "data/output/transactions/green-taxi"

    // load processed data

    val vendor_lookup = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(srcDataFile + "/reference/vendor/" + "*.csv")

    val trip_type = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(srcDataFile + "/reference/trip-type/" + "*.csv")

    val trip_month = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(srcDataFile + "/reference/trip-month/" + "*.csv")

    val trip_zone = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(srcDataFile + "/reference/taxi-zone/" + "*.csv")

    val rate_code = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(srcDataFile + "/reference/rate-code/" + "*.csv")


    val payment_type = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(srcDataFile + "/reference/payment-type/" + "*.csv")

    val green_taxi = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(srcDataFile + "/green-taxi/*/*/" + "*.csv")

    // RDD -> spark sql table

    vendor_lookup.createOrReplaceTempView("vendor_lookup")
    trip_type.createOrReplaceTempView("trip_type")
    trip_month.createOrReplaceTempView("trip_month")
    trip_zone.createOrReplaceTempView("trip_zone")
    rate_code.createOrReplaceTempView("rate_code")
    payment_type.createOrReplaceTempView("payment_type")
    green_taxi.createOrReplaceTempView("green_taxi")

    // merge spark sql table

    val curatedDF = spark.sql(
      """
          WITH CTE AS
            (SELECT green_taxi.*,
                    year(pickup_datetime) AS pickup_year,
                    month(pickup_datetime) AS pickup_month,
                    day(pickup_datetime) AS pickup_day,
                    hour(pickup_datetime) AS pickup_hour,
                    minute(pickup_datetime) AS pickup_minute,
                    second(pickup_datetime) AS pickup_second,
                    date(pickup_datetime) AS pickup_date,
                    year(dropoff_datetime) AS dropoff_year,
                    month(dropoff_datetime) AS dropoff_month,
                    day(dropoff_datetime) AS dropoff_day,
                    hour(dropoff_datetime) AS dropoff_hour,
                    minute(dropoff_datetime) AS dropoff_minute,
                    second(dropoff_datetime) AS dropoff_second,
                    date(dropoff_datetime) AS dropoff_date
                    FROM green_taxi)
          SELECT DISTINCT t.*,
                          v.abbreviation AS vendor_abbreviation,
                          v.description AS vendor_description,
                          tm.month_name_short,
                          tm.month_name_full,
                          pt.description AS payment_type_description,
                          rc.description AS rate_code_description,
                          tzpu.borough AS pickup_borough,
                          tzpu.zone AS pickup_zone,
                          tzpu.service_zone AS pickup_service_zone,
                          tzdo.borough AS dropoff_borough,
                          tzdo.zone AS dropoff_zone,
                          tzdo.service_zone AS dropoff_service_zone
                          FROM CTE t                        
                          LEFT OUTER JOIN vendor_lookup v ON (t.vendor_id = v.vendor_id)
                          LEFT OUTER JOIN trip_month tm ON (t.pickup_month = tm.trip_month)
                          LEFT OUTER JOIN payment_type pt ON (t.payment_type = pt.payment_type)
                          LEFT OUTER JOIN rate_code rc ON (t.rate_code_id = rc.rate_code_id)
                          LEFT OUTER JOIN trip_zone tzpu ON (t.pickup_location_id = tzpu.location_id)
                          LEFT OUTER JOIN trip_zone tzdo ON (t.dropoff_location_id = tzdo.location_id)

              """)

    curatedDF.show()

    // make duplicated columns : pickup_year, pickup_month, for preventing these columns been dropped out when "partitionby"
    val curatedDF_ = curatedDF.withColumn("_pickup_year", $"pickup_year").withColumn("_pickup_month", $"pickup_month")


    //Save as csv, partition by year and month
    curatedDF_
      .repartition(1) //save output in 1 csv by month by year, can do the "larger" repartition when work on the whole dataset
      .write
      .format("csv")
      .mode("append")
      .option("header", "true")
      .partitionBy("_pickup_year", "_pickup_month")
      .save(destDataDirRoot)
  }

}