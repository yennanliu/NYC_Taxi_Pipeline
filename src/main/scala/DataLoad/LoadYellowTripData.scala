package DataLoad

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType, TimestampType}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar

object LoadYellowTripData {

  def main(args: Array[String]) {

    //Source, destination directories
    val srcDataDirRoot = "data/staging/transactional-data/yellow-taxi"
    val destDataDirRoot = "data/processed/yellow-taxi"

    val sc = new SparkContext("local[*]", "LoadYellowTripData")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = SparkSession
      .builder
      .appName("LoadYellowTripData")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .config("spark.network.timeout", "6000s") // https://stackoverflow.com/questions/48219169/3600-seconds-timeout-that-spark-worker-communicating-with-spark-driver-in-heartb
      .config("spark.executor.heartbeatInterval", "10000s")
      .config("spark.executor.memory", "10g")
      .getOrCreate()


    //Canonical ordered column list for yellow taxi across years to homogenize schema
    val canonicalTripSchemaColList = Seq("taxi_type", "vendor_id", "pickup_datetime", "dropoff_datetime", "store_and_fwd_flag", "rate_code_id", "pickup_location_id", "dropoff_location_id", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "payment_type", "trip_year", "trip_month")

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

    //1) Function to determine schema for a given year and month
    //Input:  Year and month
    //Output: StructType for applicable schema
    //Sample call: println(getSchemaStruct(2009,1))

    def getTaxiSchema(tripYear: Int, tripMonth: Int): StructType = {
      var taxiSchema: StructType = null

      if (tripYear > 2008 && tripYear < 2015)
        taxiSchema = yellowTripSchemaPre2015
      else if (tripYear == 2016 && tripMonth > 6)
        taxiSchema = yellowTripSchema2016H2
      else if ((tripYear == 2016 && tripMonth < 7) || (tripYear == 2015))
        taxiSchema = yellowTripSchema20152016H1
      else if (tripYear == 2017 && tripMonth < 7)
        taxiSchema = yellowTripSchema2017H1

      taxiSchema
    }

    // COMMAND ----------

    //2) Function to add columns to dataframe as required to homogenize schema
    //Input:  Dataframe, year and month
    //Output: Dataframe with homogenized schema
    //Sample call: println(getSchemaHomogenizedDataframe(DF,2014,6))

    import org.apache.spark.sql.functions._

    def getSchemaHomogenizedDataframe(sourceDF: org.apache.spark.sql.DataFrame,
                                      tripYear: Int,
                                      tripMonth: Int): org.apache.spark.sql.DataFrame = {

      if (tripYear > 2008 && tripYear < 2015) {
        sourceDF.withColumn("pickup_location_id", lit(0).cast(IntegerType))
          .withColumn("dropoff_location_id", lit(0).cast(IntegerType))
          .withColumn("improvement_surcharge", lit(0).cast(DoubleType))
          .withColumn("junk1", lit(""))
          .withColumn("junk2", lit(""))
          .withColumn("trip_year", substring(col("pickup_datetime"), 0, 4))
          .withColumn("trip_month", substring(col("pickup_datetime"), 6, 2))
          .withColumn("taxi_type", lit("yellow"))
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
      else if ((tripYear == 2016 && tripMonth < 7) || (tripYear == 2015)) {
        sourceDF.withColumn("pickup_location_id", lit(0).cast(IntegerType))
          .withColumn("dropoff_location_id", lit(0).cast(IntegerType))
          .withColumn("junk1", lit(""))
          .withColumn("junk2", lit(""))
          .withColumn("trip_year", substring(col("pickup_datetime"), 0, 4))
          .withColumn("trip_month", substring(col("pickup_datetime"), 6, 2))
          .withColumn("taxi_type", lit("yellow"))
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
      else if (tripYear == 2016 && tripMonth > 6) {
        sourceDF.withColumn("pickup_longitude", lit(""))
          .withColumn("pickup_latitude", lit(""))
          .withColumn("dropoff_longitude", lit(""))
          .withColumn("dropoff_latitude", lit(""))
          .withColumn("trip_year", substring(col("pickup_datetime"), 0, 4))
          .withColumn("trip_month", substring(col("pickup_datetime"), 6, 2))
          .withColumn("taxi_type", lit("yellow"))
          .withColumn("temp_vendor_id", col("vendor_id").cast(StringType)).drop("vendor_id").withColumnRenamed("temp_vendor_id", "vendor_id")
          .withColumn("temp_payment_type", col("payment_type").cast(StringType)).drop("payment_type").withColumnRenamed("temp_payment_type", "payment_type")
      }
      else if (tripYear == 2017 && tripMonth < 7) {
        sourceDF.withColumn("pickup_longitude", lit(""))
          .withColumn("pickup_latitude", lit(""))
          .withColumn("dropoff_longitude", lit(""))
          .withColumn("dropoff_latitude", lit(""))
          .withColumn("trip_year", substring(col("pickup_datetime"), 0, 4))
          .withColumn("trip_month", substring(col("pickup_datetime"), 6, 2))
          .withColumn("taxi_type", lit("yellow"))
          .withColumn("junk1", lit(""))
          .withColumn("junk2", lit(""))
          .withColumn("temp_vendor_id", col("vendor_id").cast(StringType)).drop("vendor_id").withColumnRenamed("temp_vendor_id", "vendor_id")
          .withColumn("temp_payment_type", col("payment_type").cast(StringType)).drop("payment_type").withColumnRenamed("temp_payment_type", "payment_type")
      }
      else
        sourceDF
    }

    //Delete any residual data from prior executions for an idempotent run
    //dbutils.fs.rm(destDataDirRoot,recurse=true)

    // COMMAND ----------

    //Process data, save as parquet

    for (j <- 2009 to 2017)
    //for (j <- 2016 to 2017)
    //for (j <- 2017 to 2017)
    {
      val endMonth = if (j == 2017) 6 else 12
      //val endMonth = 1
      for (i <- 1 to endMonth) {
        //Source path
        //val srcDataFile= srcDataDirRoot + "year=" + j + "/month=" +  "%02d".format(i) + "/type=yellow/yellow_tripdata_" + j + "-" + "%02d".format(i) + ".csv"

        val srcDataFile = srcDataDirRoot + "/yellow_tripdata_" + j + "-" + "%02d".format(i) + ".csv"
        println(srcDataFile)

        println("Processing the yellow taxi data for year=" + j + ", month=" + i + " at " + Calendar.getInstance().getTime())
        println("...............")

        //Source schema
        val taxiSchema = getTaxiSchema(j, i)

        // Convert our csv file to a DataSet, using our Person case
        // class to infer the schema.
        import spark.implicits._

        //Read source data
        val taxiDF = sqlContext.read.format("csv")
          .option("header", "true")
          .schema(taxiSchema)
          .option("delimiter", ",")
          .load(srcDataFile)

        taxiDF.show()

        //Add additional columns to homogenize schema across years
        val taxiFormattedDF = getSchemaHomogenizedDataframe(taxiDF, j, i)

        //Order all columns to align with the canonical schema for yellow taxi
        val taxiCanonicalDF = taxiFormattedDF.select(canonicalTripSchemaColList.map(c => col(c)): _*) //.repartition(400)
        //val taxiCanonicalDF = taxiFormattedDF.select(canonicalTripSchemaColList.map(c => col(c)): _*).repartition(50)

        //To make Hive Parquet format compatible with Spark Parquet format
        spark.sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")

        //Write parquet output, calling function to calculate number of partition files
        taxiCanonicalDF
          .write //.coalesce(srcDataFile)  //.coalesce(calcOutputFileCountTxtToPrq(srcDataFile, 128))
          .format("csv")
          .mode("append")
          .option("header", "true")
          .partitionBy("trip_year", "trip_month")
          .save(destDataDirRoot)
      }
    }

  }

} 