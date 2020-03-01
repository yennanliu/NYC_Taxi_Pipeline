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


object LoadReferenceData {

    def main(args: Array[String]){ 
        //Source, destination directories
        val srcDataDirRoot = "data/staging/reference-data/" 
        val destDataDirRoot = "data/processed/reference/" 


        val sc = new SparkContext("local[*]", "LoadReferenceData")   
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val spark = SparkSession
            .builder
            .appName("LoadReferenceData")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
            .getOrCreate()

        //1.  Taxi zone lookup
        val taxiZoneSchema = StructType(Array(
            StructField("location_id", StringType, true),
            StructField("borough", StringType, true),
            StructField("zone", StringType, true),
            StructField("service_zone", StringType, true)))

        //2. Months of the year
        val tripMonthNameSchema = StructType(Array(
            StructField("trip_month", StringType, true),
            StructField("month_name_short", StringType, true),
            StructField("month_name_full", StringType, true)))

        //3.  Rate code id lookup
        val rateCodeSchema = StructType(Array(
            StructField("rate_code_id", IntegerType, true),
            StructField("description", StringType, true)))

        //4.  Payment type lookup
        val paymentTypeSchema = StructType(Array(
            StructField("payment_type", IntegerType, true),
            StructField("abbreviation", StringType, true),
            StructField("description", StringType, true)))

        //5. Trip type
        val tripTypeSchema = StructType(Array(
            StructField("trip_type", IntegerType, true),
            StructField("description", StringType, true)))


        //6. Vendor ID
        val vendorSchema = StructType(Array(
            StructField("vendor_id", IntegerType, true),
            StructField("abbreviation", StringType, true),
            StructField("description", StringType, true)))


        def loadReferenceData(srcDatasetName: String, srcDataFile: String, destDataDir: String, srcSchema: StructType, delimiter: String )
        {
          println("Dataset:  " + srcDatasetName)
          println(".......................................................") 
          
          //Execute for idempotent runs
          //println("....deleting destination directory - " + dbutils.fs.rm(destDataDir, recurse=true))
          
          //Read source data
          val refDF = spark.read.option("header", "true")
                              .schema(srcSchema)
                              .option("delimiter",delimiter)
                              .csv(srcDataFile)
              
          //Write csv output
          println("....reading source and saving as parquet")
          //refDF.coalesce(1).write.parquet(destDataDir)
          //refDF.coalesce(1).write.csv(destDataDir)

          refDF   
              .write  //.coalesce(srcDataFile)  //.coalesce(calcOutputFileCountTxtToPrq(srcDataFile, 128))
              .format("csv")
              .mode("append")
              .option("header","true")
              .save(destDataDir)
          
          //Delete residual files from job operation (_SUCCESS, _start*, _committed*)
          println("....deleting flag files")
          //dbutils.fs.ls(destDataDir + "/").foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))
          
          println("....done")
        }

        loadReferenceData("taxi zone",srcDataDirRoot + "taxi_zone_lookup.csv",destDataDirRoot + "taxi-zone",taxiZoneSchema,",")
        loadReferenceData("trip month",srcDataDirRoot + "trip_month_lookup.csv",destDataDirRoot + "trip-month",tripMonthNameSchema,",")
        loadReferenceData("rate code",srcDataDirRoot + "rate_code_lookup.csv",destDataDirRoot + "rate-code",rateCodeSchema,"|")
        loadReferenceData("payment type",srcDataDirRoot + "payment_type_lookup.csv",destDataDirRoot + "payment-type",paymentTypeSchema,"|")
        loadReferenceData("trip type",srcDataDirRoot + "trip_type_lookup.csv",destDataDirRoot + "trip-type",tripTypeSchema,"|")
        loadReferenceData("vendor",srcDataDirRoot + "vendor_lookup.csv",destDataDirRoot + "vendor",vendorSchema,"|")

        //display(dbutils.fs.ls("/mnt/workshop/curated/nyctaxi/reference/"))

        // COMMAND ----------

        // MAGIC %md
        // MAGIC ### 5. Create Hive tables

        // COMMAND ----------

        // MAGIC %sql 
        // MAGIC use taxi_db;
        // MAGIC DROP TABLE IF EXISTS taxi_zone_lookup;
        // MAGIC CREATE TABLE IF NOT EXISTS taxi_zone_lookup(
        // MAGIC location_id STRING,
        // MAGIC borough STRING,
        // MAGIC zone STRING,
        // MAGIC service_zone STRING)
        // MAGIC USING parquet
        // MAGIC LOCATION '/mnt/workshop/curated/nyctaxi/reference/taxi-zone/';

        // COMMAND ----------

        // MAGIC %sql
        // MAGIC use taxi_db;
        // MAGIC DROP TABLE IF EXISTS trip_month_lookup;
        // MAGIC CREATE TABLE IF NOT EXISTS trip_month_lookup(
        // MAGIC trip_month STRING,
        // MAGIC month_name_short STRING,
        // MAGIC month_name_full STRING)
        // MAGIC USING parquet
        // MAGIC LOCATION '/mnt/workshop/curated/nyctaxi/reference/trip-month/';

        // COMMAND ----------

        // MAGIC %sql
        // MAGIC use taxi_db;
        // MAGIC DROP TABLE IF EXISTS rate_code_lookup;
        // MAGIC CREATE TABLE IF NOT EXISTS rate_code_lookup(
        // MAGIC rate_code_id INT,
        // MAGIC description STRING)
        // MAGIC USING parquet
        // MAGIC LOCATION '/mnt/workshop/curated/nyctaxi/reference/rate-code/';

        // COMMAND ----------

        // MAGIC %sql
        // MAGIC use taxi_db;
        // MAGIC DROP TABLE IF EXISTS payment_type_lookup;
        // MAGIC CREATE TABLE IF NOT EXISTS payment_type_lookup(
        // MAGIC payment_type INT,
        // MAGIC abbreviation STRING,
        // MAGIC description STRING)
        // MAGIC USING parquet
        // MAGIC LOCATION '/mnt/workshop/curated/nyctaxi/reference/payment-type/';

        // COMMAND ----------

        // MAGIC %sql
        // MAGIC use taxi_db;
        // MAGIC DROP TABLE IF EXISTS trip_type_lookup;
        // MAGIC CREATE TABLE IF NOT EXISTS trip_type_lookup(
        // MAGIC trip_type INT,
        // MAGIC description STRING)
        // MAGIC USING parquet
        // MAGIC LOCATION '/mnt/workshop/curated/nyctaxi/reference/trip-type/';

        // COMMAND ----------

        // MAGIC %sql
        // MAGIC use taxi_db;
        // MAGIC DROP TABLE IF EXISTS vendor_lookup;
        // MAGIC CREATE TABLE IF NOT EXISTS vendor_lookup(
        // MAGIC vendor_id INT,
        // MAGIC abbreviation STRING,
        // MAGIC description STRING)
        // MAGIC USING parquet
        // MAGIC LOCATION '/mnt/workshop/curated/nyctaxi/reference/vendor/';
    }

} 