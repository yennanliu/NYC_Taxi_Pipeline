package SaveToDB

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType, TimestampType}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar
import java.util.Properties
import java.sql.{Connection, DriverManager}
import org.apache.spark.sql.SaveMode

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration


object JDBCToMysql {

  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "JDBCToMysql")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = SparkSession
      .builder
      .appName("JDBCToMysql")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .config("spark.network.timeout", "6000s") // https://stackoverflow.com/questions/48219169/3600-seconds-timeout-that-spark-worker-communicating-with-spark-driver-in-heartb
      .config("spark.executor.heartbeatInterval", "10000s")
      .config("spark.executor.memory", "10g")
      .getOrCreate()

    import spark.implicits._

    //Source directories
    val srcDataDirRoot = "data/output/"


    // create data
    val taxi_data = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(srcDataDirRoot + "/materializedview/*/*/" + "*.csv")

    taxi_data.createOrReplaceTempView("taxi")

    //only select needed columns here
    val taxi_df = spark.sql(
      """
        SELECT 
        pickup_year, 
        pickup_month, 
        pickup_day,
        dropoff_year,
        dropoff_month,
        dropoff_day,
        pickup_location_id,
        dropoff_location_id,
        trip_distance,
        fare_amount
        FROM taxi LIMIT 100
        """)

    taxi_df.show()

    // Write to mysql
    // http://bigdatums.net/2016/10/16/writing-to-a-database-from-spark/
    // https://docs.databricks.com/data/data-sources/sql-databases.html

    //val url="jdbc:mysql://localhost:3306"
    val jdbcHostname = "localhost"
    val jdbcPort = 3306
    val jdbcDatabase = "nyc_taxi_pipeline"
    val jdbcUsername = "mysql_user"
    val jdbcPassword = "0000"
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

    val table = "taxi_materializedview"

    // Create a Properties() object to hold the parameters.
    import java.util.Properties

    val connectionProperties = new Properties()

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")
    connectionProperties.put("jdbcDatabase", s"${jdbcDatabase}")

    // Insert to a new table (spark will create a new one first)
    // https://www.jowanza.com/blog/writing-a-spark-dataframe-to-mysql-tips-and

    taxi_df.write.jdbc(jdbcUrl, table, connectionProperties)


    // Load from mysql
    // https://spark.apache.org/docs/2.2.0/sql-programming-guide.html#hive-tables
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", table)
      .option("user", jdbcUsername)
      .option("password", jdbcPassword)
      .load()

    jdbcDF.show()

  }

}