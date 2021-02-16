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


object JDBCToDBDemo {

  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "JDBCToDBDemo")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = SparkSession
      .builder
      .appName("JDBCDataToDB")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .config("spark.network.timeout", "6000s") // https://stackoverflow.com/questions/48219169/3600-seconds-timeout-that-spark-worker-communicating-with-spark-driver-in-heartb
      .config("spark.executor.heartbeatInterval", "10000s")
      .config("spark.executor.memory", "10g")
      .getOrCreate()

    import spark.implicits._

    // create data
    val df = Seq(
      (100, "cat"),
      (10, "mouse"),
      (99, "horse")
    ).toDF("userid", "name")

    // write to mysql
    // http://bigdatums.net/2016/10/16/writing-to-a-database-from-spark/
    // https://docs.databricks.com/data/data-sources/sql-databases.html

    //val url="jdbc:mysql://localhost:3306"
    val jdbcHostname = "localhost"
    val jdbcPort = 3306
    val jdbcDatabase = "nyc_taxi_pipeline"
    val jdbcUsername = "mysql_user"
    val jdbcPassword = "0000"
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

    val table = "test_table"

    // Create a Properties() object to hold the parameters.
    import java.util.Properties

    val connectionProperties = new Properties()

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")
    connectionProperties.put("jdbcDatabase", s"${jdbcDatabase}")

    // Insert to a new table (spark will create a new one first)

    df.write.jdbc(jdbcUrl, "test_table", connectionProperties)

    // Append to an existing table

    df.write.mode("append").jdbc(jdbcUrl, table, connectionProperties)

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