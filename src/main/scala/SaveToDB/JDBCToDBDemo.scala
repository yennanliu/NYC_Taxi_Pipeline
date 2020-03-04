package SaveToDB

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar
import java.util.Properties
import java.sql.{Connection,DriverManager}
import org.apache.spark.sql.SaveMode

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.conf.Configuration


object JDBCToDBDemo { 

  def main(args: Array[String]){ 

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

      // load data 
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
    
      df.write.mode("append").jdbc(jdbcUrl, table, connectionProperties)


    //   jdbcDF.write
    //     .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
    //     .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

    // df_2.write().mode(SaveMode.Append).jdbc(connectionProperties.getProperty("url"), "family", connectionProperties);

      // Load from mysql 

      // https://spark.apache.org/docs/2.2.0/sql-programming-guide.html#hive-tables
      //Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
      //Loading data from a JDBC source
      // val jdbcDF = spark.read
      //   .format("jdbc")
      //   .option("url", "jdbc:mysql:dbserver")
      //   .option("dbtable", "yelp.user")
      //   .option("user", "mysql_user")
      //   .option("password", "0000")
      //   .load()

      // val connectionProperties = new Properties()
      // connectionProperties.put("user", "mysql_user")
      // connectionProperties.put("password", "0000")
      // val jdbcDF2 = spark.read.jdbc("jdbc:mysql:dbserver", "schema.tablename", connectionProperties)


      // // Saving data to a JDBC source
      // jdbcDF.write
      //   .format("jdbc")
      //   .option("url", "jdbc:postgresql:dbserver")
      //   .option("dbtable", "schema.tablename")
      //   .option("user", "username")
      //   .option("password", "password")
      //   .save()

      // jdbcDF2.write
      //   .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

      // // Specifying create table column data types on write
      // jdbcDF.write
      //   .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      //   .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

  }

}