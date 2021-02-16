package SaveToHive

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


object SaveToHiveDemo {

  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "SaveToHiveDemo")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = SparkSession
      .builder
      .appName("SaveToHiveDemo")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .config("spark.network.timeout", "6000s") // https://stackoverflow.com/questions/48219169/3600-seconds-timeout-that-spark-worker-communicating-with-spark-driver-in-heartb
      .config("spark.executor.heartbeatInterval", "10000s")
      .config("spark.executor.memory", "10g")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // create data
    val df = Seq(
      (100, "cat"),
      (10, "mouse"),
      (99, "horse")
    ).toDF("userid", "name")

    // save df to Hive
    df.write.mode(SaveMode.Overwrite).saveAsTable("hive_table")

    spark.sql("SELECT * FROM hive_table").show()


  }

}