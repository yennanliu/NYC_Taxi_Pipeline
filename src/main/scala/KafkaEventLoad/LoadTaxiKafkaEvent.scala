package KafkaEventLoad

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.from_json

/*
 * modify from 
 * https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
 *
*/

object LoadTaxiKafkaEvent {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "LoadTaxiKafkaEvent")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = SparkSession
      .builder
      .appName("LoadTaxiKafkaEvent")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()

    import spark.implicits._

    // Define df schema

    val schema = StructType(
      Array(
        StructField("id", StringType),
        StructField("event_date", StringType),
        StructField("tour_value", StringType),
        StructField("id_driver", StringType),
        StructField("id_passenger", StringType)
      )
    )

    // Subscribe to 1 topic

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092") // local kafka server
      .option("subscribe", "first_topic") // .option("startingOffsets", "earliest") // From starting
      .load()

    /*
     * spark-streaming-from-kafka-topic
     * https://sparkbyexamples.com/spark/spark-streaming-from-kafka-topic/
     *
    */

    val df_ = df.selectExpr("CAST(value AS STRING)")

    df.printSchema

    val taxiDF = df_.select(from_json(col("value"), schema).as("data"))
      .select("data.*")

    taxiDF.printSchema

    taxiDF.createOrReplaceTempView("k_event")

    spark.sql("SELECT * FROM k_event WHERE event_date IS NOT null")
      .writeStream
      .format("console")
      .start()
      .awaitTermination() // <-- should un-comment it if only have df in this script

  }

}