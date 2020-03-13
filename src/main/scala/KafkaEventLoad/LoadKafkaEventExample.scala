package KafkaEventLoad

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.{get_json_object, json_tuple}

/*
 * modify from 
 * https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
 *
*/

object LoadKafkaEventExample {

  def main(args: Array[String]): Unit = {

      val sc = new SparkContext("local[*]", "LoadKafkaEventExample")   
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val spark = SparkSession
          .builder
          .appName("LoadKafkaEventExample")
          .master("local[*]")
          .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
          .getOrCreate()

      val sparkSession = SparkSession.builder
        .master("local")
        .appName("example")
        .getOrCreate()

      import spark.implicits._

      // Subscribe to 1 topic

      val df = spark
              .readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", "127.0.0.1:9092") // local kafka server
              .option("subscribe", "first_topic")
              .load()

      df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)]

      df.printSchema

      df.createOrReplaceTempView("k_event")

      spark.sql("SELECT * FROM k_event")
            .writeStream
            .format("console")
            .start()
            .awaitTermination()

      // Subscribe to multiple topics

      // val df2 = spark
      //       .readStream
      //       .format("kafka")
      //       .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      //       .option("subscribe", "topic1,topic2")
      //       .load()

      // df2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      //   .as[(String, String)]

      // // Subscribe to a pattern
      // val df3 = spark
      //       .readStream
      //       .format("kafka")
      //       .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      //       .option("subscribePattern", "topic.*")
      //       .load()

      // df3.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      //   .as[(String, String)]

  }

}