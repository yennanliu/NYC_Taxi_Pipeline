package KafkaEventProduce

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType, TimestampType}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.{get_json_object, json_tuple}

/*
 * modify from 
 * https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html
 *
*/

object StreamFileToKafkaExample {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "StreamFileToKafkaExample")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = SparkSession
      .builder
      .appName("StreamFileToKafkaExample")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()

    val schema = StructType(
      Array(StructField("transactionId", StringType),
        StructField("customerId", StringType),
        StructField("itemId", StringType),
        StructField("amountPaid", StringType)))

    import spark.implicits._

    //create stream from folder

    val fileStreamDf = sparkSession.readStream
      .option("header", "true")
      .schema(schema)
      .csv("data/tmp") /* <--- BE AWARE TO LOAD FILE IN THIS WAY */

    // write to kafka topic : spark-stream

    println(">>> Stream to Kafaka")

    /*
     * in order to stream to kafka
     *  1. df has a value column
     *  2. StringType or BinaryType
     *
     * https://stackoverflow.com/questions/48788612/pyspark-structured-streaming-output-sink-as-kafka-giving-error
     * https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#writing-data-to-kafka
    */


    // plz clean kafka meta data if want to send the same file data to the kafka topic multiple times
    // sudo rm -fr /tmp/checkpoint

    fileStreamDf.select(to_json(struct(fileStreamDf.columns map col: _*)).alias("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092") // local kafka server
      .option("topic", "spark-stream")
      .option("checkpointLocation", "/tmp/checkpoint")
      .start()

    fileStreamDf.writeStream
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination()

  }

}