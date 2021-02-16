package StreamToES

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
 * https://blog.knoldus.com/spark-structured-streaming-with-elasticsearch/
 *
*/

object StreamToESExample {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "StreamFileToKafkaExample")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = SparkSession
      .builder
      .appName("StreamFileToKafkaExample")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .config("es.index.auto.create", "true")
      .getOrCreate()

    val sparkSession = SparkSession.builder
      .config("es_nodes", "127.0.0.1") //.config(ES_NODES, "127.0.0.1")
      .config("es_port", "9200") //.config(ES_PORT, "9200")
      .master("local[*]")
      .appName("StreamToESExample")
      .getOrCreate()

    import spark.implicits._

    val jsonSchema = StructType(
      Seq(
        StructField("transactionId", StringType, true),
        StructField("customerId", StringType, true),
        StructField("itemId", StringType, true),
        StructField("amountPaid", StringType, true)
      )
    )

    val streamingDF = sparkSession
      .readStream
      .schema(jsonSchema)
      .json("data/tmp/")

    println(">>> Stream to ES")

    streamingDF
      .writeStream
      .option("truncate", "false")
      .format("console")
      .start()

    streamingDF
      .writeStream
      .outputMode("append")
      .format("org.elasticsearch.spark.sql")
      .option("checkpointLocation", "path-to-checkpointing")
      .start("index-name/doc-type")
      .awaitTermination()

  }

}