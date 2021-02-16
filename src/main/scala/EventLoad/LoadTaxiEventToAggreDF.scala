package EventLoad

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType, TimestampType}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar

import org.apache.spark._
import org.apache.spark.streaming._

/*
Get the stream aggregated DF via Spark Structure Stream
*/

object LoadTaxiEventToAggreDF {

  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "LoadTaxiEventToAggreDF")
    val spark = SparkSession
      .builder
      .appName("LoadTaxiEventToAggreDF")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    import spark.implicits._

    // optional : define df schema

    val schema = new StructType()
      .add("id", StringType, true)
      .add("event_date", StringType, true)
      .add("tour_value", StringType, true)
      .add("id_driver", StringType, true)
      .add("id_passenger", StringType, true)

    // will listen localhost:44444 with stream from TaxiEvent.CreateBasicTaxiEvent script

    val lines = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 44444)
      .load()

    // Returns True for DataFrames that have streaming sources

    lines.isStreaming

    // print schema

    lines.printSchema

    /** *
     * *
     * json -> df  (spark structure stream)
     * https://stackoverflow.com/questions/54759366/convert-streaming-json-to-dataframe/54760442
     *
     * **/

    val df = lines
      .selectExpr("CAST(value AS STRING)").as[String]
      .select(from_json($"value", schema)
        .alias("tmp"))
      .select("tmp.*")

    df.printSchema

    df.createOrReplaceTempView("event")

    spark.sql("select * from event")

    //val wordCounts = df.groupBy("id", "id_driver").count()

    // Start running the query that prints the running counts to the console
    val query = df.groupBy("id", "id_driver")
      .count()
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    val query2 = df.groupBy("id_driver")
      .count()
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    spark.sql("SELECT * FROM event WHERE id IS NOT NULL")
      .writeStream
      .format("console")
      .start()
      .awaitTermination()

    //query.awaitTermination()
    query2.awaitTermination()

  }

}