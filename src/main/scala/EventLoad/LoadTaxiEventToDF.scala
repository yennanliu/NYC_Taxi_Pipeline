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
Get the  stream DF via Spark Stream (legacy)
*/

object LoadTaxiEventToDF {

  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "LoadTaxiEventToDF")
    val ssc = new StreamingContext(sc, Seconds(3))
    val spark = SparkSession
      .builder
      .appName("LoadTaxiEventToDF")
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

    val lines = ssc.socketTextStream("localhost", 44444)

    lines
      .foreachRDD { rdd =>

        val df = spark.read.json(rdd.map(x => x))

        //df.show()

        // Create a temporary view

        df.printSchema()

        df.createOrReplaceTempView("event")

        spark.sql("SELECT * FROM event").show()

        spark.sql("SELECT count(*) AS event_count FROM event").show()

      }

    ssc.start()

    ssc.awaitTermination()

  }

}