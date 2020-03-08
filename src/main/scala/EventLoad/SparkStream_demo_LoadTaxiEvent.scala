package EventLoad

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar

import org.apache.spark._
import org.apache.spark.streaming._


object SparkStream_demo_LoadTaxiEvent { 

    def main(args: Array[String]){ 

        val sc = new SparkContext("local[*]", "SparkStream_demo_LoadTaxiEvent")   
        //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        // val spark = SparkSession
        //     .builder
        //     .appName("LoadTaxiBasicEvent")
        //     .master("local[*]")
        //     .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        //     .getOrCreate()

        // val conf = new SparkConf()
        //            .setMaster("local[*]")
        //            .setAppName("LoadTaxiBasicEvent")
        //            .set("spark.driver.allowMultipleContexts", "true")

        //val ssc = new StreamingContext(conf, Seconds(1))
        val ssc = new StreamingContext(sc, Seconds(1))

        //import spark.implicits._

        // will listen localhost:44444 with stream from TaxiEvent.CreateBasicTaxiEvent script

        val lines = ssc.socketTextStream("localhost", 44444)

        println (lines)

        val words = lines.flatMap( x => x.split(" "))

        val pairs = words.map(word => (word, 1))

        val wordCounts = pairs.reduceByKey(_ + _)

        // Print the first ten elements of each RDD generated in this DStream to the console
        
        wordCounts.print()

        ssc.start()             // Start the computation

        ssc.awaitTermination()  // Wait for the computation to terminate

  }

}