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

        // Start the computation

        ssc.start()            

        // Wait for the computation to terminate
        
        ssc.awaitTermination()  

  }

}