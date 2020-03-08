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

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{SparkContext, SparkConf}
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}


object LoadCSVStream { 

    def main(args: Array[String]){ 

        val sc = new SparkContext("local[*]", "LoadCSVStream")   
        val ssc = new StreamingContext(sc, Seconds(1))

        val inputDirectory = "data/yellow_tripdata_sample.csv"

        val lines = ssc.fileStream[LongWritable, Text, TextInputFormat](inputDirectory).map( x => x.toString )  //.map( (x, y) => (x.toString, y.toString) )
    
        lines.print()

        ssc.start()             // Start the computation

        ssc.awaitTermination()  // Wait for the computation to terminate

  }

}