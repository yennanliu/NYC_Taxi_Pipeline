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

/*
Spark structured-streaming
https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
*/

object SparkStructureStream_demo_LoadCSV { 

    def main(args: Array[String]){ 

        val sc = new SparkContext("local[*]", "SparkStructureStream_demo_LoadCSV")   
        val ssc = new StreamingContext(sc, Seconds(1))

        val spark = SparkSession
            .builder
            .appName("JDBCToMysql")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "/temp/") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
            .config("spark.network.timeout", "6000s") // https://stackoverflow.com/questions/48219169/3600-seconds-timeout-that-spark-worker-communicating-with-spark-driver-in-heartb
            .config("spark.executor.heartbeatInterval", "10000s")
            .config("spark.executor.memory", "10g")
            .getOrCreate()

        val inputDirectory = "data/test.csv"

        val userSchema = new StructType()
                          .add("name", "string")
                          .add("age", "integer")
        
        val df = spark
          .readStream
          .option("sep", ",")
          .schema(userSchema)
          .csv(inputDirectory)  

        df.printSchema()
        
        df.isStreaming

        df.createOrReplaceTempView("tmp")

        spark.sql("select * from tmp")

        spark.sql("SELECT * FROM tmp")
              .writeStream
              .format("console")
              .start()
              .awaitTermination()

  }

}