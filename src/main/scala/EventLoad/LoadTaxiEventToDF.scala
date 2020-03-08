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


object LoadTaxiEventToDF { 

    def main(args: Array[String]){ 

        val sc = new SparkContext("local[*]", "LoadTaxiEventToDF")   
        val ssc = new StreamingContext(sc, Seconds(10))

        //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val spark = SparkSession
            .builder
            .appName("LoadTaxiEventToDF")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
            .getOrCreate()

        import spark.implicits._

        // will listen localhost:44444 with stream from TaxiEvent.CreateBasicTaxiEvent script

        val lines = ssc.socketTextStream("localhost", 44444)

        lines
            .map(p => p )  //.map(p => ( p(id) , p(event_date) ))
            .foreachRDD { rdd =>

                // Convert RDD to DataFrame
                val df = rdd.toDF("id", "event_date")

                // Create a temporary view
                df.createOrReplaceTempView("event")

                spark.sql("select * from event").show()

            }

        ssc.start()     

        ssc.awaitTermination() 

  }

}