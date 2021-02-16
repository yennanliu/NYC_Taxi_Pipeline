package utils

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType, TimestampType}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object SparkUtils {

  // https://stackoverflow.com/questions/39780792/how-to-build-a-sparksession-in-spark-2-0-using-pyspark

  val sc = new SparkContext("local[*]", "SparkUtils")

  def GetSparkInfo() {
    print("sc :" + sc + "\n")
    print("sc.getConf :" + sc.getConf + "\n")
    print("sc.startTime" + sc.startTime + "\n")
  }

  def GetSparkSession(): SparkContext = {
    sc
  }

  def GetSparkContextConf(): SparkConf = {
    sc.getConf
  }

  def GetSparkContextConfAll(): Array[(String, String)] = {
    sc.getConf.getAll
  }

  def stopSparkContext() {
    sc.stop()
  }

}