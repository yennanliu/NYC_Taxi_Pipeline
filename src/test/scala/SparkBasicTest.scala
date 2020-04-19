package NYC_Taxi_Pipeline_Test

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.scalatest.FunSuite
import collection.mutable.Stack
import org.scalatest._

// http://www.scalatest.org/user_guide/writing_your_first_test

class SparkBasicTest extends FunSuite {

  test("a spark OP should work as below") {

  val sc = new SparkContext("local[*]", "LoadGreenTripData")   

  val data = sc.parallelize(List(1,2,3,4))

  assert(data.collect().length ==  4)
  assert(data.sum() ==  10)
  assert(data.count() == 4)

  }

}