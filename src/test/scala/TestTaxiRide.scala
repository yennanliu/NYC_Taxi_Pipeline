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

import datatypes.GeoPoint

// http://www.scalatest.org/user_guide/writing_your_first_test

class TestTaxiRide extends FunSuite {

  test("TaxiRide OP should work as below") {

  // case class GeoPoint(lon: Double, lat: Double)
  val myGeoPoint = GeoPoint(123, 321)
  assert(myGeoPoint.lon == 123)

  }

}