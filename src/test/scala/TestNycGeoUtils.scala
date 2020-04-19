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

import utils.NycGeoUtils

class TestNycGeoUtils extends FunSuite {

  test("NycGeoUtils OP should work as below") {
  
  val myGeoPoint = datatypes.GeoPoint(20, 10)

  assert(myGeoPoint.lon == 20)
  assert(myGeoPoint.lat == 10)
  assert(NycGeoUtils.isInNYC(myGeoPoint) == true )
  assert(NycGeoUtils.mapToGridCell(myGeoPoint) == 6238607 )
  assert(NycGeoUtils.getGridCellCenter(10) ==  datatypes.GeoPoint(-74.0353012084961,40.99937438964844) )

  }

}