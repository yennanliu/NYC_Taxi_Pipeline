package DevClass

import java.util.concurrent.TimeUnit
import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

// user defined class 
import utils.NycGeoUtils
import datatypes.TaxiRide


object ImportUserDefinedClassTest {

  def main(args: Array[String]): Unit = {

    println(">>> import user defined class")

    println(utils.NycGeoUtils)

    println(datatypes.TaxiRide)

    println(datatypes.GeoPoint)
    

  }

}