package DevClass

import java.util.concurrent.TimeUnit
import java.io.File

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