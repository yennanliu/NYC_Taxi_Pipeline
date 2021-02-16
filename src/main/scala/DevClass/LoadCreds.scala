package DevClass

import java.util.concurrent.TimeUnit
import java.io.File

// user defined class 
import utils.GetCreds

object LoadCreds {

  def main(args: Array[String]): Unit = {

    println(">>> import user creds")

    GetCreds

  }

}